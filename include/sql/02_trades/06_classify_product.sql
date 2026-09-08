-- target: trd.trades_classified
-- depends: trd.trades_converted
--
-- Size and product classification, used for regulatory banding downstream.
--
-- The size tier is a CASE over base_amount. base_amount is USD, so the
-- thresholds are absolute. A NULL base_amount matches none of the WHEN arms --
-- NULL > 25000000 is NULL, not TRUE -- and therefore lands in the ELSE arm.
-- The ELSE was written as a catch-all for genuinely tiny tickets; it is now
-- also where anything unconvertible quietly ends up.

CREATE OR REPLACE TABLE trd.trades_classified AS
WITH tiered AS (
    SELECT
        c.*,
        CASE
            WHEN c.base_amount >= 25000000 THEN 'TIER_1_BLOCK'
            WHEN c.base_amount >= 5000000  THEN 'TIER_2_LARGE'
            WHEN c.base_amount >= 500000   THEN 'TIER_3_STANDARD'
            WHEN c.base_amount >= 50000    THEN 'TIER_4_SMALL'
            ELSE                                'TIER_5_RESIDUAL'
        END AS size_tier,
        CASE
            WHEN c.product_type IN ('IRS', 'FXFWD')       THEN 'DERIVATIVE'
            WHEN c.product_type IN ('BOND', 'REPO')       THEN 'FINANCING'
            WHEN c.product_type = 'EQTY'                  THEN 'CASH_EQUITY'
            ELSE                                               'OTHER'
        END AS asset_class,
        CASE
            WHEN c.accounting_treatment = 'ACCRUAL' THEN 'BANKING_BOOK'
            ELSE                                         'TRADING_BOOK'
        END AS regulatory_book
    FROM trd.trades_converted c
)

SELECT
    t.*,
    t.size_tier = 'TIER_5_RESIDUAL' AND t.base_amount IS NULL AS tier_from_null_amount,
    CASE
        WHEN t.asset_class = 'DERIVATIVE'  THEN t.base_amount * 0.05
        WHEN t.asset_class = 'FINANCING'   THEN t.base_amount * 0.02
        WHEN t.asset_class = 'CASH_EQUITY' THEN t.base_amount * 0.15
        ELSE                                    t.base_amount * 0.10
    END AS regulatory_add_on
FROM tiered t;
