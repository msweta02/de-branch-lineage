-- target: rpt.regulatory_extract
-- depends: rpt.pnl_daily, ref.counterparty, ref.book_hierarchy
--
-- The externally-published regulatory line items. Exposure is banded, the LEI
-- is attached for each counterparty, and banking-book positions are reported
-- separately from trading-book ones.
--
-- 03_desk_summary reads from here rather than from rpt.pnl_daily so the desk
-- numbers tie out to what was filed.

CREATE OR REPLACE TABLE rpt.regulatory_extract AS
WITH pnl AS (
    SELECT
        p.business_date,
        p.book_code,
        p.desk_code,
        p.desk_name,
        p.division,
        p.legal_entity,
        p.regulatory_book,
        p.asset_class,
        p.cp_code,
        p.credit_bucket,
        p.settlement_basis,
        p.net_amount_usd,
        p.market_value_usd,
        p.mtm_pnl_usd,
        p.accrued_interest_usd,
        p.regulatory_add_on_usd,
        p.total_pnl_usd,
        p.trade_count
    FROM rpt.pnl_daily p
    WHERE p.is_business_day
),

with_cp AS (
    SELECT
        p.*,
        c.cp_lei,
        c.cp_name,
        c.country AS cp_country
    FROM pnl p
    LEFT JOIN ref.counterparty c
           ON p.cp_code = c.cp_code
),

banded AS (
    SELECT
        w.*,
        CASE
            WHEN ABS(w.net_amount_usd) >= 100000000 THEN 'BAND_A_GE_100M'
            WHEN ABS(w.net_amount_usd) >=  10000000 THEN 'BAND_B_10M_100M'
            WHEN ABS(w.net_amount_usd) >=   1000000 THEN 'BAND_C_1M_10M'
            WHEN ABS(w.net_amount_usd) >         0  THEN 'BAND_D_LT_1M'
            ELSE                                         'BAND_E_ZERO_OR_NULL'
        END AS exposure_band,
        CASE
            WHEN w.regulatory_book = 'BANKING_BOOK' THEN w.regulatory_add_on_usd * 1.5
            ELSE                                         w.regulatory_add_on_usd
        END AS capital_add_on_usd
    FROM with_cp w
)

SELECT
    business_date,
    legal_entity,
    division,
    desk_code,
    desk_name,
    book_code,
    regulatory_book,
    asset_class,
    cp_code,
    COALESCE(cp_name, 'UNALLOCATED') AS cp_name,
    cp_lei,
    cp_country,
    credit_bucket,
    settlement_basis,
    exposure_band,
    net_amount_usd,
    market_value_usd,
    mtm_pnl_usd,
    accrued_interest_usd,
    total_pnl_usd,
    capital_add_on_usd,
    trade_count
FROM banded;
