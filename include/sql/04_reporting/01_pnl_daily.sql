-- target: rpt.pnl_daily
-- depends: pos.positions_netted, ref.calendar
--
-- Daily P&L by book and counterparty: mark-to-market plus accrual, with a
-- day-on-day movement column computed over the book's own history.

CREATE OR REPLACE TABLE rpt.pnl_daily AS
WITH base AS (
    SELECT
        n.business_date,
        n.book_code,
        n.desk_code,
        n.desk_name,
        n.division,
        n.legal_entity,
        n.regulatory_book,
        n.asset_class,
        n.currency,
        n.cp_code,
        n.credit_bucket,
        n.settlement_basis,
        n.net_amount_usd,
        n.market_value_usd,
        n.mtm_pnl_usd,
        n.accrued_interest_usd,
        n.regulatory_add_on_usd,
        n.apportioned_trade_count
    FROM pos.positions_netted n
),

daily AS (
    SELECT
        b.business_date,
        b.book_code,
        b.desk_code,
        b.desk_name,
        b.division,
        b.legal_entity,
        b.regulatory_book,
        b.asset_class,
        b.cp_code,
        b.credit_bucket,
        b.settlement_basis,
        SUM(b.net_amount_usd)          AS net_amount_usd,
        SUM(b.market_value_usd)        AS market_value_usd,
        SUM(b.mtm_pnl_usd)             AS mtm_pnl_usd,
        SUM(b.accrued_interest_usd)    AS accrued_interest_usd,
        SUM(b.regulatory_add_on_usd)   AS regulatory_add_on_usd,
        SUM(b.apportioned_trade_count) AS trade_count
    FROM base b
    GROUP BY
        b.business_date, b.book_code, b.desk_code, b.desk_name, b.division,
        b.legal_entity, b.regulatory_book, b.asset_class, b.cp_code,
        b.credit_bucket, b.settlement_basis
),

with_movement AS (
    SELECT
        d.*,
        COALESCE(d.mtm_pnl_usd, 0) + COALESCE(d.accrued_interest_usd, 0) AS total_pnl_usd,
        LAG(d.mtm_pnl_usd) OVER (
            PARTITION BY d.book_code, d.cp_code
            ORDER BY d.business_date
        ) AS prev_mtm_pnl_usd
    FROM daily d
)

SELECT
    w.*,
    c.is_business_day,
    c.is_month_end,
    w.mtm_pnl_usd - COALESCE(w.prev_mtm_pnl_usd, 0) AS mtm_movement_usd,
    CASE
        WHEN w.total_pnl_usd >  1000000 THEN 'LARGE_GAIN'
        WHEN w.total_pnl_usd >        0 THEN 'GAIN'
        WHEN w.total_pnl_usd =        0 THEN 'FLAT'
        WHEN w.total_pnl_usd > -1000000 THEN 'LOSS'
        ELSE                                 'LARGE_LOSS'
    END AS pnl_band
FROM with_movement w
JOIN ref.calendar c
  ON w.business_date = c.calendar_date;
