-- target: rpt.desk_summary
-- depends: rpt.regulatory_extract, ref.book_hierarchy
--
-- THE REPORT SOMEONE ACTUALLY READS.
--
-- One row per (business_date, desk). This is where a number being wrong gets
-- noticed, and it is fifteen-plus reconciliation hops and three module
-- boundaries away from anything that could have caused it. Nothing on this row
-- carries a null, a warning or a status column -- the aggregates absorbed all
-- of that upstream.

CREATE OR REPLACE TABLE rpt.desk_summary AS
WITH extract_rows AS (
    SELECT
        r.business_date,
        r.desk_code,
        r.desk_name,
        r.division,
        r.legal_entity,
        r.regulatory_book,
        r.asset_class,
        r.cp_code,
        r.net_amount_usd,
        r.market_value_usd,
        r.mtm_pnl_usd,
        r.accrued_interest_usd,
        r.total_pnl_usd,
        r.capital_add_on_usd,
        r.trade_count
    FROM rpt.regulatory_extract r
),

by_desk AS (
    SELECT
        business_date,
        desk_code,
        desk_name,
        division,
        COUNT(DISTINCT cp_code)          AS counterparty_count,
        SUM(trade_count)                 AS trade_count,
        SUM(net_amount_usd)              AS net_amount_usd,
        SUM(market_value_usd)            AS market_value_usd,
        SUM(mtm_pnl_usd)                 AS mtm_pnl_usd,
        SUM(accrued_interest_usd)        AS accrued_interest_usd,
        SUM(total_pnl_usd)               AS total_pnl_usd,
        SUM(capital_add_on_usd)          AS capital_add_on_usd
    FROM extract_rows
    GROUP BY business_date, desk_code, desk_name, division
),

ranked AS (
    SELECT
        d.*,
        RANK() OVER (
            PARTITION BY d.business_date
            ORDER BY d.total_pnl_usd DESC
        ) AS pnl_rank_in_day,
        SUM(d.total_pnl_usd) OVER (
            PARTITION BY d.business_date
        ) AS firm_total_pnl_usd
    FROM by_desk d
)

SELECT
    business_date,
    desk_code,
    desk_name,
    division,
    counterparty_count,
    trade_count,
    net_amount_usd,
    market_value_usd,
    mtm_pnl_usd,
    accrued_interest_usd,
    total_pnl_usd,
    capital_add_on_usd,
    pnl_rank_in_day,
    firm_total_pnl_usd,
    CASE
        WHEN firm_total_pnl_usd = 0 THEN 0.0
        ELSE total_pnl_usd / firm_total_pnl_usd
    END AS pnl_share_of_firm
FROM ranked
ORDER BY business_date, desk_code;
