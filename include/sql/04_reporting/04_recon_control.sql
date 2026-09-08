-- target: rpt.recon_control
-- depends: rpt.desk_summary, pos.positions_netted, trd.trades_classified
--
-- The control that is supposed to catch all of this.
--
-- It ties the published desk summary back to the position layer and the trade
-- blotter. Note what it compares: trade COUNTS. Counts tie perfectly under BUG
-- A, because no row is ever lost -- only values go null. The amount comparison
-- is against pos.positions_netted, which is itself downstream of the break, so
-- both sides of that check are wrong by the same amount and it reconciles to
-- zero.
--
-- A control that reconciles a number against a number derived from it is not a
-- control. Finding this out is normally what a week of incident review is for.

CREATE OR REPLACE TABLE rpt.recon_control AS
WITH report_side AS (
    SELECT
        business_date,
        SUM(trade_count)      AS report_trade_count,
        SUM(net_amount_usd)   AS report_net_amount_usd,
        SUM(total_pnl_usd)    AS report_total_pnl_usd
    FROM rpt.desk_summary
    GROUP BY business_date
),

position_side AS (
    SELECT
        business_date,
        SUM(apportioned_trade_count) AS position_trade_count,
        SUM(net_amount_usd)          AS position_net_amount_usd
    FROM pos.positions_netted
    GROUP BY business_date
),

blotter_side AS (
    SELECT
        trade_date AS business_date,
        COUNT(*)   AS blotter_trade_count
    FROM trd.trades_classified
    GROUP BY trade_date
)

SELECT
    r.business_date,
    r.report_trade_count,
    p.position_trade_count,
    b.blotter_trade_count,
    r.report_net_amount_usd,
    p.position_net_amount_usd,
    r.report_total_pnl_usd,
    ROUND(r.report_trade_count - p.position_trade_count, 6)       AS count_break_vs_positions,
    b.blotter_trade_count - ROUND(p.position_trade_count)         AS count_break_vs_blotter,
    ROUND(r.report_net_amount_usd - p.position_net_amount_usd, 2) AS amount_break_usd,
    CASE
        WHEN ABS(r.report_net_amount_usd - p.position_net_amount_usd) < 0.01
         AND ABS(r.report_trade_count    - p.position_trade_count)    < 0.01
        THEN 'RECONCILED'
        ELSE 'BREAK'
    END AS control_status
FROM report_side r
LEFT JOIN position_side p ON r.business_date = p.business_date
LEFT JOIN blotter_side  b ON r.business_date = b.business_date
ORDER BY r.business_date;
