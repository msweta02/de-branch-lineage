-- target: pos.positions_netted
-- depends: pos.positions_accrued, trd.trades_classified, ref.counterparty
--
-- Roll the instrument-level positions up to the netting set. Exposure nets
-- across instruments where a netting agreement is in force and is summed gross
-- where it is not.
--
-- The counterparty attribution comes back from the trade blotter, because the
-- position grain dropped cp_code at 01_aggregate_by_book. The join is on
-- (business_date, book_code, instrument_id) and fans out, so the measures are
-- apportioned by the counterparty's share of the day's traded amount.

CREATE OR REPLACE TABLE pos.positions_netted AS
WITH cp_share AS (
    SELECT
        t.trade_date AS business_date,
        t.book_code,
        t.instrument_id,
        t.cp_code,
        SUM(t.base_amount) AS cp_base_amount,
        SUM(SUM(t.base_amount)) OVER (
            PARTITION BY t.trade_date, t.book_code, t.instrument_id
        ) AS position_base_amount
    FROM trd.trades_classified t
    GROUP BY t.trade_date, t.book_code, t.instrument_id, t.cp_code
),

apportioned AS (
    SELECT
        p.business_date,
        p.book_code,
        p.desk_code,
        p.desk_name,
        p.division,
        p.legal_entity,
        p.instrument_id,
        p.asset_class,
        p.regulatory_book,
        p.currency,
        s.cp_code,
        CASE
            WHEN s.position_base_amount IS NULL OR s.position_base_amount = 0 THEN 0.0
            ELSE s.cp_base_amount / s.position_base_amount
        END AS cp_weight,
        p.trade_count,
        p.net_amount_usd,
        p.market_value_usd,
        p.mtm_pnl_usd,
        p.accrued_interest_usd,
        p.regulatory_add_on_usd
    FROM pos.positions_accrued p
    LEFT JOIN cp_share s
           ON p.business_date  = s.business_date
          AND p.book_code      = s.book_code
          AND p.instrument_id  = s.instrument_id
),

with_terms AS (
    SELECT
        a.*,
        c.credit_bucket,
        c.settlement_basis,
        c.netting_agreement
    FROM apportioned a
    LEFT JOIN ref.counterparty c
           ON a.cp_code = c.cp_code
)

SELECT
    business_date,
    book_code,
    desk_code,
    desk_name,
    division,
    legal_entity,
    regulatory_book,
    asset_class,
    currency,
    COALESCE(cp_code, 'UNALLOCATED')       AS cp_code,
    COALESCE(credit_bucket, 'UNRATED')     AS credit_bucket,
    COALESCE(settlement_basis, 'GROSS')    AS settlement_basis,
    SUM(net_amount_usd       * cp_weight)  AS net_amount_usd,
    SUM(market_value_usd     * cp_weight)  AS market_value_usd,
    SUM(mtm_pnl_usd          * cp_weight)  AS mtm_pnl_usd,
    SUM(accrued_interest_usd * cp_weight)  AS accrued_interest_usd,
    SUM(regulatory_add_on_usd * cp_weight) AS regulatory_add_on_usd,
    SUM(trade_count * cp_weight)           AS apportioned_trade_count,
    COUNT(*)                               AS position_legs
FROM with_terms
GROUP BY
    business_date, book_code, desk_code, desk_name, division, legal_entity,
    regulatory_book, asset_class, currency,
    COALESCE(cp_code, 'UNALLOCATED'),
    COALESCE(credit_bucket, 'UNRATED'),
    COALESCE(settlement_basis, 'GROSS');
