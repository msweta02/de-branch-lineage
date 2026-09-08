-- target: pos.positions_marked
-- depends: pos.positions_adjusted, raw.marks, ref.fx_rates
--
-- Revalue each position at the official end-of-day mark and book the unrealised
-- P&L against the traded amount.
--
-- mtm_pnl_usd is derived from net_amount_usd, which is derived from
-- base_amount. Where base_amount was NULL the whole chain is NULL, and the SUM
-- at the desk level simply returns a smaller number. No row is dropped, no
-- error is raised, and no null appears in the final report -- the aggregate
-- absorbs it. This is the hop where BUG A stops being visible.

CREATE OR REPLACE TABLE pos.positions_marked AS
WITH official_marks AS (
    SELECT
        mark_date,
        instrument_id,
        mark_price,
        mark_source
    FROM raw.marks
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY mark_date, instrument_id
        ORDER BY ingested_at DESC
    ) = 1
),

marked AS (
    SELECT
        p.*,
        m.mark_price,
        m.mark_source,
        f.rate AS mark_fx_rate
    FROM pos.positions_adjusted p
    LEFT JOIN official_marks m
           ON p.instrument_id = m.instrument_id
          AND p.business_date = m.mark_date
    LEFT JOIN ref.fx_rates f
           ON p.currency      = f.ccy
          AND p.business_date = f.rate_date
),

valued AS (
    SELECT
        m.*,
        m.net_quantity * m.mark_price * m.mark_fx_rate AS market_value_usd
    FROM marked m
)

SELECT
    business_date,
    book_code,
    desk_code,
    desk_name,
    division,
    legal_entity,
    instrument_id,
    product_type,
    asset_class,
    regulatory_book,
    currency,
    trade_count,
    converted_trade_count,
    unconverted_count,
    net_quantity,
    gross_quantity,
    gross_amount_local,
    gross_amount_usd,
    net_amount_usd,
    regulatory_add_on_usd,
    counterparty_count,
    corporate_action_applied,
    mark_price,
    mark_source,
    mark_fx_rate,
    market_value_usd,
    market_value_usd - net_amount_usd AS mtm_pnl_usd,
    CASE
        WHEN mark_price IS NULL       THEN 'UNMARKED'
        WHEN market_value_usd IS NULL THEN 'UNVALUED'
        ELSE                               'MARKED'
    END AS valuation_status
FROM valued;
