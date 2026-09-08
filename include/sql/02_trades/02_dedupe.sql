-- target: trd.trades_deduped
-- depends: trd.trades_ingested
--
-- The upstream feeds occasionally deliver the same (trade_id, trade_version)
-- twice, typically when BOOKIT replays a batch. Keep one row per
-- (trade_id, trade_version), preferring the earliest ingest so that a replay
-- does not overwrite the original timestamps.
--
-- QUALIFY rather than a self-join: the duplicate count is carried forward so
-- the control report can see how much the feed replayed.

CREATE OR REPLACE TABLE trd.trades_deduped AS
SELECT
    t.trade_id,
    t.trade_version,
    t.event_type,
    t.event_ts,
    t.trade_date,
    t.value_date,
    t.book_code,
    t.cp_code,
    t.instrument_id,
    t.product_type,
    t.currency,
    t.quantity,
    t.price,
    t.direction,
    t.direction_sign,
    t.trader_id,
    t.source_system,
    t.ingested_at,
    t.is_business_day,
    t.day_type,
    COUNT(*) OVER (PARTITION BY t.trade_id, t.trade_version) AS landing_count
FROM trd.trades_ingested t
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY t.trade_id, t.trade_version
    ORDER BY t.ingested_at ASC, t.source_system ASC
) = 1;
