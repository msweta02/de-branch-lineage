-- target: trd.trades_ingested
-- depends: raw.trades, ref.calendar
--
-- Landing-to-staging. Types are asserted, the trade date is validated against
-- the calendar, and the raw feed's flags are normalised into a single event
-- type so the dedupe and amendment stages downstream have one column to reason
-- about instead of three booleans.

CREATE OR REPLACE TABLE trd.trades_ingested AS
WITH raw_feed AS (
    SELECT
        t.trade_id,
        t.trade_version,
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
        t.trader_id,
        t.source_system,
        t.amend_flag,
        t.cancel_flag,
        t.ingested_at
    FROM raw.trades t
),

typed AS (
    SELECT
        r.trade_id,
        r.trade_version,
        r.event_ts,
        r.trade_date,
        r.value_date,
        r.book_code,
        r.cp_code,
        r.instrument_id,
        r.product_type,
        r.currency,
        CAST(r.quantity AS DOUBLE) AS quantity,
        CAST(r.price    AS DOUBLE) AS price,
        r.direction,
        CASE WHEN r.direction = 'BUY' THEN 1 ELSE -1 END AS direction_sign,
        r.trader_id,
        r.source_system,
        r.ingested_at,
        CASE
            WHEN r.cancel_flag THEN 'CANCEL'
            WHEN r.amend_flag  THEN 'AMEND'
            ELSE                    'NEW'
        END AS event_type
    FROM raw_feed r
)

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
    c.is_business_day,
    c.day_type
FROM typed t
JOIN ref.calendar c
  ON t.trade_date = c.calendar_date;
