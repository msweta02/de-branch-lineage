-- target: trd.trades_amended
-- depends: trd.trades_deduped
--
-- Supersession. A trade can arrive as v1 and later be amended (v2, restated
-- quantity) or cancelled (v2, cancel event). Only the highest version survives,
-- and cancelled trades are dropped entirely rather than carried with a zero
-- quantity -- the risk systems downstream treat a zero-quantity row and an
-- absent row differently.

CREATE OR REPLACE TABLE trd.trades_amended AS
WITH versioned AS (
    SELECT
        d.*,
        ROW_NUMBER() OVER (
            PARTITION BY d.trade_id
            ORDER BY d.trade_version DESC, d.event_ts DESC
        )                                                  AS version_rank,
        MAX(d.trade_version) OVER (PARTITION BY d.trade_id) AS max_version,
        BOOL_OR(d.event_type = 'CANCEL')
            OVER (PARTITION BY d.trade_id)                 AS ever_cancelled
    FROM trd.trades_deduped d
),

current_version AS (
    SELECT
        v.trade_id,
        v.trade_version,
        v.event_type,
        v.event_ts,
        v.trade_date,
        v.value_date,
        v.book_code,
        v.cp_code,
        v.instrument_id,
        v.product_type,
        v.currency,
        v.quantity,
        v.price,
        v.direction,
        v.direction_sign,
        v.trader_id,
        v.source_system,
        v.is_business_day,
        v.day_type,
        v.max_version,
        v.ever_cancelled,
        CASE
            WHEN v.max_version > 1 THEN 'AMENDED'
            ELSE                        'ORIGINAL'
        END AS amendment_status
    FROM versioned v
    WHERE v.version_rank = 1
)

SELECT
    *,
    quantity * price AS gross_amount_local
FROM current_version
WHERE NOT ever_cancelled;
