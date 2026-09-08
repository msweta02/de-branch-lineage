-- target: ref.fx_rates
-- depends: raw.fx_rates, ref.calendar
--
-- Daily FX rates against USD, one row per (rate_date, ccy).
--
-- The vendor feed sometimes delivers a late second tick for the same pair on
-- the same date. We keep the latest ingested row per (rate_date, ccy) and
-- record the runner-up's source for audit. USD is synthesised as an identity
-- rate rather than sourced, because no vendor quotes USD/USD.
--
-- Rows that arrive with a null or non-positive rate are rejected here. Rows
-- that never arrive at all cannot be rejected here -- there is nothing to
-- reject. That distinction is the whole of BUG A.

CREATE OR REPLACE TABLE ref.fx_rates AS
WITH vendor AS (
    SELECT
        rate_date,
        ccy,
        quote_ccy,
        rate,
        rate_source,
        ingested_at
    FROM raw.fx_rates
    WHERE rate IS NOT NULL
      AND rate > 0
),

ranked AS (
    SELECT
        v.*,
        ROW_NUMBER() OVER (
            PARTITION BY v.rate_date, v.ccy
            ORDER BY v.ingested_at DESC, v.rate_source
        ) AS ingest_rank,
        COUNT(*) OVER (PARTITION BY v.rate_date, v.ccy) AS tick_count
    FROM vendor v
),

winning AS (
    SELECT
        rate_date,
        ccy,
        quote_ccy,
        rate,
        rate_source,
        ingested_at,
        tick_count,
        CASE
            WHEN tick_count > 1 THEN 'DEDUPED_LATEST'
            ELSE 'SINGLE_TICK'
        END AS rate_provenance
    FROM ranked
    WHERE ingest_rank = 1
),

identity_rate AS (
    SELECT
        c.calendar_date AS rate_date,
        'USD'           AS ccy,
        'USD'           AS quote_ccy,
        1.0             AS rate,
        'SYNTHETIC'     AS rate_source,
        NULL::TIMESTAMP AS ingested_at,
        1               AS tick_count,
        'IDENTITY'      AS rate_provenance
    FROM ref.calendar c
)

SELECT * FROM winning WHERE ccy <> 'USD'
UNION ALL
SELECT * FROM identity_rate
ORDER BY rate_date, ccy;
