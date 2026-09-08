-- target: trd.trades_converted
-- depends: trd.trades_enriched, ref.fx_rates, ref.validity_window
--
-- Convert each trade's local-currency gross amount into the USD reporting
-- currency.
--
-- The FX join is a LEFT JOIN on (currency, trade_date). It is a LEFT JOIN on
-- purpose: an INNER JOIN would delete the trade outright if the rate were
-- absent, and a vanished trade is worse than a trade with an unknown value.
-- The consequence is that a missing rate produces base_amount = NULL rather
-- than a missing row, and NULL is the value that every aggregate downstream
-- silently ignores.
--
-- ref.validity_window is consulted so trades outside the covered date range are
-- marked. It cannot see a hole in the middle of the range -- see the note in
-- 01_reference/05_ref_validity_window.sql.

CREATE OR REPLACE TABLE trd.trades_converted AS
WITH priced AS (
    SELECT
        e.*,
        e.gross_amount_local * e.direction_sign AS signed_amount_local
    FROM trd.trades_enriched e
),

converted AS (
    SELECT
        p.*,
        f.rate            AS fx_rate,
        f.rate_source     AS fx_source,
        f.rate_provenance AS fx_provenance,
        p.gross_amount_local  * f.rate AS base_amount,
        p.signed_amount_local * f.rate AS signed_base_amount
    FROM priced p
    LEFT JOIN ref.fx_rates f
           ON p.currency   = f.ccy
          AND p.trade_date = f.rate_date
),

windowed AS (
    SELECT
        c.*,
        w.coverage_status,
        c.trade_date BETWEEN w.covered_from AND w.covered_to AS inside_coverage
    FROM converted c
    LEFT JOIN ref.validity_window w
           ON c.currency = w.ccy
)

SELECT
    *,
    CASE
        WHEN fx_rate IS NOT NULL              THEN 'CONVERTED'
        WHEN NOT inside_coverage              THEN 'OUTSIDE_COVERAGE'
        ELSE                                       'RATE_NOT_FOUND'
    END AS conversion_status
FROM windowed;
