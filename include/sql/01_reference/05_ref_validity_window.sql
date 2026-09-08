-- target: ref.validity_window
-- depends: ref.fx_rates, ref.calendar, ref.book_hierarchy, ref.counterparty
--
-- Coverage control for the reference layer. Answers "for which date range do we
-- hold rates for each currency", and is joined by 02_trades/05_convert_currency
-- to reject trades outside the covered window before conversion is attempted.
--
-- NOTE: this checks the OUTER BOUNDS of coverage per currency, not that every
-- business day inside those bounds is present. A single missing day sits inside
-- min(rate_date)..max(rate_date) and passes this control untouched. That is
-- realistic -- and it is why BUG A survives the reference layer.

CREATE OR REPLACE TABLE ref.validity_window AS
WITH fx_coverage AS (
    SELECT
        ccy,
        MIN(rate_date)      AS covered_from,
        MAX(rate_date)      AS covered_to,
        COUNT(*)            AS rate_days,
        COUNT(DISTINCT rate_date) AS distinct_rate_days
    FROM ref.fx_rates
    GROUP BY ccy
),

calendar_span AS (
    SELECT
        MIN(calendar_date) AS window_from,
        MAX(calendar_date) AS window_to,
        COUNT(*)           AS calendar_days
    FROM ref.calendar
)

SELECT
    f.ccy,
    f.covered_from,
    f.covered_to,
    f.rate_days,
    f.distinct_rate_days,
    s.window_from,
    s.window_to,
    s.calendar_days,
    CASE
        WHEN f.covered_from <= s.window_from
         AND f.covered_to   >= s.window_to  THEN 'FULL_SPAN'
        WHEN f.covered_to   <  s.window_to  THEN 'STALE'
        ELSE                                     'PARTIAL'
    END AS coverage_status
FROM fx_coverage f
CROSS JOIN calendar_span s
ORDER BY f.ccy;
