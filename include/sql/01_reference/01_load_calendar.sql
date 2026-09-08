-- target: ref.calendar
-- depends: raw.calendar
--
-- Trading calendar. Everything downstream that needs to know whether a date is
-- a business day, which accrual period it falls in, or what the previous
-- business day was reads this table.
--
-- accrual_period_end arrives from the source system and is passed through
-- unchanged. It is documented as an EXCLUSIVE upper bound: the first day of the
-- following month. 03_positions/04_accrue_interest.sql consumes it as such.

CREATE OR REPLACE TABLE ref.calendar AS
WITH src AS (
    SELECT
        calendar_date,
        is_business_day,
        is_weekend,
        holiday_name,
        accrual_period_start,
        accrual_period_end
    FROM raw.calendar
),

classified AS (
    SELECT
        calendar_date,
        is_business_day,
        is_weekend,
        holiday_name,
        accrual_period_start,
        accrual_period_end,
        DATE_TRUNC('month', calendar_date)          AS month_start,
        LAST_DAY(calendar_date)                     AS month_end,
        EXTRACT(DAY   FROM calendar_date)           AS day_of_month,
        EXTRACT(MONTH FROM calendar_date)           AS month_no,
        EXTRACT(YEAR  FROM calendar_date)           AS year_no,
        calendar_date = LAST_DAY(calendar_date)     AS is_month_end,
        CASE
            WHEN is_weekend                THEN 'WEEKEND'
            WHEN holiday_name IS NOT NULL  THEN 'HOLIDAY'
            WHEN is_business_day           THEN 'BUSINESS'
            ELSE 'UNKNOWN'
        END                                         AS day_type
    FROM src
),

sequenced AS (
    SELECT
        c.*,
        ROW_NUMBER() OVER (
            PARTITION BY c.month_start
            ORDER BY c.calendar_date
        )                                                       AS day_seq_in_month,
        LAG(CASE WHEN c.is_business_day THEN c.calendar_date END IGNORE NULLS)
            OVER (ORDER BY c.calendar_date)                     AS prev_business_date,
        LEAD(CASE WHEN c.is_business_day THEN c.calendar_date END IGNORE NULLS)
            OVER (ORDER BY c.calendar_date)                     AS next_business_date,
        SUM(CASE WHEN c.is_business_day THEN 1 ELSE 0 END)
            OVER (PARTITION BY c.month_start ORDER BY c.calendar_date)
                                                                AS business_days_mtd
    FROM classified c
)

SELECT
    calendar_date,
    day_type,
    is_business_day,
    is_month_end,
    day_of_month,
    month_no,
    year_no,
    month_start,
    month_end,
    day_seq_in_month,
    business_days_mtd,
    prev_business_date,
    next_business_date,
    accrual_period_start,
    accrual_period_end,
    DATE_DIFF('day', prev_business_date, calendar_date) AS day_count_from_prev,
    holiday_name
FROM sequenced
ORDER BY calendar_date;
