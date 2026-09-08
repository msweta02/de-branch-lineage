-- target: pos.positions_accrued
-- depends: pos.positions_marked, ref.calendar, raw.rate_curve
--
-- Overnight funding accrual on the marked position, ACT/360.
--
-- accrual_basis builds one row per (date, currency) that is eligible to accrue.
-- A date is eligible when it falls inside its accrual period, and the period is
-- half-open: on or after accrual_period_start, strictly before
-- accrual_period_end. That is the standard convention, and it is correct
-- provided accrual_period_end is the first day of the FOLLOWING month, which is
-- what ref.calendar documents it to hold.
--
-- Positions are LEFT JOINed to the basis rather than filtered by it, so a date
-- with no eligible basis keeps its position row and simply accrues nothing.
-- That is why an excluded date does not vanish from the report -- it just comes
-- back slightly light, on one day a month, in a column nobody reconciles.

CREATE OR REPLACE TABLE pos.positions_accrued AS
WITH accrual_periods AS (
    SELECT
        calendar_date,
        accrual_period_start,
        accrual_period_end,
        COALESCE(day_count_from_prev, 1) AS accrual_days
    FROM ref.calendar
),

overnight AS (
    SELECT
        curve_date,
        ccy,
        rate            AS overnight_rate,
        day_count_basis
    FROM raw.rate_curve
    WHERE tenor = 'ON'
),

accrual_basis AS (
    SELECT
        p.calendar_date AS business_date,
        o.ccy,
        o.overnight_rate,
        o.day_count_basis,
        p.accrual_days,
        p.accrual_period_start,
        p.accrual_period_end
    FROM accrual_periods p
    JOIN overnight o
      ON o.curve_date = p.calendar_date
    WHERE p.calendar_date >= p.accrual_period_start
      AND p.calendar_date <  p.accrual_period_end
),

accrued AS (
    SELECT
        m.*,
        b.accrual_period_start,
        b.accrual_period_end,
        b.accrual_days,
        b.overnight_rate,
        b.day_count_basis,
        m.net_amount_usd
            * b.overnight_rate
            * (CAST(b.accrual_days AS DOUBLE) / b.day_count_basis) AS accrued_interest_usd
    FROM pos.positions_marked m
    LEFT JOIN accrual_basis b
           ON m.business_date = b.business_date
          AND m.currency      = b.ccy
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
    market_value_usd,
    mtm_pnl_usd,
    valuation_status,
    accrual_period_start,
    accrual_period_end,
    accrual_days,
    overnight_rate,
    accrued_interest_usd,
    CASE
        WHEN accrued_interest_usd IS NOT NULL THEN 'ACCRUED'
        WHEN overnight_rate IS NULL           THEN 'NO_CURVE'
        ELSE                                       'OUTSIDE_ACCRUAL_PERIOD'
    END AS accrual_status
FROM accrued;
