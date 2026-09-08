-- Staging: one rate per (rate_date, ccy).
--
-- Nothing here can detect the missing GBP row for 2026-09-03. A row that never
-- arrived cannot fail a not-null test on a table it is not in.

with source as (

    select * from {{ ref('fx_rates') }}

),

typed as (

    select
        cast(rate_date as date) as rate_date,
        ccy,
        cast(rate as double)    as rate
    from source

)

select * from typed
