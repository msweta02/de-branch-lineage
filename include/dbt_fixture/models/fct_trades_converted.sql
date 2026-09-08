-- The join that misses.
--
-- A left join, deliberately -- an inner join would delete the trade rather than
-- leave it unvalued. The consequence is base_amount = null for any trade whose
-- (currency, trade_date) has no rate, and null is what the aggregate in
-- fct_desk_summary silently skips.

with trades as (

    select * from {{ ref('stg_trades') }}

),

rates as (

    select * from {{ ref('stg_fx_rates') }}

),

converted as (

    select
        t.trade_id,
        t.trade_date,
        t.book_code,
        t.currency,
        t.quantity,
        t.price,
        t.gross_amount_local,
        r.rate                        as fx_rate,
        t.gross_amount_local * r.rate as base_amount,
        case
            when r.rate is not null then 'CONVERTED'
            else 'RATE_NOT_FOUND'
        end as conversion_status
    from trades t
    left join rates r
           on t.currency   = r.ccy
          and t.trade_date = r.rate_date

)

select * from converted
