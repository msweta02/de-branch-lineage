-- Where the shortfall surfaces.
--
-- sum(base_amount) ignores nulls, so the total for 2026-09-03 comes back light
-- by the value of the two GBP trades booked that day. No null appears in this
-- model's output and no row is missing -- only the number is wrong.

with converted as (

    select * from {{ ref('fct_trades_converted') }}

)

select
    trade_date,
    book_code,
    count(*)                as trade_count,
    count(base_amount)      as converted_trade_count,
    sum(gross_amount_local) as gross_amount_local,
    sum(base_amount)        as base_amount_usd
from converted
group by trade_date, book_code
order by trade_date, book_code
