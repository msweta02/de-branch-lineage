-- Staging: type the raw trade feed and derive the local-currency amount.

with source as (

    select * from {{ ref('trades') }}

),

typed as (

    select
        trade_id,
        cast(trade_date as date)  as trade_date,
        book_code,
        currency,
        cast(quantity as double)  as quantity,
        cast(price    as double)  as price,
        cast(quantity as double) * cast(price as double) as gross_amount_local
    from source

)

select * from typed
