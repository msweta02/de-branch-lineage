-- target: pos.book_positions
-- depends: trd.trades_classified
--
-- Collapse the trade blotter into positions: one row per
-- (business_date, book, instrument, currency).
--
-- SUM ignores NULLs. A book whose trades all converted and a book where a
-- tenth of them did not produce the same shape of row here, differing only in
-- a number. converted_trade_count is carried so the difference is at least
-- recoverable later, but nothing in this pipeline compares it to trade_count.

CREATE OR REPLACE TABLE pos.book_positions AS
WITH trade_level AS (
    SELECT
        t.trade_date        AS business_date,
        t.book_code,
        t.desk_code,
        t.desk_name,
        t.division,
        t.legal_entity,
        t.cp_code,
        t.credit_bucket,
        t.instrument_id,
        t.product_type,
        t.asset_class,
        t.regulatory_book,
        t.currency,
        t.size_tier,
        t.direction_sign,
        t.quantity,
        t.price,
        t.gross_amount_local,
        t.base_amount,
        t.signed_base_amount,
        t.fx_rate,
        t.conversion_status,
        t.regulatory_add_on
    FROM trd.trades_classified t
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
    COUNT(*)                                                     AS trade_count,
    COUNT(base_amount)                                           AS converted_trade_count,
    SUM(quantity * direction_sign)                               AS net_quantity,
    SUM(quantity)                                                AS gross_quantity,
    SUM(gross_amount_local)                                      AS gross_amount_local,
    SUM(base_amount)                                             AS gross_amount_usd,
    SUM(signed_base_amount)                                      AS net_amount_usd,
    SUM(regulatory_add_on)                                       AS regulatory_add_on_usd,
    MAX(fx_rate)                                                 AS fx_rate_applied,
    COUNT(DISTINCT cp_code)                                      AS counterparty_count,
    SUM(CASE WHEN conversion_status = 'CONVERTED' THEN 0 ELSE 1 END) AS unconverted_count
FROM trade_level
GROUP BY
    business_date, book_code, desk_code, desk_name, division, legal_entity,
    instrument_id, product_type, asset_class, regulatory_book, currency;
