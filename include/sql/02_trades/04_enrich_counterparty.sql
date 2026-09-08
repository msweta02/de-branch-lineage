-- target: trd.trades_enriched
-- depends: trd.trades_amended, ref.book_hierarchy, ref.counterparty, ref.calendar
--
-- Attach the book hierarchy and the counterparty master. Both joins are
-- multi-column: the reference records are validity-bounded, so a trade must
-- fall inside the record's effective window as well as matching on code.
--
-- Both are LEFT joins with an explicit unmatched marker, because dropping a
-- trade because its book is missing from the hierarchy would be a silent loss
-- of notional -- exactly the failure mode this project exists to surface.

CREATE OR REPLACE TABLE trd.trades_enriched AS
WITH with_book AS (
    SELECT
        t.*,
        b.desk_code,
        b.desk_name,
        b.division,
        b.legal_entity,
        b.accounting_treatment,
        b.book_code IS NULL AS book_unmatched
    FROM trd.trades_amended t
    LEFT JOIN ref.book_hierarchy b
           ON t.book_code  = b.book_code
          AND t.trade_date >= b.valid_from
          AND t.trade_date <= b.valid_to
),

with_counterparty AS (
    SELECT
        w.*,
        c.cp_name,
        c.cp_lei,
        c.country          AS cp_country,
        c.credit_rating,
        c.credit_bucket,
        c.netting_agreement,
        c.settlement_basis,
        c.cp_code IS NULL  AS cp_unmatched
    FROM with_book w
    LEFT JOIN ref.counterparty c
           ON w.cp_code    = c.cp_code
          AND w.trade_date >= c.valid_from
          AND w.trade_date <= c.valid_to
)

SELECT
    e.*,
    CASE
        WHEN e.book_unmatched AND e.cp_unmatched THEN 'BOOK_AND_CP_MISSING'
        WHEN e.book_unmatched                    THEN 'BOOK_MISSING'
        WHEN e.cp_unmatched                      THEN 'CP_MISSING'
        ELSE                                          'ENRICHED'
    END AS enrichment_status
FROM with_counterparty e;
