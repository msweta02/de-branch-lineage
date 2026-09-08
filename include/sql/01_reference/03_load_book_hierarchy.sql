-- target: ref.book_hierarchy
-- depends: raw.book_hierarchy
--
-- Books to desks to divisions. The source is slowly-changing: a book that is
-- re-parented arrives as a closed record plus an open one. We collapse to the
-- currently-effective record per book.

CREATE OR REPLACE TABLE ref.book_hierarchy AS
WITH scd AS (
    SELECT
        book_code,
        book_name,
        desk_code,
        desk_name,
        division,
        legal_entity,
        valid_from,
        valid_to,
        ROW_NUMBER() OVER (
            PARTITION BY book_code
            ORDER BY valid_to DESC, valid_from DESC
        ) AS recency_rank,
        COUNT(*) OVER (PARTITION BY book_code) AS version_count
    FROM raw.book_hierarchy
)

SELECT
    book_code,
    book_name,
    desk_code,
    desk_name,
    division,
    legal_entity,
    valid_from,
    valid_to,
    version_count > 1 AS was_reparented,
    CASE
        WHEN division = 'BANKING' THEN 'ACCRUAL'
        ELSE 'TRADING'
    END AS accounting_treatment
FROM scd
WHERE recency_rank = 1
ORDER BY book_code;
