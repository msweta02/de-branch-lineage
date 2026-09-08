-- target: ref.counterparty
-- depends: raw.counterparty
--
-- Counterparty master with a derived credit bucket. The bucket drives netting
-- eligibility in 03_positions/05_net_by_counterparty.sql and the exposure
-- banding in 04_reporting/02_regulatory_extract.sql.

CREATE OR REPLACE TABLE ref.counterparty AS
WITH latest AS (
    SELECT
        cp_code,
        cp_name,
        cp_lei,
        country,
        credit_rating,
        netting_agreement,
        valid_from,
        valid_to,
        ROW_NUMBER() OVER (
            PARTITION BY cp_code
            ORDER BY valid_from DESC
        ) AS recency_rank
    FROM raw.counterparty
)

SELECT
    cp_code,
    cp_name,
    cp_lei,
    country,
    credit_rating,
    netting_agreement,
    valid_from,
    valid_to,
    CASE
        WHEN credit_rating IN ('AAA', 'AA+', 'AA', 'AA-') THEN 'IG_HIGH'
        WHEN credit_rating IN ('A+', 'A', 'A-')           THEN 'IG_MID'
        WHEN credit_rating IN ('BBB+', 'BBB', 'BBB-')     THEN 'IG_LOW'
        ELSE                                                   'SUB_IG'
    END AS credit_bucket,
    CASE
        WHEN netting_agreement THEN 'NETTED'
        ELSE 'GROSS'
    END AS settlement_basis
FROM latest
WHERE recency_rank = 1
ORDER BY cp_code;
