-- target: pos.positions_adjusted
-- depends: pos.book_positions, raw.corporate_actions
--
-- Apply splits and dividends on their ex-date. A split restates quantity by the
-- ratio and leaves value untouched; a dividend leaves quantity alone and adds
-- cash. Positions with no action on the date pass through unchanged.

CREATE OR REPLACE TABLE pos.positions_adjusted AS
WITH actions AS (
    SELECT
        instrument_id,
        ex_date,
        action_type,
        ratio,
        cash_amount,
        ROW_NUMBER() OVER (
            PARTITION BY instrument_id, ex_date
            ORDER BY action_id
        ) AS action_rank
    FROM raw.corporate_actions
),

joined AS (
    SELECT
        p.*,
        a.action_type,
        a.ratio,
        a.cash_amount
    FROM pos.book_positions p
    LEFT JOIN actions a
           ON p.instrument_id = a.instrument_id
          AND p.business_date = a.ex_date
          AND a.action_rank   = 1
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
    CASE
        WHEN action_type = 'SPLIT' THEN net_quantity * ratio
        ELSE                            net_quantity
    END AS net_quantity,
    CASE
        WHEN action_type = 'SPLIT' THEN gross_quantity * ratio
        ELSE                            gross_quantity
    END AS gross_quantity,
    gross_amount_local,
    gross_amount_usd,
    CASE
        WHEN action_type = 'DIVIDEND' THEN net_amount_usd + (net_quantity * cash_amount)
        ELSE                               net_amount_usd
    END AS net_amount_usd,
    regulatory_add_on_usd,
    fx_rate_applied,
    counterparty_count,
    COALESCE(action_type, 'NONE') AS corporate_action_applied
FROM joined;
