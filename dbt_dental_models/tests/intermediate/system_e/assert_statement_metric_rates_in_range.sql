{{ config(severity='warn') }}

-- Failing rows: statement metric rates outside [0, 1].
-- Warn only: balance_collection_rate_30days can exceed 1 when 30d payments
-- exceed statement balances; collection_payment_rate can exceed 1 because
-- the numerator counts all paying statements while the denominator is
-- collection-flagged statements only.
SELECT
    statement_metric_id,
    metric_type,
    delivery_method,
    collection_statement_ratio,
    statement_payment_rate,
    collection_payment_rate,
    balance_collection_rate_30days,
    collection_balance_rate_30days,
    full_payment_rate
FROM {{ ref('int_statement_metrics') }}
WHERE
    collection_statement_ratio < 0 OR collection_statement_ratio > 1
    OR statement_payment_rate < 0 OR statement_payment_rate > 1
    OR collection_payment_rate < 0 OR collection_payment_rate > 1
    OR balance_collection_rate_30days < 0 OR balance_collection_rate_30days > 1
    OR collection_balance_rate_30days < 0 OR collection_balance_rate_30days > 1
    OR full_payment_rate < 0 OR full_payment_rate > 1
