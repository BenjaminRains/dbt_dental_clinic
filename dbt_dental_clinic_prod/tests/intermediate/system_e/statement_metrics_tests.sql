-- Test statement metrics for valid rate calculations
-- This test returns records with invalid rate values (outside 0-1 range)
SELECT
    statement_metric_id,
    metric_type,
    delivery_method,
    collection_statement_ratio,
    statement_payment_rate,
    collection_payment_rate,
    balance_collection_rate_30days,
    collection_balance_rate_30days,
    full_payment_rate,
    CASE
        WHEN collection_statement_ratio < 0 OR collection_statement_ratio > 1 THEN 'Invalid collection_statement_ratio'
        WHEN statement_payment_rate < 0 OR statement_payment_rate > 1 THEN 'Invalid statement_payment_rate'
        WHEN collection_payment_rate < 0 OR collection_payment_rate > 1 THEN 'Invalid collection_payment_rate'
        WHEN balance_collection_rate_30days < 0 OR balance_collection_rate_30days > 1 THEN 'Invalid balance_collection_rate_30days'
        WHEN collection_balance_rate_30days < 0 OR collection_balance_rate_30days > 1 THEN 'Invalid collection_balance_rate_30days'
        WHEN full_payment_rate < 0 OR full_payment_rate > 1 THEN 'Invalid full_payment_rate'
        ELSE 'Unknown validation error'
    END AS validation_error
FROM {{ ref('int_statement_metrics') }}
WHERE 
    collection_statement_ratio < 0 OR collection_statement_ratio > 1
    OR statement_payment_rate < 0 OR statement_payment_rate > 1
    OR collection_payment_rate < 0 OR collection_payment_rate > 1
    OR balance_collection_rate_30days < 0 OR balance_collection_rate_30days > 1
    OR collection_balance_rate_30days < 0 OR collection_balance_rate_30days > 1
    OR full_payment_rate < 0 OR full_payment_rate > 1
