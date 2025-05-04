-- Test that billing statements have valid dates
SELECT
    billing_statement_id,
    statement_id,
    date_sent,
    date_range_from,
    date_range_to
FROM {{ ref('int_billing_statements') }}
WHERE 
    date_sent IS NULL
    OR (date_range_from > date_range_to)
    OR date_sent < '2000-01-01';

-- Test for valid payment amounts
SELECT
    billing_statement_id,
    statement_id,
    payment_amount_7days,
    payment_amount_14days,
    payment_amount_30days
FROM {{ ref('int_billing_statements') }}
WHERE 
    payment_amount_7days < 0
    OR payment_amount_14days < 0
    OR payment_amount_30days < 0
    OR payment_amount_7days > payment_amount_14days
    OR payment_amount_14days > payment_amount_30days;

-- Test statement metrics for valid rate calculations
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
    OR full_payment_rate < 0 OR full_payment_rate > 1;

-- Test that statements with payments have valid payment results
SELECT
    billing_statement_id,
    statement_id,
    payment_amount_30days,
    resulted_in_payment,
    payment_result
FROM {{ ref('int_billing_statements') }}
WHERE 
    (payment_amount_30days > 0 AND resulted_in_payment = FALSE)
    OR (payment_amount_30days = 0 AND resulted_in_payment = TRUE)
    OR (payment_amount_30days > 0 AND payment_result = 'no_payment')
    OR (payment_amount_30days = 0 AND payment_result != 'no_payment');

-- Test that campaign statement counts match between models
WITH CampaignStatementCounts AS (
    SELECT
        campaign_id,
        COUNT(DISTINCT billing_statement_id) AS statement_count_from_statements
    FROM {{ ref('int_billing_statements') }}
    WHERE campaign_id IS NOT NULL
    GROUP BY campaign_id
),
MetricStatementCounts AS (
    SELECT
        campaign_id,
        total_statements AS statement_count_from_metrics
    FROM {{ ref('int_statement_metrics') }}
    WHERE 
        metric_type = 'by_campaign'
        AND campaign_id IS NOT NULL
)
SELECT
    csc.campaign_id,
    csc.statement_count_from_statements,
    msc.statement_count_from_metrics
FROM CampaignStatementCounts csc
JOIN MetricStatementCounts msc
    ON csc.campaign_id = msc.campaign_id
WHERE csc.statement_count_from_statements != msc.statement_count_from_metrics;