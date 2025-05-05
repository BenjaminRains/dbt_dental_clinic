{{ config(
    materialized='incremental',
    schema='intermediate',
    unique_key=['snapshot_date', 'metric_type']
) }}

/*
    Intermediate model for statement metrics
    Part of System E: Collections
    
    This model:
    1. Measures statement effectiveness for collections
    2. Tracks statement response rates
    3. Compares different statement delivery methods
    4. Provides insights for optimizing collection strategies
*/

WITH 
-- Get base metrics for statements, grouped by various dimensions
StatementMetrics AS (
    -- Overall metrics
    SELECT
        CURRENT_DATE AS snapshot_date,
        'overall' AS metric_type,
        NULL AS delivery_method,
        NULL::integer AS campaign_id,
        COUNT(DISTINCT statement_id) AS total_statements,
        COUNT(DISTINCT CASE WHEN is_collection_statement THEN statement_id END) AS collection_statements,
        SUM(balance_total) AS total_balance,
        SUM(CASE WHEN is_collection_statement THEN balance_total ELSE 0 END) AS collection_balance,
        SUM(payment_amount_7days) AS payment_amount_7days,
        SUM(payment_amount_14days) AS payment_amount_14days,
        SUM(payment_amount_30days) AS payment_amount_30days,
        COUNT(DISTINCT CASE WHEN resulted_in_payment THEN statement_id END) AS statements_with_payment,
        COUNT(DISTINCT CASE WHEN payment_result = 'full_payment' THEN statement_id END) AS statements_with_full_payment,
        COUNT(DISTINCT CASE WHEN payment_result = 'partial_payment' THEN statement_id END) AS statements_with_partial_payment
    FROM {{ ref('int_billing_statements') }}
    WHERE date_sent >= CURRENT_DATE - INTERVAL '90 days'
    
    UNION ALL
    
    -- By delivery method
    SELECT
        CURRENT_DATE AS snapshot_date,
        'by_delivery_method' AS metric_type,
        delivery_method,
        NULL::integer AS campaign_id,
        COUNT(DISTINCT statement_id) AS total_statements,
        COUNT(DISTINCT CASE WHEN is_collection_statement THEN statement_id END) AS collection_statements,
        SUM(balance_total) AS total_balance,
        SUM(CASE WHEN is_collection_statement THEN balance_total ELSE 0 END) AS collection_balance,
        SUM(payment_amount_7days) AS payment_amount_7days,
        SUM(payment_amount_14days) AS payment_amount_14days,
        SUM(payment_amount_30days) AS payment_amount_30days,
        COUNT(DISTINCT CASE WHEN resulted_in_payment THEN statement_id END) AS statements_with_payment,
        COUNT(DISTINCT CASE WHEN payment_result = 'full_payment' THEN statement_id END) AS statements_with_full_payment,
        COUNT(DISTINCT CASE WHEN payment_result = 'partial_payment' THEN statement_id END) AS statements_with_partial_payment
    FROM {{ ref('int_billing_statements') }}
    WHERE date_sent >= CURRENT_DATE - INTERVAL '90 days'
    GROUP BY delivery_method
    
    UNION ALL
    
    -- By campaign (for statements linked to campaigns)
    SELECT
        CURRENT_DATE AS snapshot_date,
        'by_campaign' AS metric_type,
        NULL AS delivery_method,
        campaign_id,
        COUNT(DISTINCT statement_id) AS total_statements,
        COUNT(DISTINCT CASE WHEN is_collection_statement THEN statement_id END) AS collection_statements,
        SUM(balance_total) AS total_balance,
        SUM(CASE WHEN is_collection_statement THEN balance_total ELSE 0 END) AS collection_balance,
        SUM(payment_amount_7days) AS payment_amount_7days,
        SUM(payment_amount_14days) AS payment_amount_14days,
        SUM(payment_amount_30days) AS payment_amount_30days,
        COUNT(DISTINCT CASE WHEN resulted_in_payment THEN statement_id END) AS statements_with_payment,
        COUNT(DISTINCT CASE WHEN payment_result = 'full_payment' THEN statement_id END) AS statements_with_full_payment,
        COUNT(DISTINCT CASE WHEN payment_result = 'partial_payment' THEN statement_id END) AS statements_with_partial_payment
    FROM {{ ref('int_billing_statements') }}
    WHERE 
        date_sent >= CURRENT_DATE - INTERVAL '90 days'
        AND campaign_id IS NOT NULL
    GROUP BY campaign_id
),

-- Calculate derived metrics
DerivedMetrics AS (
    SELECT
        snapshot_date,
        metric_type,
        delivery_method,
        campaign_id,
        total_statements,
        collection_statements,
        total_balance,
        collection_balance,
        payment_amount_7days,
        payment_amount_14days,
        payment_amount_30days,
        statements_with_payment,
        statements_with_full_payment,
        statements_with_partial_payment,
        
        -- Calculated metrics
        CASE 
            WHEN total_statements > 0 THEN ROUND((collection_statements::numeric / total_statements::numeric), 4) 
            ELSE 0 
        END AS collection_statement_ratio,
        
        CASE 
            WHEN total_statements > 0 THEN ROUND((statements_with_payment::numeric / total_statements::numeric), 4) 
            ELSE 0 
        END AS statement_payment_rate,
        
        CASE 
            WHEN collection_statements > 0 THEN ROUND((statements_with_payment::numeric / collection_statements::numeric), 4) 
            ELSE 0 
        END AS collection_payment_rate,
        
        CASE 
            WHEN total_balance > 0 THEN ROUND((payment_amount_30days::numeric / total_balance::numeric), 4) 
            ELSE 0 
        END AS balance_collection_rate_30days,
        
        CASE 
            WHEN collection_balance > 0 THEN ROUND((payment_amount_30days::numeric / collection_balance::numeric), 4) 
            ELSE 0 
        END AS collection_balance_rate_30days,
        
        CASE 
            WHEN statements_with_payment > 0 
            THEN ROUND((statements_with_full_payment::numeric / statements_with_payment::numeric), 4)
            ELSE 0 
        END AS full_payment_rate,
        
        CASE 
            WHEN payment_amount_7days > 0 AND payment_amount_30days > 0
            THEN ROUND((payment_amount_7days::numeric / payment_amount_30days::numeric), 4)
            ELSE 0 
        END AS day7_response_ratio
    FROM StatementMetrics
)

-- Final selection
SELECT
    ROW_NUMBER() OVER (ORDER BY snapshot_date, metric_type, COALESCE(delivery_method, 'N/A'), COALESCE(campaign_id, 0)) AS statement_metric_id,
    snapshot_date,
    metric_type,
    delivery_method,
    campaign_id,
    total_statements,
    collection_statements,
    collection_statement_ratio,
    total_balance,
    collection_balance,
    payment_amount_7days,
    payment_amount_14days,
    payment_amount_30days,
    statements_with_payment,
    statements_with_full_payment,
    statements_with_partial_payment,
    statement_payment_rate,
    collection_payment_rate,
    balance_collection_rate_30days,
    collection_balance_rate_30days,
    full_payment_rate,
    day7_response_ratio,
    CURRENT_TIMESTAMP AS model_created_at,
    CURRENT_TIMESTAMP AS model_updated_at
FROM DerivedMetrics

{% if is_incremental() %}
    WHERE snapshot_date > (SELECT MAX(snapshot_date) FROM {{ this }})
{% endif %}