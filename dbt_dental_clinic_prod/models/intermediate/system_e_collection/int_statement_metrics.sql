{{ config(
    materialized='incremental',
    
    unique_key=['snapshot_date', 'metric_type', 'delivery_method', 'campaign_id']
) }}

/*
    ======================================================
    Note on grain and key strategy:

    GRAIN:
    - The grain of this model is (snapshot_date, metric_type, delivery_method, campaign_id)
    - Each combination of these 4 fields represents a unique metrics record
    - delivery_method is relevant for 'by_delivery_method' metric_type
    - campaign_id is relevant for 'by_campaign' metric_type
    - Both delivery_method and campaign_id are NULL for 'overall' metric_type
    - All 4 fields must be included in the unique_key to prevent incorrect merges

    KEY STRATEGY:
    - Using a deterministic numeric ID based on hashing all grain fields
    - The hash is converted to a stable numeric value using ABS(MOD())
    - This ensures consistent IDs during incremental processing
    - Maintains numeric type for compatibility with existing data
    - Creates reliable surrogate keys for downstream models
    - Avoids problems that can occur with ROW_NUMBER() in incremental models
    ======================================================
*/

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
        COUNT(DISTINCT CASE WHEN payment_result = 'partial_payment' THEN statement_id END) AS statements_with_partial_payment,
        -- Preserve primary source metadata for aggregation
        MIN(_loaded_at) AS earliest_loaded_at,
        MAX(_transformed_at) AS latest_transformed_at,
        MIN(_created_at) AS earliest_created_at,
        MAX(_updated_at) AS latest_updated_at
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
        COUNT(DISTINCT CASE WHEN payment_result = 'partial_payment' THEN statement_id END) AS statements_with_partial_payment,
        -- Preserve primary source metadata for aggregation
        MIN(_loaded_at) AS earliest_loaded_at,
        MAX(_transformed_at) AS latest_transformed_at,
        MIN(_created_at) AS earliest_created_at,
        MAX(_updated_at) AS latest_updated_at
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
        COUNT(DISTINCT CASE WHEN payment_result = 'partial_payment' THEN statement_id END) AS statements_with_partial_payment,
        -- Preserve primary source metadata for aggregation
        MIN(_loaded_at) AS earliest_loaded_at,
        MAX(_transformed_at) AS latest_transformed_at,
        MIN(_created_at) AS earliest_created_at,
        MAX(_updated_at) AS latest_updated_at
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
        earliest_loaded_at,
        latest_transformed_at,
        earliest_created_at,
        latest_updated_at,
        
        -- Calculated metrics
        -- Note: Using LEAST() to cap rates at 1.0 to handle edge cases where:
        -- - Payment amounts exceed statement balances (due to payment timing or multiple payments)
        -- - Statement counts exceed expected ratios (due to payment tracking across statements)
        -- - Early payment ratios exceed 100% (due to payment timing windows)
        CASE 
            WHEN total_statements > 0 THEN ROUND((collection_statements::numeric / total_statements::numeric), 4) 
            ELSE 0 
        END AS collection_statement_ratio,
        
        CASE 
            WHEN total_statements > 0 THEN ROUND((statements_with_payment::numeric / total_statements::numeric), 4) 
            ELSE 0 
        END AS statement_payment_rate,
        
        CASE 
            WHEN collection_statements > 0 THEN 
                ROUND(LEAST((statements_with_payment::numeric / collection_statements::numeric), 1.0), 4) 
            ELSE 0 
        END AS collection_payment_rate,
        
        CASE 
            WHEN total_balance > 0 THEN 
                ROUND(LEAST((payment_amount_30days::numeric / total_balance::numeric), 1.0), 4)
            ELSE 0 
        END AS balance_collection_rate_30days,
        
        CASE 
            WHEN collection_balance > 0 THEN 
                ROUND(LEAST((payment_amount_30days::numeric / collection_balance::numeric), 1.0), 4)
            ELSE 0 
        END AS collection_balance_rate_30days,
        
        CASE 
            WHEN statements_with_payment > 0 
            THEN ROUND((statements_with_full_payment::numeric / statements_with_payment::numeric), 4)
            ELSE 0 
        END AS full_payment_rate,
        
        CASE 
            WHEN payment_amount_7days > 0 AND payment_amount_30days > 0
            THEN ROUND(LEAST((payment_amount_7days::numeric / payment_amount_30days::numeric), 1.0), 4)
            ELSE 0 
        END AS day7_response_ratio
    FROM StatementMetrics
)

-- Final selection
SELECT
    -- Create a stable numeric ID based on the grain fields
    -- Hash the combined fields then use ABS(MOD()) to get a stable numeric ID
    ABS(MOD(
        ('x' || SUBSTR(MD5(
            CAST(snapshot_date AS VARCHAR) || '|' ||
            CAST(metric_type AS VARCHAR) || '|' ||
            COALESCE(CAST(delivery_method AS VARCHAR), 'NULL') || '|' ||
            COALESCE(CAST(campaign_id AS VARCHAR), 'NULL')
        ), 1, 16))::bit(64)::bigint,
        9223372036854775807  -- Max bigint value to avoid overflow
    )) AS statement_metric_id,
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
    
    -- Standardized metadata using the macro
    -- Note: Since this is an aggregation model, we use the earliest/latest metadata from source
    earliest_loaded_at as _loaded_at,
    earliest_created_at as _created_at,
    latest_updated_at as _updated_at,
    current_timestamp as _transformed_at
FROM DerivedMetrics

{% if is_incremental() %}
    WHERE snapshot_date > (SELECT MAX(snapshot_date) FROM {{ this }})
{% endif %}
