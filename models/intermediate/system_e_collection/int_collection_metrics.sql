{{ config(
    materialized='incremental',
    unique_key=['snapshot_date', 'campaign_id', 'user_id', 'metric_level'],
    schema='intermediate'
) }}

/*
    ======================================================
    Note on grain and key strategy:

    GRAIN:
    - The grain of this model is (snapshot_date, metric_level, campaign_id, user_id)
    - Each combination of these fields represents a unique metrics record
    - campaign_id is relevant for 'campaign' metric_level
    - user_id is relevant for 'user' metric_level
    - Both are NULL for 'overall' metric_level
    - All fields are included in the unique_key for proper incremental processing

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
    Intermediate model for collection metrics
    Part of System E: Collections

    This model:
    1. Aggregates metrics across collection campaigns
    2. Provides performance indicators at campaign and user levels
    3. Tracks changes in collection performance over time
    4. Enables data-driven optimization of collection strategies
*/

WITH MetricsBase AS (
    -- Base metrics calculations at different levels
    
    -- Campaign-level metrics
    SELECT
        CURRENT_DATE AS snapshot_date,
        cc.campaign_id,
        NULL::integer AS user_id,
        'campaign' AS metric_level,
        COUNT(DISTINCT ct.patient_id) AS total_accounts,
        SUM(ar.total_ar_balance) AS total_ar_amount,
        SUM(CASE 
            WHEN clc.outcome = 'successful' THEN COALESCE(clc.actual_payment_amount, 0)
            ELSE 0
        END) AS collected_amount,
        COUNT(DISTINCT clc.patient_id) AS accounts_contacted,
        SUM(CASE 
            WHEN clc.promised_payment_amount > 0 THEN clc.promised_payment_amount
            ELSE 0
        END) AS promised_payment_amount,
        COUNT(DISTINCT CASE 
            WHEN clc.promised_payment_amount > 0 THEN clc.collection_communication_id
            ELSE NULL
        END) AS promised_payment_count,
        COUNT(DISTINCT CASE 
            WHEN clc.actual_payment_amount > 0 THEN clc.collection_communication_id
            ELSE NULL
        END) AS fulfilled_payment_count,
        COUNT(DISTINCT CASE 
            WHEN clc.actual_payment_amount > 0 
                AND clc.actual_payment_date <= clc.promised_payment_date 
            THEN clc.collection_communication_id
            ELSE NULL
        END) AS on_time_payment_count,
        AVG(CASE 
            WHEN clc.actual_payment_date IS NOT NULL AND clc.promised_payment_date IS NOT NULL
            THEN (clc.actual_payment_date - clc.promised_payment_date)
            ELSE NULL
        END) AS avg_days_to_payment,
        COUNT(DISTINCT ct.collection_task_id) AS tasks_total,
        COUNT(DISTINCT CASE 
            WHEN ct.task_status = 'completed' THEN ct.collection_task_id
            ELSE NULL
        END) AS tasks_completed,
        COUNT(DISTINCT CASE 
            WHEN ct.task_status IN ('pending', 'in_progress') THEN ct.collection_task_id
            ELSE NULL
        END) AS tasks_pending,
        COUNT(DISTINCT CASE 
            WHEN clc.direction = 'outbound' THEN clc.collection_communication_id
            ELSE NULL
        END) AS communications_sent,
        COUNT(DISTINCT CASE 
            WHEN clc.direction = 'inbound' THEN clc.collection_communication_id
            ELSE NULL
        END) AS communications_received
    FROM {{ ref('int_collection_campaigns') }} cc
    LEFT JOIN {{ ref('int_collection_tasks') }} ct
        ON cc.campaign_id = ct.campaign_id
    LEFT JOIN {{ ref('int_collection_communication') }} clc
        ON ct.collection_task_id = clc.collection_task_id
    LEFT JOIN {{ ref('int_ar_analysis') }} ar
        ON ct.patient_id = ar.patient_id
    GROUP BY 1, 2, 3, 4
    
    UNION ALL
    
    -- User-level metrics
    SELECT
        CURRENT_DATE AS snapshot_date,
        NULL::integer AS campaign_id,
        ct.assigned_user_id AS user_id,
        'user' AS metric_level,
        COUNT(DISTINCT ct.patient_id) AS total_accounts,
        SUM(ar.total_ar_balance) AS total_ar_amount,
        SUM(CASE 
            WHEN clc.outcome = 'successful' THEN COALESCE(clc.actual_payment_amount, 0)
            ELSE 0
        END) AS collected_amount,
        COUNT(DISTINCT clc.patient_id) AS accounts_contacted,
        SUM(CASE 
            WHEN clc.promised_payment_amount > 0 THEN clc.promised_payment_amount
            ELSE 0
        END) AS promised_payment_amount,
        COUNT(DISTINCT CASE 
            WHEN clc.promised_payment_amount > 0 THEN clc.collection_communication_id
            ELSE NULL
        END) AS promised_payment_count,
        COUNT(DISTINCT CASE 
            WHEN clc.actual_payment_amount > 0 THEN clc.collection_communication_id
            ELSE NULL
        END) AS fulfilled_payment_count,
        COUNT(DISTINCT CASE 
            WHEN clc.actual_payment_amount > 0 
                AND clc.actual_payment_date <= clc.promised_payment_date 
            THEN clc.collection_communication_id
            ELSE NULL
        END) AS on_time_payment_count,
        AVG(CASE 
            WHEN clc.actual_payment_date IS NOT NULL AND clc.promised_payment_date IS NOT NULL
            THEN (clc.actual_payment_date - clc.promised_payment_date)
            ELSE NULL
        END) AS avg_days_to_payment,
        COUNT(DISTINCT ct.collection_task_id) AS tasks_total,
        COUNT(DISTINCT CASE 
            WHEN ct.task_status = 'completed' THEN ct.collection_task_id
            ELSE NULL
        END) AS tasks_completed,
        COUNT(DISTINCT CASE 
            WHEN ct.task_status IN ('pending', 'in_progress') THEN ct.collection_task_id
            ELSE NULL
        END) AS tasks_pending,
        COUNT(DISTINCT CASE 
            WHEN clc.direction = 'outbound' THEN clc.collection_communication_id
            ELSE NULL
        END) AS communications_sent,
        COUNT(DISTINCT CASE 
            WHEN clc.direction = 'inbound' THEN clc.collection_communication_id
            ELSE NULL
        END) AS communications_received
    FROM {{ ref('int_collection_tasks') }} ct
    LEFT JOIN {{ ref('int_collection_communication') }} clc
        ON ct.collection_task_id = clc.collection_task_id
    LEFT JOIN {{ ref('int_ar_analysis') }} ar
        ON ct.patient_id = ar.patient_id
    WHERE ct.assigned_user_id IS NOT NULL
    GROUP BY 1, 2, 3, 4
    
    UNION ALL
    
    -- Overall metrics
    SELECT
        CURRENT_DATE AS snapshot_date,
        NULL::integer AS campaign_id,
        NULL::integer AS user_id,
        'overall' AS metric_level,
        COUNT(DISTINCT ct.patient_id) AS total_accounts,
        SUM(ar.total_ar_balance) AS total_ar_amount,
        SUM(CASE 
            WHEN clc.outcome = 'successful' THEN COALESCE(clc.actual_payment_amount, 0)
            ELSE 0
        END) AS collected_amount,
        COUNT(DISTINCT clc.patient_id) AS accounts_contacted,
        SUM(CASE 
            WHEN clc.promised_payment_amount > 0 THEN clc.promised_payment_amount
            ELSE 0
        END) AS promised_payment_amount,
        COUNT(DISTINCT CASE 
            WHEN clc.promised_payment_amount > 0 THEN clc.collection_communication_id
            ELSE NULL
        END) AS promised_payment_count,
        COUNT(DISTINCT CASE 
            WHEN clc.actual_payment_amount > 0 THEN clc.collection_communication_id
            ELSE NULL
        END) AS fulfilled_payment_count,
        COUNT(DISTINCT CASE 
            WHEN clc.actual_payment_amount > 0 
                AND clc.actual_payment_date <= clc.promised_payment_date 
            THEN clc.collection_communication_id
            ELSE NULL
        END) AS on_time_payment_count,
        AVG(CASE 
            WHEN clc.actual_payment_date IS NOT NULL AND clc.promised_payment_date IS NOT NULL
            THEN (clc.actual_payment_date - clc.promised_payment_date)
            ELSE NULL
        END) AS avg_days_to_payment,
        COUNT(DISTINCT ct.collection_task_id) AS tasks_total,
        COUNT(DISTINCT CASE 
            WHEN ct.task_status = 'completed' THEN ct.collection_task_id
            ELSE NULL
        END) AS tasks_completed,
        COUNT(DISTINCT CASE 
            WHEN ct.task_status IN ('pending', 'in_progress') THEN ct.collection_task_id
            ELSE NULL
        END) AS tasks_pending,
        COUNT(DISTINCT CASE 
            WHEN clc.direction = 'outbound' THEN clc.collection_communication_id
            ELSE NULL
        END) AS communications_sent,
        COUNT(DISTINCT CASE 
            WHEN clc.direction = 'inbound' THEN clc.collection_communication_id
            ELSE NULL
        END) AS communications_received
    FROM {{ ref('int_collection_tasks') }} ct
    LEFT JOIN {{ ref('int_collection_communication') }} clc
        ON ct.collection_task_id = clc.collection_task_id
    LEFT JOIN {{ ref('int_ar_analysis') }} ar
        ON ct.patient_id = ar.patient_id
    GROUP BY 1, 2, 3, 4
),

FinalMetrics AS (
    -- Calculate derived metrics
    SELECT
        mb.snapshot_date,
        mb.campaign_id,
        mb.user_id,
        mb.metric_level,
        mb.total_accounts,
        mb.total_ar_amount,
        mb.collected_amount,
        
        -- Collection rate
        CASE 
            WHEN mb.total_ar_amount > 0 
            THEN mb.collected_amount / mb.total_ar_amount 
            ELSE 0 
        END AS collection_rate,
        
        mb.accounts_contacted,
        
        -- Contact rate
        CASE 
            WHEN mb.total_accounts > 0 
            THEN mb.accounts_contacted::float / mb.total_accounts 
            ELSE 0 
        END AS contact_rate,
        
        mb.promised_payment_amount,
        mb.promised_payment_count,
        
        -- Payment fulfillment rate
        CASE 
            WHEN mb.promised_payment_count > 0 
            THEN mb.fulfilled_payment_count::float / mb.promised_payment_count 
            ELSE 0 
        END AS payment_fulfillment_rate,
        
        -- Payment punctuality rate
        CASE 
            WHEN mb.fulfilled_payment_count > 0 
            THEN mb.on_time_payment_count::float / mb.fulfilled_payment_count 
            ELSE 0 
        END AS payment_punctuality_rate,
        
        mb.avg_days_to_payment,
        
        -- Average contacts per payment
        CASE 
            WHEN mb.fulfilled_payment_count > 0 
            THEN mb.communications_sent::float / mb.fulfilled_payment_count 
            ELSE 0 
        END AS avg_contacts_per_payment,
        
        mb.tasks_completed,
        mb.tasks_pending,
        mb.communications_sent,
        mb.communications_received,
        CURRENT_TIMESTAMP AS model_created_at,
        CURRENT_TIMESTAMP AS model_updated_at
    FROM MetricsBase mb
)

SELECT
    -- Create a stable numeric ID based on the combined grain fields
    ABS(MOD(
        ('x' || SUBSTR(MD5(
            CAST(snapshot_date AS VARCHAR) || '|' ||
            CAST(metric_level AS VARCHAR) || '|' ||
            COALESCE(CAST(campaign_id AS VARCHAR), 'NULL') || '|' ||
            COALESCE(CAST(user_id AS VARCHAR), 'NULL')
        ), 1, 16))::bit(64)::bigint,
        9223372036854775807  -- Max bigint value to avoid overflow
    )) AS metric_id,
    snapshot_date,
    campaign_id,
    user_id,
    metric_level,
    total_accounts,
    total_ar_amount,
    collected_amount,
    collection_rate,
    accounts_contacted,
    contact_rate,
    promised_payment_amount,
    promised_payment_count,
    payment_fulfillment_rate,
    payment_punctuality_rate,
    avg_days_to_payment,
    avg_contacts_per_payment,
    tasks_completed,
    tasks_pending,
    communications_sent,
    communications_received,
    model_created_at,
    model_updated_at
FROM FinalMetrics

{% if is_incremental() %}
    WHERE snapshot_date > (SELECT MAX(snapshot_date) FROM {{ this }})
{% endif %}