{{ config(
    materialized='incremental',
    schema='intermediate',
    unique_key='collection_task_id'
) }}

/*
    Intermediate model for collection tasks
    Part of System E: Collections
    
    This model:
    1. Tracks individual collection tasks from campaigns
    2. Ties tasks to specific patients and their AR balances
    3. Monitors task status and outcomes
    4. Enables follow-up tracking
*/

WITH CollectionTasks AS (
    -- Join tasks with campaign and patient information
    SELECT 
        ROW_NUMBER() OVER (ORDER BY t.task_id) AS collection_task_id,
        cc.campaign_id,
        t.task_id,
        t.description,
        COALESCE(t.key_id, ar.patient_id) AS patient_id, -- Use key_id if it's a patient-linked task
        t.user_id AS assigned_user_id,
        CASE
            WHEN t.description LIKE '%call%' OR t.description LIKE '%Call%' THEN 'call'
            WHEN t.description LIKE '%email%' OR t.description LIKE '%Email%' THEN 'email'
            WHEN t.description LIKE '%letter%' OR t.description LIKE '%Letter%' THEN 'letter'
            WHEN t.description LIKE '%text%' OR t.description LIKE '%Text%' THEN 'text'
            ELSE 'other'
        END AS task_type,
        t.description AS task_description,
        t.task_date AS due_date,
        t.finished_timestamp::date AS completion_date,
        CASE 
            WHEN t.task_status = 0 THEN 'pending'
            WHEN t.task_status = 1 THEN 'in_progress'
            WHEN t.task_status = 2 THEN 'completed'
            ELSE 'unknown'
        END AS task_status,
        CASE
            WHEN t.priority_def_id = 1 THEN 'high'
            WHEN t.priority_def_id = 2 THEN 'medium'
            ELSE 'low'
        END AS priority,
        ar.total_ar_balance AS ar_balance_at_creation,
        CASE
            WHEN ar.balance_over_90_days > 0 THEN '90+'
            WHEN ar.balance_61_90_days > 0 THEN '61-90'
            WHEN ar.balance_31_60_days > 0 THEN '31-60'
            WHEN ar.balance_0_30_days > 0 THEN '0-30'
            ELSE 'unknown'
        END AS aging_bucket_at_creation,
        
        -- These fields would typically be populated from task notes or custom fields
        -- For demonstration, setting default values
        'pending' AS outcome,
        0.00 AS promised_payment_amount,
        NULL::date AS promised_payment_date,
        0.00 AS actual_payment_amount,
        NULL::date AS actual_payment_date,
        false AS follow_up_required,
        NULL::date AS follow_up_date,
        NULL::integer AS follow_up_task_id,
        t.description AS notes,
        
        -- Metadata
        CURRENT_TIMESTAMP AS model_created_at,
        CURRENT_TIMESTAMP AS model_updated_at
    FROM {{ ref('stg_opendental__task') }} t
    INNER JOIN {{ ref('int_collection_campaigns') }} cc
        ON t.description LIKE '%' || cc.campaign_name || '%'
        OR t.description LIKE '%collection%'
    LEFT JOIN {{ ref('int_ar_analysis') }} ar
        ON t.key_id = ar.patient_id
    WHERE 
        -- Filter for tasks that match our criteria
        (t.description LIKE '%collection%' 
        OR t.description LIKE '%overdue%'
        OR t.description LIKE '%payment%')
        AND t.task_date >= '2025-01-01' -- Match campaign start dates
        
    {% if is_incremental() %}
        -- If this is an incremental run, only process new or changed tasks
        AND (t.entry_timestamp > (SELECT MAX(model_created_at) FROM {{ this }})
            OR t.finished_timestamp > (SELECT MAX(model_updated_at) FROM {{ this }}))
    {% endif %}
)

SELECT * FROM CollectionTasks