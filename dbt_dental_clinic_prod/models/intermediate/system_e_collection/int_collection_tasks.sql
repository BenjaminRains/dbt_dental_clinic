{{ config(
    materialized='incremental',
    
    unique_key='collection_task_id'
) }}

/*
    ======================================================
    Note on key strategy:
    - Using a deterministic numeric ID based on the source task_id
    - This ensures stable IDs during incremental processing
    - Avoids problems that can occur with ROW_NUMBER() in incrementals
    - Maintains consistent surrogate keys for downstream models
    - Keeps the numeric type for compatibility with existing data
    ======================================================
*/

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
        -- Create a stable numeric ID based on the source task_id
        ABS(MOD(CAST(t.task_id AS BIGINT), 9223372036854775807)) AS collection_task_id,
        cc.campaign_id,
        t.task_id,
        t.description,
        COALESCE(t.key_id, ar.patient_id) AS patient_id,
        t.user_id AS assigned_user_id,
        
        -- Enhanced task type categorization
        CASE
            WHEN LOWER(t.description) LIKE '%call%' THEN 'call'
            WHEN LOWER(t.description) LIKE '%email%' THEN 'email'
            WHEN LOWER(t.description) LIKE '%text%' THEN 'text'
            WHEN LOWER(t.description) LIKE '%letter%' THEN 'letter'
            WHEN LOWER(t.description) LIKE '%mail%' THEN 'letter'
            ELSE 'other'
        END AS task_type,
        
        -- Enhanced collection type categorization
        CASE
            WHEN LOWER(t.description) LIKE '%collect%' 
                OR LOWER(t.description) LIKE '%payment%' 
                THEN 'direct_collection'
            WHEN LOWER(t.description) LIKE '%insurance%' 
                OR LOWER(t.description) LIKE '%claim%' 
                THEN 'insurance_follow_up'
            WHEN LOWER(t.description) LIKE '%write off%' 
                OR LOWER(t.description) LIKE '%write-off%' 
                THEN 'write_off'
            WHEN LOWER(t.description) LIKE '%balance%' 
                OR LOWER(t.description) LIKE '%owe%' 
                THEN 'balance_inquiry'
            ELSE 'other_collection'
        END AS collection_type,
        
        t.description AS task_description,
        t.task_date AS due_date,
        t.finished_timestamp::date AS completion_date,
        
        -- Enhanced status mapping
        CASE 
            WHEN t.task_status = 0 THEN 'pending'
            WHEN t.task_status = 1 THEN 'in_progress'
            WHEN t.task_status = 2 THEN 'completed'
            ELSE 'unknown'
        END AS task_status,
        
        -- Enhanced priority mapping
        CASE
            WHEN t.priority_def_id = 1 THEN 'high'
            WHEN t.priority_def_id = 2 THEN 'medium'
            ELSE 'low'
        END AS priority,
        
        -- Extract amount from description if present
        CASE
            WHEN t.description ~ '\$[0-9]+(\.[0-9]{2})?' 
            THEN CAST(
                REGEXP_REPLACE(
                    t.description, 
                    '.*\$([0-9]+(\.[0-9]{2})?).*', 
                    '\1', 
                    'g'
                ) AS DECIMAL(10,2)
            )
            ELSE NULL
        END AS collection_amount,
        
        ar.total_ar_balance AS ar_balance_at_creation,
        
        -- Enhanced aging bucket
        CASE
            WHEN ar.balance_over_90_days > 0 THEN '90+'
            WHEN ar.balance_61_90_days > 0 THEN '61-90'
            WHEN ar.balance_31_60_days > 0 THEN '31-60'
            WHEN ar.balance_0_30_days > 0 THEN '0-30'
            ELSE 'unknown'
        END AS aging_bucket_at_creation,
        
        -- Enhanced outcome tracking
        CASE
            WHEN LOWER(t.description) LIKE '%paid%' 
                OR LOWER(t.description) LIKE '%received%' 
            THEN 'payment_received'
            WHEN LOWER(t.description) LIKE '%promised%' 
                OR LOWER(t.description) LIKE '%will pay%' 
            THEN 'payment_promised'
            WHEN LOWER(t.description) LIKE '%write off%' 
                OR LOWER(t.description) LIKE '%write-off%' 
            THEN 'written_off'
            WHEN LOWER(t.description) LIKE '%denied%' 
                OR LOWER(t.description) LIKE '%rejected%' 
            THEN 'denied'
            ELSE 'pending'
        END AS outcome,
        
        -- Payment promises and actuals
        CASE
            WHEN LOWER(t.description) LIKE '%promised%' OR LOWER(t.description) LIKE '%will pay%' THEN
                CASE
                    WHEN t.description ~ '\$[0-9]+(\.[0-9]{2})?' THEN 
                        CAST(REGEXP_REPLACE(t.description, '.*\$([0-9]+(\.[0-9]{2})?).*', '\1', 'g') AS DECIMAL(10,2))
                    ELSE NULL
                END
            ELSE NULL
        END AS promised_payment_amount,
        
        CASE
            WHEN (LOWER(t.description) LIKE '%promised%' OR LOWER(t.description) LIKE '%will pay%') 
                 AND t.description ~ 'on [0-9]{1,2}/[0-9]{1,2}' THEN
                TO_DATE(REGEXP_REPLACE(t.description, '.*on ([0-9]{1,2}/[0-9]{1,2}).*', '\1', 'g'), 'MM/DD')
            ELSE NULL
        END AS promised_payment_date,
        
        CASE
            WHEN LOWER(t.description) LIKE '%paid%' OR LOWER(t.description) LIKE '%received%' THEN
                CASE
                    WHEN t.description ~ '\$[0-9]+(\.[0-9]{2})?' THEN 
                        CAST(REGEXP_REPLACE(t.description, '.*\$([0-9]+(\.[0-9]{2})?).*', '\1', 'g') AS DECIMAL(10,2))
                    ELSE NULL
                END
            ELSE NULL
        END AS actual_payment_amount,
        
        CASE
            WHEN LOWER(t.description) LIKE '%paid%' OR LOWER(t.description) LIKE '%received%' THEN
                t.finished_timestamp::date
            ELSE NULL
        END AS actual_payment_date,
        
        -- Enhanced follow-up tracking
        CASE
            WHEN LOWER(t.description) LIKE '%follow%' 
                OR LOWER(t.description) LIKE '%f/u%'
                OR LOWER(t.description) LIKE '%call back%'
                OR LOWER(t.description) LIKE '%check back%'
            THEN true
            ELSE false
        END AS follow_up_required,
        
        -- Extract follow-up date if present
        CASE
            WHEN t.description ~ '(f/u|follow up|follow-up)' THEN
                CASE
                    WHEN t.description ~ '(f/u|follow up|follow-up).*?([0-9]{1,2}/[0-9]{1,2})' 
                    THEN TO_DATE(
                        REGEXP_REPLACE(
                            t.description, 
                            '.*(f/u|follow up|follow-up).*?([0-9]{1,2}/[0-9]{1,2}).*', 
                            '\2', 
                            'g'
                        ), 
                        'MM/DD'
                    )
                    WHEN t.description ~ '(f/u|follow up|follow-up).*?([0-9]{1,2} [A-Za-z]{3})' 
                    THEN TO_DATE(
                        REGEXP_REPLACE(
                            t.description, 
                            '.*(f/u|follow up|follow-up).*?([0-9]{1,2} [A-Za-z]{3}).*', 
                            '\2', 
                            'g'
                        ), 
                        'DD Mon'
                    )
                    ELSE NULL
                END
            ELSE NULL
        END AS follow_up_date,
        
        -- Link to any follow-up task
        (SELECT MIN(related_task.task_id)
         FROM {{ ref('stg_opendental__task') }} related_task
         WHERE related_task.key_id = t.key_id
           AND related_task.task_id > t.task_id
           AND LOWER(related_task.description) LIKE '%follow%up%' 
         LIMIT 1) AS follow_up_task_id,
        
        -- Enhanced notes field
        CASE
            WHEN t.description ~ '\$[0-9]+(\.[0-9]{2})?' THEN
                REGEXP_REPLACE(t.description, '.*\$([0-9]+(\.[0-9]{2})?).*', 'Amount: $\1', 'g')
            ELSE t.description
        END AS notes,
        
        -- Data quality flags
        CASE WHEN t.description ~ '\$[0-9]+(\.[0-9]{2})?' THEN true ELSE false END AS has_amount,
        CASE WHEN t.description ~ '(f/u|follow up|follow-up)' THEN true ELSE false END AS has_follow_up,
        CASE WHEN t.description ~ 'paid|received|promised|will pay' THEN true ELSE false END AS has_outcome,
        
        -- Standardized metadata from primary source (stg_opendental__task)
        t._loaded_at,
        t._created_at,
        t._updated_at,
        NULL as _created_by,  -- Not available in source
        current_timestamp as _transformed_at
    FROM {{ ref('stg_opendental__task') }} t
    LEFT JOIN {{ ref('int_ar_analysis') }} ar
        ON t.key_id = ar.patient_id
    -- Join to campaigns based on patient AR criteria (after ar table is available)
    LEFT JOIN {{ ref('int_collection_campaigns') }} cc 
        ON (
            -- Direct campaign assignment based on patient AR criteria
            (ar.total_ar_balance >= CAST(cc.target_ar_balance_min AS NUMERIC) OR cc.target_ar_balance_min IS NULL)
            AND (cc.target_ar_balance_max IS NULL OR ar.total_ar_balance <= CAST(cc.target_ar_balance_max AS NUMERIC))
            AND (cc.target_aging_min IS NULL OR ar.balance_over_90_days >= cc.target_aging_min)
            AND (cc.target_aging_max IS NULL OR ar.balance_over_90_days <= CAST(cc.target_aging_max AS INTEGER))
            AND cc.campaign_status = 'active'
            AND (t.task_date IS NULL OR t.task_date BETWEEN cc.start_date AND cc.end_date)
        )
    WHERE 
        -- Enhanced collection task filtering with expanded keywords
        (
            -- Direct collection keywords (expanded to match actual task descriptions)
            LOWER(t.description) LIKE '%collect%' 
            OR LOWER(t.description) LIKE '%payment%'
            OR LOWER(t.description) LIKE '%balance%'
            OR LOWER(t.description) LIKE '%due%'
            OR LOWER(t.description) LIKE '%owe%'
            OR LOWER(t.description) LIKE '%pay%'
            OR LOWER(t.description) LIKE '%bill%'
            OR LOWER(t.description) LIKE '%account%'
            OR LOWER(t.description) LIKE '%write off%'
            OR LOWER(t.description) LIKE '%write-off%'
            OR LOWER(t.description) LIKE '%$%'  -- Match dollar amount mentions
            OR LOWER(t.description) LIKE '%insurance%'
            OR LOWER(t.description) LIKE '%claim%'
            
            -- Collection-related task lists
            OR t.task_list_id IN (
                -- Direct collection task lists
                42,  -- "call patient to collect remaining $16 balance after claims close"
                0,   -- "Call pt to collect full acct balance"
                29,  -- "add ins for Angelica Dix and submitt claim"
                30,  -- "Add metlife ins"
                44,  -- "please add her insurance, see appt notes"
                47,  -- "Chelsea email treatment plan of 29 and 30 implants to patient"
                50,  -- "Discuss Full Mouth Treatment Plan and f/u with pt after"
                8,   -- Treatment plan related
                36,  -- Insurance verification related
                51   -- Scheduling related to collections
            )
        )
        
    -- Incremental logic removed for testing campaign assignment
)

SELECT DISTINCT * FROM CollectionTasks
