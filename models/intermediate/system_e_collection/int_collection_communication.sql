{{ config(
    materialized='incremental',
    schema='intermediate',
    unique_key='collection_communication_id'
) }}

/*
    Intermediate model for collection communications
    Part of System E: Collections
    
    This model:
    1. Tracks communications related to collection efforts
    2. Links communications to campaigns and tasks
    3. Captures interaction details and outcomes
    4. Enables follow-up tracking
*/

WITH CollectionCommunications AS (
    -- Get communications that are related to collections
    SELECT
        ROW_NUMBER() OVER (ORDER BY cl.commlog_id) AS collection_communication_id,
        cc.campaign_id,
        ct.collection_task_id,
        cl.commlog_id,
        cl.patient_id,
        cl.user_id,
        cl.communication_datetime::date AS communication_date,
        CASE 
            WHEN cl.communication_type = 1 THEN 'phone'
            WHEN cl.communication_type = 2 THEN 'email'
            WHEN cl.communication_type = 3 THEN 'text'
            WHEN cl.communication_type = 4 THEN 'letter'
            ELSE 'other'
        END AS communication_type,
        CASE 
            WHEN cl.is_sent = 1 THEN 'outbound'
            ELSE 'inbound'
        END AS direction,
        CASE
            WHEN cl.mode = 1 THEN 'sent'
            WHEN cl.mode = 2 THEN 'received'
            WHEN cl.mode = 3 THEN 'failed'
            ELSE 'unknown'
        END AS status,
        NULL AS template_id, -- Would be populated from actual template data
        'Collection Follow-up' AS subject, -- Default subject
        cl.note AS message_content,
        
        -- These fields would typically be populated from communication details or follow-up data
        -- For demonstration, setting default/derived values
        CASE
            WHEN cl.note LIKE '%will pay%' OR cl.note LIKE '%promise%' THEN 'Will make payment'
            WHEN cl.note LIKE '%cannot pay%' OR cl.note LIKE '%hardship%' THEN 'Financial hardship'
            WHEN cl.note LIKE '%dispute%' THEN 'Disputing charge'
            WHEN cl.note LIKE '%insurance%' THEN 'Insurance should cover'
            ELSE 'No response'
        END AS patient_response,
        
        CASE
            WHEN cl.note LIKE '%will pay%' OR cl.note LIKE '%promise%' THEN 'successful'
            WHEN cl.note LIKE '%partial%' THEN 'partial'
            ELSE 'pending'
        END AS outcome,
        
        -- Extract promised payment amount if present in note
        CASE
            WHEN cl.note LIKE '%$%' AND (cl.note LIKE '%will pay%' OR cl.note LIKE '%promise%')
            THEN REGEXP_REPLACE(SUBSTRING(cl.note FROM '\$[0-9,]+(\.[0-9]{2})?'), '[$,]', '', 'g')::numeric
            ELSE 0.00
        END AS promised_payment_amount,
        
        -- Extract promised date if present in note
        CASE
            WHEN cl.note LIKE '%will pay%' OR cl.note LIKE '%promise%'
            THEN CURRENT_DATE + INTERVAL '7 days' -- Default to 7 days if specific date not parsed
            ELSE NULL::date
        END AS promised_payment_date,
        
        0.00 AS actual_payment_amount, -- Would be updated with payment data
        NULL::date AS actual_payment_date, -- Would be updated with payment data
        
        CASE
            WHEN cl.note LIKE '%follow up%' OR cl.note LIKE '%callback%' THEN true
            ELSE false
        END AS follow_up_required,
        
        CASE
            WHEN cl.note LIKE '%follow up%' OR cl.note LIKE '%callback%'
            THEN CURRENT_DATE + INTERVAL '14 days' -- Default to 14 days for follow-up
            ELSE NULL::date
        END AS follow_up_date,
        
        NULL::integer AS follow_up_task_id, -- Would be linked to follow-up task
        
        cl.note AS notes,
        CURRENT_TIMESTAMP AS model_created_at,
        CURRENT_TIMESTAMP AS model_updated_at
    FROM {{ ref('stg_opendental__commlog') }} cl
    LEFT JOIN {{ ref('int_collection_tasks') }} ct
        ON cl.patient_id = ct.patient_id
        AND cl.communication_datetime::date BETWEEN ct.due_date AND COALESCE(ct.completion_date, CURRENT_DATE)
    LEFT JOIN {{ ref('int_collection_campaigns') }} cc
        ON ct.campaign_id = cc.campaign_id
    WHERE 
        -- Filter for collection-related communications
        (cl.note LIKE '%collection%'
        OR cl.note LIKE '%payment%'
        OR cl.note LIKE '%overdue%'
        OR cl.note LIKE '%balance%')
        AND cl.communication_datetime >= '2025-01-01' -- Match campaign start dates
        
    {% if is_incremental() %}
        -- If this is an incremental run, only process new communications
        AND cl.communication_datetime > (SELECT MAX(model_created_at) FROM {{ this }})
    {% endif %}
)

SELECT * FROM CollectionCommunications