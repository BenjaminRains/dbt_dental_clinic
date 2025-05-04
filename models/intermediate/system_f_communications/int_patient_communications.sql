{{ config(
    materialized='incremental',
    schema='intermediate',
    unique_key='communication_id'
) }}

/*
    Intermediate model for patient communications
    Part of System F: Communications
    
    This model:
    1. Consolidates communication logs with context
    2. Enhances communications with patient and user data
    3. Categorizes communications for reporting
    4. Tracks follow-up activities
*/

WITH CommunicationBase AS (
    SELECT
        cl.commlog_id AS communication_id,
        cl.patient_id,
        cl.user_id,
        cl.communication_datetime,
        cl.communication_end_datetime,
        cl.communication_type,
        cl.mode AS communication_mode,
        CASE 
            WHEN cl.is_sent = 1 THEN 'outbound'
            ELSE 'inbound'
        END AS direction,
        -- Fields to be populated with extracted data
        NULL AS subject,
        cl.note AS content,
        -- Extract appointment ID if present in the note (simplified approach)
        CASE
            WHEN cl.note LIKE '%appointment%' OR cl.note LIKE '%appt%'
            THEN REGEXP_REPLACE(SUBSTRING(cl.note FROM 'appt #([0-9]+)'), '[^0-9]', '', 'g')::integer
            ELSE NULL
        END AS linked_appointment_id,
        -- Extract claim ID if present in the note (simplified approach)
        CASE
            WHEN cl.note LIKE '%claim%'
            THEN REGEXP_REPLACE(SUBSTRING(cl.note FROM 'claim #([0-9]+)'), '[^0-9]', '', 'g')::integer
            ELSE NULL
        END AS linked_claim_id,
        -- Extract procedure ID if present in the note (simplified approach)
        CASE
            WHEN cl.note LIKE '%procedure%' OR cl.note LIKE '%proc%'
            THEN REGEXP_REPLACE(SUBSTRING(cl.note FROM 'proc #([0-9]+)'), '[^0-9]', '', 'g')::integer
            ELSE NULL
        END AS linked_procedure_id,
        -- Categorize communications
        CASE
            WHEN cl.note LIKE '%appointment%' OR cl.note LIKE '%appt%' THEN 'appointment'
            WHEN cl.note LIKE '%bill%' OR cl.note LIKE '%payment%' OR cl.note LIKE '%balance%' THEN 'billing'
            WHEN cl.note LIKE '%treatment%' OR cl.note LIKE '%procedure%' THEN 'clinical'
            WHEN cl.note LIKE '%insurance%' OR cl.note LIKE '%coverage%' THEN 'insurance'
            WHEN cl.note LIKE '%follow up%' OR cl.note LIKE '%followup%' THEN 'follow_up'
            ELSE 'general'
        END AS communication_category,
        
        -- Outcome would typically come from a structured field
        -- Creating a derived outcome based on content patterns
        CASE
            WHEN cl.note LIKE '%confirmed%' OR cl.note LIKE '%agreed%' THEN 'confirmed'
            WHEN cl.note LIKE '%rescheduled%' OR cl.note LIKE '%changed%' THEN 'rescheduled'
            WHEN cl.note LIKE '%cancelled%' OR cl.note LIKE '%declined%' THEN 'cancelled'
            WHEN cl.note LIKE '%no answer%' OR cl.note LIKE '%voicemail%' THEN 'no_answer'
            ELSE 'completed'
        END AS outcome,
        
        -- Follow-up fields
        CASE
            WHEN cl.note LIKE '%follow up%' OR cl.note LIKE '%followup%' OR 
                 cl.note LIKE '%call back%' OR cl.note LIKE '%callback%'
            THEN TRUE
            ELSE FALSE
        END AS follow_up_required,
        
        -- Extract follow-up date if present
        CASE
            WHEN cl.note LIKE '%follow up%' OR cl.note LIKE '%followup%'
            THEN CURRENT_DATE + INTERVAL '7 days' -- Default to 7 days if specific date not parsed
            ELSE NULL::date
        END AS follow_up_date,
        
        NULL AS follow_up_task_id, -- Would be linked to a task record
        
        cl.created_at,
        CURRENT_TIMESTAMP AS updated_at
    FROM {{ ref('stg_opendental__commlog') }} cl
    
    -- For incremental models, only process new records
    {% if is_incremental() %}
    WHERE cl.communication_datetime > (SELECT MAX(communication_datetime) FROM {{ this }})
    {% endif %}
),

PatientInfo AS (
    SELECT
        p.patient_id,
        p.preferred_name,
        p.patient_status,
        p.birth_date,
        p.first_visit_date,
        p.last_visit_date
    FROM {{ ref('int_patient_profile') }} p
),

UserInfo AS (
    SELECT
        u.user_id,
        u.user_name,
        u.is_hidden,
        u.provider_id
    FROM {{ ref('stg_opendental__userod') }} u
)

-- Final selection
SELECT
    cb.communication_id,
    cb.patient_id,
    cb.user_id,
    cb.communication_datetime,
    cb.communication_end_datetime,
    cb.communication_type,
    cb.communication_mode,
    cb.direction,
    cb.subject,
    cb.content,
    cb.linked_appointment_id,
    cb.linked_claim_id,
    cb.linked_procedure_id,
    cb.communication_category,
    cb.outcome,
    cb.follow_up_required,
    cb.follow_up_date,
    cb.follow_up_task_id,
    
    -- Additional context fields
    pi.preferred_name AS patient_name,
    pi.patient_status,
    pi.birth_date,
    ui.user_name,
    ui.provider_id,
    
    -- Metadata fields
    cb.created_at,
    cb.updated_at,
    CURRENT_TIMESTAMP AS model_created_at,
    CURRENT_TIMESTAMP AS model_updated_at
FROM CommunicationBase cb
LEFT JOIN PatientInfo pi
    ON cb.patient_id = pi.patient_id
LEFT JOIN UserInfo ui
    ON cb.user_id = ui.user_id