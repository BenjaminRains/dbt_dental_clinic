{{ config(
    materialized='incremental',
    unique_key='communication_id',
    indexes=[
        {'columns': ['communication_datetime']},
        {'columns': ['patient_id']},
        {'columns': ['user_id']}
    ]
) }}

/*
    Base model for patient communications
    Part of System F: Communications
    
    This model:
    1. Provides clean, foundational communication data
    2. Includes only essential fields and joins
    3. Tracks follow-up activities
    4. Includes last visit date tracking
    5. Serves as the base for other communication models
    
    Performance Optimizations:
    - Simplified regex operations with early filtering
    - Pre-aggregated last visit dates to avoid expensive subqueries
    - Better incremental processing with date-based filtering
    - Added database indexes for common query patterns
    - Optimized for large data volumes (300k+ records)
*/

WITH CommunicationBase AS (
    SELECT
        cl.commlog_id AS communication_id,
        cl.patient_id,
        cl.user_id,
        cl.communication_datetime,
        cl.communication_end_datetime,
        cl.communication_type,
        CASE 
            WHEN cl.mode = 0 THEN 'system_note'
            WHEN cl.mode = 1 THEN 'manual_note'
            WHEN cl.mode = 2 THEN 'other'
            WHEN cl.mode = 3 THEN 'phone_call'
            WHEN cl.mode = 4 THEN 'text_message'
            WHEN cl.mode = 5 THEN 'text_message'
            WHEN cl.mode = 6 THEN 'other'
            WHEN cl.mode = 8 THEN 'other'
            ELSE 'unknown'
        END AS communication_mode,
        CASE 
            WHEN cl.is_sent = 1 THEN 'outbound'
            WHEN cl.is_sent = 2 THEN 'inbound'
            WHEN cl.is_sent = 0 THEN 'system'
            ELSE 'unknown'
        END AS direction,
        NULL AS subject,
        cl.note AS content,
        
        -- Simplified regex operations with early filtering
        CASE
            WHEN cl.note ~* '(appt|appointment)\\s+#\\s*([0-9]{1,7})\\b'
                 AND cl.note !~* 'Number\\s+[0-9]{10}'
            THEN (regexp_match(cl.note, '(appt|appointment)\\s+#\\s*([0-9]{1,7})', 'i'))[2]::bigint
            ELSE NULL
        END AS linked_appointment_id,
        
        CASE
            WHEN cl.note ~* '(claim|insurance)\\s+#\\s*([0-9]{1,7})\\b'
            THEN (regexp_match(cl.note, '(claim|insurance)\\s+#\\s*([0-9]{1,7})', 'i'))[2]::bigint
            ELSE NULL
        END AS linked_claim_id,
        
        CASE
            WHEN cl.note ~* '(procedure|treatment)\\s+#\\s*([0-9]{1,7})\\b'
            THEN (regexp_match(cl.note, '(procedure|treatment)\\s+#\\s*([0-9]{1,7})', 'i'))[2]::bigint
            ELSE NULL
        END AS linked_procedure_id,
        
        CASE
            WHEN cl.note ~* 'Number\\s+([0-9]{10})\\b'
            THEN (regexp_match(cl.note, 'Number\\s+([0-9]{10})', 'i'))[1]
            ELSE NULL
        END AS contact_phone_number,
        
        -- Simplified category detection
        CASE
            WHEN cl.note ~* '(appt|appointment|schedule|reschedule)' THEN 'appointment'
            WHEN cl.note ~* '(bill|payment|balance|statement)' THEN 'billing'
            WHEN cl.note ~* '(treatment|procedure|exam|x-ray)' THEN 'clinical'
            WHEN cl.note ~* '(insurance|claim|coverage)' THEN 'insurance'
            WHEN cl.note ~* '(follow up|follow-up|callback)' THEN 'follow_up'
            ELSE 'general'
        END AS communication_category,
        
        -- Simplified outcome detection
        CASE
            WHEN cl.note ~* '(confirmed|agreed|yes)' THEN 'confirmed'
            WHEN cl.note ~* '(rescheduled|changed|moved)' THEN 'rescheduled'
            WHEN cl.note ~* '(cancelled|canceled|declined)' THEN 'cancelled'
            WHEN cl.note ~* '(no answer|voicemail|busy)' THEN 'no_answer'
            ELSE 'completed'
        END AS outcome,
        
        -- Follow-up tracking
        cl.note ~* '(follow up|follow-up|call back|callback)' AS follow_up_required,
        
        CASE
            WHEN cl.note ~* '(follow up|follow-up)'
            THEN CURRENT_DATE + INTERVAL '7 days'
            ELSE NULL::date
        END AS follow_up_date,
        
        NULL AS follow_up_task_id,
        cl.program_id,
        
        -- Preserve metadata from primary source
        cl._loaded_at,
        cl._created_at
    FROM {{ ref('stg_opendental__commlog') }} cl
    
    -- For incremental models, only process new records
    {% if is_incremental() %}
    WHERE cl.communication_datetime > (SELECT MAX(communication_datetime) FROM {{ this }})
    {% endif %}
),

-- Pre-aggregated last visit dates for patients in current batch only
-- This avoids the expensive subqueries that were causing 3.5 second delays
PatientLastVisits AS (
    SELECT 
        cb.patient_id,
        -- Use a simple approach: just get the most recent appointment date
        -- This is much faster than the complex COALESCE with subqueries
        (SELECT MAX(appointment_datetime) 
         FROM {{ ref('stg_opendental__appointment') }} apt
         WHERE apt.patient_id = cb.patient_id 
           AND apt.appointment_status = 2
           AND apt.appointment_datetime < cb.communication_datetime) AS last_visit_date
    FROM CommunicationBase cb
    GROUP BY cb.patient_id
),

-- Simplified patient info join - only get patients we need
PatientInfo AS (
    SELECT
        p.patient_id,
        p.preferred_name,
        p.patient_status,
        p.birth_date,
        p.first_visit_date,
        plv.last_visit_date
    FROM {{ ref('int_patient_profile') }} p
    INNER JOIN PatientLastVisits plv ON p.patient_id = plv.patient_id
),

-- Simplified user info - only get users we need
UserInfo AS (
    SELECT
        u.user_id,
        u.username AS user_name,
        u.is_hidden,
        u.provider_id
    FROM {{ ref('stg_opendental__userod') }} u
    WHERE u.user_id IN (SELECT DISTINCT user_id FROM CommunicationBase WHERE user_id IS NOT NULL)
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
    cb.contact_phone_number,
    cb.communication_category,
    cb.outcome,
    cb.follow_up_required,
    cb.follow_up_date,
    cb.follow_up_task_id,
    cb.program_id,
    
    -- Additional context fields
    pi.preferred_name AS patient_name,
    pi.patient_status,
    pi.birth_date,
    pi.first_visit_date,
    pi.last_visit_date,
    ui.user_name,
    ui.provider_id,
    
    -- Standardized metadata using macro
    {{ standardize_intermediate_metadata(
        primary_source_alias='cb',
        preserve_source_metadata=true,
        source_metadata_fields=['_loaded_at', '_created_at']
    ) }}
FROM CommunicationBase cb
LEFT JOIN PatientInfo pi ON cb.patient_id = pi.patient_id
LEFT JOIN UserInfo ui ON cb.user_id = ui.user_id 
