{{ config(
    materialized='incremental',
    
    unique_key='communication_id'
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
            WHEN cl.is_sent = 2 THEN 'outbound'
            WHEN cl.is_sent = 1 THEN 'inbound'
            WHEN cl.is_sent = 0 THEN 'system'
            ELSE 'unknown'
        END AS direction,
        NULL AS subject,
        cl.note AS content,
        -- Extract appointment ID if present in the note
        CASE
            WHEN cl.note ~* '(appt|appointment)\\s+#\\s*([0-9]{1,7})\\b'
                 AND NOT (SUBSTRING(cl.note FROM '(appt|appointment)\\s+#\\s*([0-9]{1,7})\\b') ~ 'Number')
            THEN REGEXP_REPLACE(SUBSTRING(cl.note FROM '(appt|appointment)\\s+#\\s*([0-9]{1,7})\\b'), '[^0-9]', '', 'g')::bigint
            ELSE NULL
        END AS linked_appointment_id,
        -- Extract claim ID if present
        CASE
            WHEN cl.note ~* '(claim|insurance)\\s+#\\s*([0-9]{1,7})\\b'
            THEN REGEXP_REPLACE(SUBSTRING(cl.note FROM '(claim|insurance)\\s+#\\s*([0-9]{1,7})\\b'), '[^0-9]', '', 'g')::bigint
            ELSE NULL
        END AS linked_claim_id,
        -- Extract procedure ID if present
        CASE
            WHEN cl.note ~* '(procedure|treatment)\\s+#\\s*([0-9]{1,7})\\b'
            THEN REGEXP_REPLACE(SUBSTRING(cl.note FROM '(procedure|treatment)\\s+#\\s*([0-9]{1,7})\\b'), '[^0-9]', '', 'g')::bigint
            ELSE NULL
        END AS linked_procedure_id,
        -- Extract phone number if present
        CASE
            WHEN cl.note ~* 'Number\\s+([0-9]{10})\\b'
            THEN REGEXP_REPLACE(SUBSTRING(cl.note FROM 'Number\\s+([0-9]{10})\\b'), '[^0-9]', '', 'g')
            ELSE NULL
        END AS contact_phone_number,
        -- Basic communication category based on content
        CASE
            WHEN cl.note ~* '(appt|appointment|schedule|reschedule)' THEN 'appointment'
            WHEN cl.note ~* '(bill|payment|balance|statement)' THEN 'billing'
            WHEN cl.note ~* '(treatment|procedure|exam|x-ray)' THEN 'clinical'
            WHEN cl.note ~* '(insurance|claim|coverage)' THEN 'insurance'
            WHEN cl.note ~* '(follow up|follow-up|callback)' THEN 'follow_up'
            ELSE 'general'
        END AS communication_category,
        -- Basic outcome based on content
        CASE
            WHEN cl.note ~* '(confirmed|agreed|yes)' THEN 'confirmed'
            WHEN cl.note ~* '(rescheduled|changed|moved)' THEN 'rescheduled'
            WHEN cl.note ~* '(cancelled|canceled|declined)' THEN 'cancelled'
            WHEN cl.note ~* '(no answer|voicemail|busy)' THEN 'no_answer'
            ELSE 'completed'
        END AS outcome,
        -- Follow-up tracking
        CASE
            WHEN cl.note ~* '(follow up|follow-up|call back|callback)'
            THEN TRUE
            ELSE FALSE
        END AS follow_up_required,
        -- Extract follow-up date if present
        CASE
            WHEN cl.note ~* '(follow up|follow-up)'
            THEN CURRENT_DATE + INTERVAL '7 days' -- Default to 7 days if specific date not parsed
            ELSE NULL::date
        END AS follow_up_date,
        NULL AS follow_up_task_id, -- Would be linked to a task record
        cl.program_id,
        cl.created_at,
        CURRENT_TIMESTAMP AS updated_at
    FROM {{ ref('stg_opendental__commlog') }} cl
    
    -- For incremental models, only process new records
    {% if is_incremental() %}
    WHERE cl.communication_datetime > (SELECT MAX(communication_datetime) FROM {{ this }})
    {% endif %}
),

-- Get information about last completed visit from appointment data
LastCompletedAppointment AS (
    SELECT
        patient_id,
        MAX(appointment_datetime) AS last_appointment_date
    FROM {{ ref('stg_opendental__appointment') }}
    WHERE appointment_status = 2 -- Complete status
    GROUP BY patient_id
),

-- Get information about completed visits from historical appointment data
LastHistoricalVisit AS (
    SELECT
        patient_id,
        MAX(history_timestamp) AS last_historical_date
    FROM {{ ref('stg_opendental__histappointment') }}
    WHERE history_action = 6 -- Complete action
    GROUP BY patient_id
),

-- Combine both sources to get the most accurate last visit date
LastCompletedVisit AS (
    SELECT
        COALESCE(lca.patient_id, lhv.patient_id) AS patient_id,
        GREATEST(
            COALESCE(lca.last_appointment_date, '1900-01-01'::timestamp),
            COALESCE(lhv.last_historical_date, '1900-01-01'::timestamp)
        ) AS last_visit_date
    FROM LastCompletedAppointment lca
    FULL OUTER JOIN LastHistoricalVisit lhv
        ON lca.patient_id = lhv.patient_id
    WHERE
        lca.last_appointment_date IS NOT NULL
        OR lhv.last_historical_date IS NOT NULL
),

PatientInfo AS (
    SELECT
        p.patient_id,
        p.preferred_name,
        p.patient_status,
        p.birth_date,
        p.first_visit_date,
        lcv.last_visit_date
    FROM {{ ref('int_patient_profile') }} p
    LEFT JOIN LastCompletedVisit lcv ON p.patient_id = lcv.patient_id
),

UserInfo AS (
    SELECT
        u.user_id,
        u.username AS user_name,
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
