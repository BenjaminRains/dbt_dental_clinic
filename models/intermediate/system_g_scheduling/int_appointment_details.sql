{{ config(
    materialized='incremental',
    schema='intermediate',
    unique_key='appointment_id'
) }}

/*
    Intermediate model for appointment details
    Part of System G: Scheduling
    
    This model:
    1. Joins appointment data with appointment types
    2. Enriches with provider information
    3. Calculates appointment metrics
    4. Standardizes status descriptions
*/

WITH AppointmentBase AS (
    SELECT
        apt.appointment_id,
        apt.patient_id,
        apt.provider_id,
        apt.appointment_datetime,
        apt.appointment_datetime + 
            INTERVAL '1 minute' * COALESCE(apt.pattern_length, 30) AS appointment_end_datetime,
        apt.appointment_type_id,
        apt.appointment_status,
        apt.confirmed,
        apt.op AS operatory,
        apt.pattern_length,
        apt.note,
        apt.is_hygiene,
        apt.is_new_patient,
        apt.check_in_time,
        apt.check_out_time,
        CASE
            WHEN apt.check_in_time IS NOT NULL AND apt.check_out_time IS NOT NULL
            THEN EXTRACT(EPOCH FROM (apt.check_out_time - apt.check_in_time))/60
            ELSE NULL
        END AS actual_length,
        apt.created_at
    FROM {{ ref('stg_opendental__appointment') }} apt
    
    {% if is_incremental() %}
    WHERE apt.appointment_datetime > (SELECT MAX(appointment_datetime) FROM {{ this }})
    {% endif %}
),

AppointmentTypes AS (
    SELECT
        at.appointment_type_id,
        at.appointment_type_name,
        at.appointment_length,
        at.color
    FROM {{ ref('stg_opendental__appointmenttype') }} at
),

ProviderInfo AS (
    SELECT
        p.provider_id,
        p.provider_abbr,
        p.provider_color,
        p.is_hidden,
        p.specialty
    FROM {{ ref('stg_opendental__provider') }} p
),

PatientInfo AS (
    SELECT
        pt.patient_id,
        pt.preferred_name,
        pt.patient_status,
        pt.first_visit_date
    FROM {{ ref('stg_opendental__patient') }} pt
),

HistoricalAppointments AS (
    SELECT
        patient_id,
        appointment_id,
        appointment_status,
        action_type,
        CASE
            WHEN action_type = 1 THEN 'Rescheduled'
            WHEN action_type = 2 THEN 'Confirmed'
            WHEN action_type = 3 THEN 'Failed Appointment'
            WHEN action_type = 4 THEN 'Cancelled'
            WHEN action_type = 5 THEN 'ASAP'
            WHEN action_type = 6 THEN 'Complete'
            ELSE 'Unknown'
        END AS action_description,
        action_note,
        CASE
            WHEN action_type = 4 AND action_note IS NOT NULL
            THEN action_note
            ELSE NULL
        END AS cancellation_reason,
        CASE
            WHEN action_type = 1 AND action_note LIKE '%new appt:#%'
            THEN REGEXP_REPLACE(
                SUBSTRING(action_note FROM 'new appt:#([0-9]+)'),
                '[^0-9]', '', 'g'
            )::integer
            ELSE NULL
        END AS rescheduled_appointment_id
    FROM {{ ref('stg_opendental__histappointment') }}
)

-- Final selection
SELECT
    ab.appointment_id,
    ab.patient_id,
    ab.provider_id,
    ab.appointment_datetime,
    ab.appointment_end_datetime,
    ab.appointment_type_id,
    at.appointment_type_name,
    at.appointment_length,
    ab.appointment_status,
    CASE
        WHEN ab.appointment_status = 1 THEN 'Scheduled'
        WHEN ab.appointment_status = 2 THEN 'Complete'
        WHEN ab.appointment_status = 3 THEN 'UnschedList'
        WHEN ab.appointment_status = 4 THEN 'ASAP'
        WHEN ab.appointment_status = 5 THEN 'Broken'
        WHEN ab.appointment_status = 6 THEN 'Planned'
        WHEN ab.appointment_status = 7 THEN 'PtNote'
        WHEN ab.appointment_status = 8 THEN 'PtNoteCompleted'
        ELSE 'Unknown'
    END AS appointment_status_desc,
    ab.confirmed AS is_confirmed,
    CASE WHEN ab.appointment_status = 2 THEN TRUE ELSE FALSE END AS is_complete,
    ab.is_hygiene,
    ab.is_new_patient,
    ab.note,
    ab.operatory,
    ab.check_in_time,
    ab.check_out_time,
    ab.actual_length,
    CASE
        WHEN ab.check_in_time IS NOT NULL AND ab.appointment_datetime IS NOT NULL
        THEN EXTRACT(EPOCH FROM (ab.check_in_time - ab.appointment_datetime))/60
        ELSE NULL
    END AS waiting_time,
    ha.cancellation_reason,
    ha.rescheduled_appointment_id,
    
    -- Provider information
    pi.provider_abbr AS provider_name,
    pi.specialty AS provider_specialty,
    pi.provider_color,
    
    -- Patient information
    pat.preferred_name AS patient_name,
    pat.patient_status,
    pat.first_visit_date,
    
    -- Metadata
    ab.created_at,
    CURRENT_TIMESTAMP AS updated_at,
    CURRENT_TIMESTAMP AS model_created_at,
    CURRENT_TIMESTAMP AS model_updated_at
FROM AppointmentBase ab
LEFT JOIN AppointmentTypes at
    ON ab.appointment_type_id = at.appointment_type_id
LEFT JOIN ProviderInfo pi
    ON ab.provider_id = pi.provider_id
LEFT JOIN PatientInfo pat
    ON ab.patient_id = pat.patient_id
LEFT JOIN HistoricalAppointments ha
    ON ab.appointment_id = ha.appointment_id
    AND ha.action_type IN (1, 4) -- Rescheduled or Cancelled