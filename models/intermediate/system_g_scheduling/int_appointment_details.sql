{{ config(
    materialized='incremental',
    
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
            INTERVAL '1 minute' * (
                {{ calculate_pattern_length('apt.pattern') }}
            ) AS appointment_end_datetime,
        apt.appointment_type_id,
        apt.appointment_status,
        apt.confirmation_status as confirmed,
        apt.operatory_id as operatory,
        apt.pattern,
        {{ calculate_pattern_length('apt.pattern') }} as pattern_length,
        apt.note,
        apt.is_hygiene,
        apt.is_new_patient,
        apt.arrival_datetime as check_in_time,
        apt.dismissed_datetime as check_out_time,
        CASE
            WHEN apt.arrival_datetime IS NOT NULL 
            AND apt.dismissed_datetime IS NOT NULL
            AND apt.dismissed_datetime > apt.arrival_datetime
            AND apt.dismissed_datetime::time != '00:00:00'::time  -- Exclude placeholder times
            AND apt.arrival_datetime::time != '00:00:00'::time    -- Exclude placeholder times
            THEN (
                -- Count business hours between check-in and check-out
                WITH RECURSIVE dates AS (
                    SELECT 
                        apt.arrival_datetime as dt,
                        LEAST(
                            apt.dismissed_datetime,
                            DATE(apt.arrival_datetime) + INTERVAL '1 day' - INTERVAL '1 second'
                        ) as end_dt
                    UNION ALL
                    SELECT 
                        dt + INTERVAL '1 day',
                        LEAST(
                            apt.dismissed_datetime,
                            dt + INTERVAL '2 days' - INTERVAL '1 second'
                        )
                    FROM dates
                    WHERE dt + INTERVAL '1 day' < apt.dismissed_datetime
                )
                SELECT 
                    CASE 
                        WHEN SUM(
                            CASE 
                                -- Skip Sundays
                                WHEN EXTRACT(DOW FROM dt) = 0 THEN 0
                                -- Business hours only (9:00-17:00)
                                ELSE LEAST(
                                    EXTRACT(EPOCH FROM (
                                        LEAST(end_dt, dt + INTERVAL '17:00:00') - 
                                        GREATEST(dt, dt + INTERVAL '09:00:00')
                                    ))/60,
                                    480  -- 8 hours in minutes
                                )
                            END
                        ) > 0 THEN ROUND(SUM(
                            CASE 
                                -- Skip Sundays
                                WHEN EXTRACT(DOW FROM dt) = 0 THEN 0
                                -- Business hours only (9:00-17:00)
                                ELSE LEAST(
                                    EXTRACT(EPOCH FROM (
                                        LEAST(end_dt, dt + INTERVAL '17:00:00') - 
                                        GREATEST(dt, dt + INTERVAL '09:00:00')
                                    ))/60,
                                    480  -- 8 hours in minutes
                                )
                            END
                        ))::integer
                        ELSE NULL
                    END
                FROM dates
            )
            ELSE NULL
        END AS actual_length,
        apt.entry_datetime as created_at
    FROM {{ ref('stg_opendental__appointment') }} apt
    
    {% if is_incremental() %}
    WHERE apt.appointment_datetime > (SELECT MAX(appointment_datetime) FROM {{ this }})
    {% endif %}
),

AppointmentTypes AS (
    SELECT
        at.appointment_type_id,
        at.appointment_type_name,
        {{ calculate_pattern_length('at.pattern') }} as appointment_length,
        at.appointment_type_color as color
    FROM {{ ref('stg_opendental__appointmenttype') }} at
),

ProviderInfo AS (
    SELECT
        p.provider_id,
        p.provider_abbreviation as provider_abbr,
        p.provider_color,
        p.is_hidden,
        p.specialty_id as specialty
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
        history_action as action_type,
        CASE
            WHEN history_action = 1 THEN 'Rescheduled'
            WHEN history_action = 2 THEN 'Confirmed'
            WHEN history_action = 3 THEN 'Failed Appointment'
            WHEN history_action = 4 THEN 'Cancelled'
            WHEN history_action = 5 THEN 'ASAP'
            WHEN history_action = 6 THEN 'Complete'
            ELSE 'Unknown'
        END AS action_description,
        note as action_note,
        CASE
            WHEN history_action = 4 AND note IS NOT NULL
            THEN note
            ELSE NULL
        END AS cancellation_reason,
        CASE
            WHEN history_action = 1 AND note LIKE '%new appt:#%'
            THEN REGEXP_REPLACE(
                SUBSTRING(note FROM 'new appt:#([0-9]+)'),
                '[^0-9]', '', 'g'
            )::integer
            ELSE NULL
        END AS rescheduled_appointment_id,
        ROW_NUMBER() OVER (
            PARTITION BY appointment_id 
            ORDER BY history_timestamp DESC
        ) as history_rank
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
        WHEN ab.check_in_time IS NOT NULL 
        AND ab.appointment_datetime IS NOT NULL
        AND ab.check_in_time::time != '00:00:00'::time  -- Exclude placeholder times
        AND ab.check_in_time >= ab.appointment_datetime
        THEN ROUND(EXTRACT(EPOCH FROM (ab.check_in_time - ab.appointment_datetime))/60)::integer
        ELSE 0  -- Default to 0 for waiting time when no valid check-in time
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
    AND ha.history_rank = 1  -- Only get the latest history record
    AND ha.action_type IN (1, 4) -- Rescheduled or Cancelled
