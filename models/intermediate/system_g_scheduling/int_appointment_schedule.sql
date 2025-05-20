{{ config(
    materialized='incremental',
    schema='intermediate',
    unique_key='schedule_id'
) }}

/*
    Intermediate model for appointment scheduling.
    Aggregates appointment data by provider and day to show schedule utilization
    and appointment metrics.
    
    This model:
    1. Joins appointment data with provider information
    2. Calculates daily schedule metrics
    3. Determines provider availability
    4. Computes utilization rates
*/

WITH ProviderSchedule AS (
    SELECT
        p.provider_id,
        p.provider_abbreviation as provider_name,
        p.is_hidden,
        p.specialty_id as specialty
    FROM {{ ref('stg_opendental__provider') }} p
    WHERE p.is_hidden = 0  -- 0 = not hidden, 1 = hidden
),

AppointmentMetrics AS (
    SELECT
        DATE(apt.appointment_datetime) as schedule_date,
        apt.provider_id,
        COUNT(*) as total_appointments,
        COUNT(CASE WHEN apt.appointment_status = 2 THEN 1 END) as completed_appointments,
        COUNT(CASE WHEN apt.appointment_status = 5 THEN 1 END) as cancelled_appointments,
        COUNT(CASE WHEN apt.appointment_status = 3 THEN 1 END) as no_show_appointments,
        COUNT(CASE WHEN apt.confirmation_status = 0 THEN 1 END) as unconfirmed_appointments,
        SUM(
            CASE 
                WHEN apt.pattern IS NULL THEN 30
                WHEN apt.pattern ~ '^[0-9]+$' THEN apt.pattern::integer
                ELSE LENGTH(apt.pattern) * 10
            END
        ) as total_appointment_minutes
    FROM {{ ref('stg_opendental__appointment') }} apt
    WHERE apt.appointment_datetime >= CURRENT_DATE - INTERVAL '90 days'
    GROUP BY DATE(apt.appointment_datetime), apt.provider_id
),

ProviderAvailability AS (
    SELECT
        pa.provider_id,
        pa.schedule_date,
        pa.start_time,
        pa.end_time,
        CASE 
            WHEN pa.start_time IS NULL OR pa.end_time IS NULL THEN true
            ELSE false
        END as is_day_off,
        EXTRACT(EPOCH FROM (pa.end_time - pa.start_time))/60 as available_minutes
    FROM {{ ref('int_provider_availability') }} pa
),

DailySchedule AS (
    SELECT
        md5(COALESCE(ps.provider_id::text, '') || COALESCE(am.schedule_date::text, '')) as schedule_id,
        COALESCE(am.schedule_date, pa.schedule_date) as schedule_date,
        ps.provider_id,
        ps.provider_name,
        COALESCE(am.total_appointments, 0) as total_appointments,
        COALESCE(am.completed_appointments, 0) as completed_appointments,
        COALESCE(am.cancelled_appointments, 0) as cancelled_appointments,
        COALESCE(am.no_show_appointments, 0) as no_show_appointments,
        COALESCE(am.unconfirmed_appointments, 0) as unconfirmed_appointments,
        COALESCE(am.total_appointment_minutes, 0) as total_appointment_minutes,
        COALESCE(pa.available_minutes, 0) as available_minutes,
        CASE 
            WHEN COALESCE(pa.available_minutes, 0) = 0 THEN 0
            ELSE ROUND((COALESCE(am.total_appointment_minutes, 0)::numeric / pa.available_minutes) * 100, 2)
        END as utilization_rate,
        pa.start_time,
        pa.end_time,
        COALESCE(pa.is_day_off, true) as is_day_off
    FROM ProviderSchedule ps
    CROSS JOIN (
        SELECT DISTINCT schedule_date 
        FROM AppointmentMetrics
        UNION
        SELECT DISTINCT schedule_date 
        FROM ProviderAvailability
    ) dates
    LEFT JOIN AppointmentMetrics am
        ON ps.provider_id = am.provider_id
        AND dates.schedule_date = am.schedule_date
    LEFT JOIN ProviderAvailability pa
        ON ps.provider_id = pa.provider_id
        AND dates.schedule_date = pa.schedule_date
)

SELECT
    schedule_id,
    schedule_date,
    provider_id,
    provider_name,
    total_appointments,
    completed_appointments,
    cancelled_appointments,
    no_show_appointments,
    unconfirmed_appointments,
    total_appointment_minutes,
    available_minutes,
    utilization_rate,
    start_time,
    end_time,
    is_day_off,
    CURRENT_TIMESTAMP as model_created_at,
    CURRENT_TIMESTAMP as model_updated_at
FROM DailySchedule

{% if is_incremental() %}
WHERE schedule_date >= (SELECT MAX(schedule_date) FROM {{ this }})
{% endif %}
