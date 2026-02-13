{{ config(
    materialized='incremental',
    unique_key='schedule_id',
    on_schema_change='sync_all_columns'
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

WITH
    {% if is_incremental() %}
    max_loaded AS (
        SELECT COALESCE(MAX(_loaded_at), '1900-01-01'::timestamp) AS cutoff
        FROM {{ this }}
    ),
    {% endif %}
date_spine AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="(CURRENT_DATE - INTERVAL '" ~ var('schedule_window_days') ~ " days')::date",
        end_date="CURRENT_DATE::date"
    ) }}
),

date_spine_formatted AS (
    SELECT date_day::date as schedule_date
    FROM date_spine
),

provider_schedule AS (
    SELECT
        p.provider_id,
        p.is_hidden,
        p.specialty_id as specialty
    FROM {{ ref('stg_opendental__provider') }} p
    WHERE p.is_hidden = false  -- false = not hidden, true = hidden
),

appointment_metrics AS (
    SELECT
        DATE(apt.appointment_datetime) as schedule_date,
        apt.provider_id,
        COUNT(*) as total_appointments,
        COUNT(CASE WHEN apt.appointment_status = 2 THEN 1 END) as completed_appointments,
        COUNT(CASE WHEN apt.appointment_status = 5 THEN 1 END) as cancelled_appointments,
        COUNT(CASE WHEN apt.appointment_status = 3 THEN 1 END) as no_show_appointments,
        COUNT(CASE WHEN apt.confirmation_status = 0 THEN 1 END) as unconfirmed_appointments,
        SUM({{ calculate_pattern_length('apt.pattern') }}) as total_appointment_minutes,
        MAX(apt._loaded_at) as _loaded_at
    FROM {{ ref('stg_opendental__appointment') }} apt
    WHERE apt.appointment_datetime >= CURRENT_DATE - INTERVAL '{{ var("schedule_window_days") }} days'
    {% if is_incremental() %}
        AND apt._loaded_at > (SELECT cutoff FROM max_loaded)
    {% endif %}
    GROUP BY DATE(apt.appointment_datetime), apt.provider_id
),

provider_availability AS (
    SELECT
        pa.provider_id,
        pa.schedule_date,
        pa.start_time,
        pa.end_time,
        pa._loaded_at,
        CASE 
            WHEN pa.start_time IS NULL OR pa.end_time IS NULL THEN true
            ELSE false
        END as is_day_off,
        EXTRACT(EPOCH FROM (pa.end_time - pa.start_time))/60 as available_minutes
    FROM {{ ref('int_provider_availability') }} pa
),

daily_schedule AS (
    SELECT
        md5(COALESCE(ps.provider_id::text, '') || COALESCE(ds.schedule_date::text, '')) as schedule_id,
        ds.schedule_date,
        ps.provider_id,
        COALESCE(am.total_appointments, 0) as total_appointments,
        COALESCE(am.completed_appointments, 0) as completed_appointments,
        COALESCE(am.cancelled_appointments, 0) as cancelled_appointments,
        COALESCE(am.no_show_appointments, 0) as no_show_appointments,
        COALESCE(am.unconfirmed_appointments, 0) as unconfirmed_appointments,
        COALESCE(am.total_appointment_minutes, 0) as total_appointment_minutes,
        COALESCE(pa.available_minutes, 480) as available_minutes,  -- Default to 8 hours (480 minutes) if NULL
        CASE 
            WHEN COALESCE(pa.available_minutes, 480) = 0 THEN 0
            ELSE ROUND((COALESCE(am.total_appointment_minutes, 0)::numeric / COALESCE(pa.available_minutes, 480)) * 100, 2)
        END as utilization_rate,
        pa.start_time,
        pa.end_time,
        COALESCE(pa.is_day_off, true) as is_day_off,
        COUNT(*) OVER (PARTITION BY ps.provider_id) as days_scheduled,
        COUNT(*) FILTER (WHERE NOT COALESCE(pa.is_day_off, true)) OVER (PARTITION BY ps.provider_id) as days_worked,
        GREATEST(COALESCE(am._loaded_at, '1900-01-01'::timestamp), COALESCE(pa._loaded_at, '1900-01-01'::timestamp)) as _loaded_at
    FROM provider_schedule ps
    CROSS JOIN date_spine_formatted ds
    LEFT JOIN appointment_metrics am
        ON ps.provider_id = am.provider_id
        AND ds.schedule_date = am.schedule_date
    LEFT JOIN provider_availability pa
        ON ps.provider_id = pa.provider_id
        AND ds.schedule_date = pa.schedule_date
    GROUP BY 
        ps.provider_id,
        ds.schedule_date,
        am.total_appointments,
        am.completed_appointments,
        am.cancelled_appointments,
        am.no_show_appointments,
        am.unconfirmed_appointments,
        am.total_appointment_minutes,
        pa.available_minutes,
        pa.start_time,
        pa.end_time,
        pa.is_day_off,
        am._loaded_at,
        pa._loaded_at
)

SELECT
    schedule_id,
    schedule_date,
    provider_id,
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
    days_scheduled,
    days_worked,
    _loaded_at,
    {{ standardize_intermediate_metadata(preserve_source_metadata=false) }}
FROM daily_schedule
{% if is_incremental() %}
WHERE _loaded_at > (SELECT COALESCE(MAX(_loaded_at), '1900-01-01'::timestamp) FROM {{ this }})
{% endif %}
