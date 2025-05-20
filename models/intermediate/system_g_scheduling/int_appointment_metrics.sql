{{
    config(
        materialized='incremental',
        schema='intermediate',
        unique_key=['metric_id']
    )
}}

/*
    Intermediate model for appointment metrics.
    Provides key performance indicators and operational insights for the dental clinic.
    
    This model:
    1. Aggregates appointment data at multiple levels (provider, clinic, overall)
    2. Calculates completion, cancellation, and no-show rates
    3. Monitors schedule and chair time utilization
    4. Tracks average appointment lengths and wait times
    5. Measures new patient acquisition rates
    
    Part of System G: Scheduling
*/

WITH AppointmentMetrics AS (
    -- Provider-level metrics
    SELECT
        'provider' as metric_level,
        a.provider_id,
        DATE(a.appointment_datetime) as date,
        COUNT(*) as total_appointments,
        SUM(CASE WHEN a.is_complete THEN 1 ELSE 0 END) as completed_appointments,
        SUM(CASE WHEN a.appointment_status = 5 THEN 1 ELSE 0 END) as cancelled_appointments,
        SUM(CASE WHEN a.appointment_status = 3 THEN 1 ELSE 0 END) as no_show_appointments,
        AVG(a.actual_length) as average_appointment_length,
        AVG(a.waiting_time) as average_wait_time,
        SUM(CASE WHEN a.is_new_patient = 1 THEN 1 ELSE 0 END) as new_patient_appointments
    FROM {{ ref('int_appointment_details') }} a
    WHERE a.appointment_datetime >= CURRENT_DATE - INTERVAL '90 days'
    GROUP BY a.provider_id, DATE(a.appointment_datetime)

    UNION ALL

    -- Clinic-level metrics
    SELECT
        'clinic' as metric_level,
        NULL as provider_id,
        DATE(a.appointment_datetime) as date,
        COUNT(*) as total_appointments,
        SUM(CASE WHEN a.is_complete THEN 1 ELSE 0 END) as completed_appointments,
        SUM(CASE WHEN a.appointment_status = 5 THEN 1 ELSE 0 END) as cancelled_appointments,
        SUM(CASE WHEN a.appointment_status = 3 THEN 1 ELSE 0 END) as no_show_appointments,
        AVG(a.actual_length) as average_appointment_length,
        AVG(a.waiting_time) as average_wait_time,
        SUM(CASE WHEN a.is_new_patient = 1 THEN 1 ELSE 0 END) as new_patient_appointments
    FROM {{ ref('int_appointment_details') }} a
    WHERE a.appointment_datetime >= CURRENT_DATE - INTERVAL '90 days'
    GROUP BY DATE(a.appointment_datetime)

    UNION ALL

    -- Overall metrics
    SELECT
        'overall' as metric_level,
        NULL as provider_id,
        DATE(a.appointment_datetime) as date,
        COUNT(*) as total_appointments,
        SUM(CASE WHEN a.is_complete THEN 1 ELSE 0 END) as completed_appointments,
        SUM(CASE WHEN a.appointment_status = 5 THEN 1 ELSE 0 END) as cancelled_appointments,
        SUM(CASE WHEN a.appointment_status = 3 THEN 1 ELSE 0 END) as no_show_appointments,
        AVG(a.actual_length) as average_appointment_length,
        AVG(a.waiting_time) as average_wait_time,
        SUM(CASE WHEN a.is_new_patient = 1 THEN 1 ELSE 0 END) as new_patient_appointments
    FROM {{ ref('int_appointment_details') }} a
    WHERE a.appointment_datetime >= CURRENT_DATE - INTERVAL '90 days'
    GROUP BY DATE(a.appointment_datetime)
),

ScheduleUtilization AS (
    SELECT
        s.provider_id,
        s.schedule_date as date,
        s.available_minutes,
        s.total_appointment_minutes,
        CASE 
            WHEN s.available_minutes > 0 
            THEN (s.total_appointment_minutes::float / s.available_minutes) * 100 
            ELSE 0 
        END as schedule_utilization
    FROM {{ ref('int_appointment_schedule') }} s
    WHERE s.schedule_date >= CURRENT_DATE - INTERVAL '90 days'
),

ChairTimeUtilization AS (
    SELECT
        a.provider_id,
        DATE(a.appointment_datetime) as date,
        SUM(a.actual_length) as actual_chair_time,
        SUM(a.appointment_length) as scheduled_chair_time,
        CASE 
            WHEN SUM(a.appointment_length) > 0 
            THEN (SUM(a.actual_length)::float / SUM(a.appointment_length)) * 100 
            ELSE 0 
        END as chair_time_utilization
    FROM {{ ref('int_appointment_details') }} a
    WHERE a.appointment_datetime >= CURRENT_DATE - INTERVAL '90 days'
    GROUP BY a.provider_id, DATE(a.appointment_datetime)
)

SELECT
    -- Generate unique metric ID
    MD5(
        COALESCE(am.provider_id::text, '') || 
        am.date::text || 
        am.metric_level
    ) as metric_id,
    
    -- Base metrics
    am.date,
    am.provider_id,
    am.metric_level,
    am.total_appointments,
    am.completed_appointments,
    am.cancelled_appointments,
    
    -- Calculate rates
    CASE 
        WHEN am.total_appointments > 0 
        THEN (am.no_show_appointments::float / am.total_appointments) * 100 
        ELSE 0 
    END as no_show_rate,
    
    CASE 
        WHEN am.total_appointments > 0 
        THEN (am.cancelled_appointments::float / am.total_appointments) * 100 
        ELSE 0 
    END as cancellation_rate,
    
    CASE 
        WHEN am.total_appointments > 0 
        THEN (am.completed_appointments::float / am.total_appointments) * 100 
        ELSE 0 
    END as completion_rate,
    
    -- Utilization metrics
    COALESCE(su.schedule_utilization, 0) as schedule_utilization,
    COALESCE(ctu.chair_time_utilization, 0) as chair_time_utilization,
    
    -- Time metrics
    am.average_appointment_length,
    am.average_wait_time,
    
    -- New patient rate
    CASE 
        WHEN am.total_appointments > 0 
        THEN (am.new_patient_appointments::float / am.total_appointments) * 100 
        ELSE 0 
    END as new_patient_rate,
    
    -- Metadata
    CURRENT_TIMESTAMP as model_created_at,
    CURRENT_TIMESTAMP as model_updated_at

FROM AppointmentMetrics am
LEFT JOIN ScheduleUtilization su
    ON am.provider_id = su.provider_id
    AND am.date = su.date
LEFT JOIN ChairTimeUtilization ctu
    ON am.provider_id = ctu.provider_id
    AND am.date = ctu.date

{% if is_incremental() %}
WHERE am.date > (SELECT MAX(date) FROM {{ this }})
{% endif %}
