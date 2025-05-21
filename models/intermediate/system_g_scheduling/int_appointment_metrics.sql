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
    LEFT JOIN {{ ref('stg_opendental__provider') }} p
        ON a.provider_id = p.provider_id
    WHERE a.appointment_datetime >= CURRENT_DATE - INTERVAL '90 days'
    GROUP BY a.provider_id, DATE(a.appointment_datetime)

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

date_spine AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="(CURRENT_DATE - INTERVAL '90 days')::date",
        end_date="(CURRENT_DATE + INTERVAL '365 days')::date"
    ) }}
),

schedule_with_dates AS (
    SELECT
        COALESCE(s.provider_id, p.provider_id) as provider_id,
        COALESCE(s.schedule_date, d.date_day::date) as schedule_date,
        d.date_day::date as spine_date,
        s.available_minutes,
        s.total_appointment_minutes
    FROM date_spine d
    CROSS JOIN {{ ref('stg_opendental__provider') }} p
    LEFT JOIN {{ ref('int_appointment_schedule') }} s
        ON s.schedule_date = d.date_day::date
        AND s.provider_id = p.provider_id
    WHERE d.date_day::date >= CURRENT_DATE - INTERVAL '90 days'
        AND p.is_hidden = 0  -- Only include active providers
),

ScheduleUtilization AS (
    -- Provider-level utilization
    SELECT
        s.provider_id,
        s.spine_date as date,
        COALESCE(s.available_minutes, 480) as available_minutes,
        COALESCE(s.total_appointment_minutes, 0) as total_appointment_minutes,
        CASE 
            WHEN COALESCE(s.available_minutes, 480) = 0 THEN 0
            ELSE ROUND((COALESCE(s.total_appointment_minutes, 0)::numeric / COALESCE(s.available_minutes, 480)) * 100, 2)
        END as schedule_utilization
    FROM schedule_with_dates s
    WHERE s.provider_id IS NOT NULL

    UNION ALL

    -- Overall utilization (aggregated across all providers)
    SELECT
        NULL as provider_id,
        s.spine_date as date,
        SUM(COALESCE(s.available_minutes, 480)) as available_minutes,
        SUM(COALESCE(s.total_appointment_minutes, 0)) as total_appointment_minutes,
        CASE 
            WHEN SUM(COALESCE(s.available_minutes, 480)) = 0 THEN 0
            ELSE ROUND((SUM(COALESCE(s.total_appointment_minutes, 0))::numeric / SUM(COALESCE(s.available_minutes, 480))) * 100, 2)
        END as schedule_utilization
    FROM schedule_with_dates s
    GROUP BY s.spine_date
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
),

-- Add rolling averages
RollingMetrics AS (
    SELECT
        *,
        -- 7-day rolling averages
        AVG(total_appointments) OVER (
            PARTITION BY metric_level, provider_id
            ORDER BY date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as rolling_7d_total_appointments,
        AVG(completed_appointments) OVER (
            PARTITION BY metric_level, provider_id
            ORDER BY date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as rolling_7d_completed_appointments,
        AVG(cancelled_appointments) OVER (
            PARTITION BY metric_level, provider_id
            ORDER BY date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as rolling_7d_cancelled_appointments,
        AVG(no_show_appointments) OVER (
            PARTITION BY metric_level, provider_id
            ORDER BY date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as rolling_7d_no_show_appointments,
        AVG(average_appointment_length) OVER (
            PARTITION BY metric_level, provider_id
            ORDER BY date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as rolling_7d_avg_appointment_length,
        AVG(average_wait_time) OVER (
            PARTITION BY metric_level, provider_id
            ORDER BY date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as rolling_7d_avg_wait_time,
        -- Month-to-date totals
        SUM(total_appointments) OVER (
            PARTITION BY metric_level, provider_id, 
            DATE_TRUNC('month', date)
            ORDER BY date
        ) as mtd_total_appointments,
        SUM(completed_appointments) OVER (
            PARTITION BY metric_level, provider_id, 
            DATE_TRUNC('month', date)
            ORDER BY date
        ) as mtd_completed_appointments,
        SUM(cancelled_appointments) OVER (
            PARTITION BY metric_level, provider_id, 
            DATE_TRUNC('month', date)
            ORDER BY date
        ) as mtd_cancelled_appointments,
        SUM(no_show_appointments) OVER (
            PARTITION BY metric_level, provider_id, 
            DATE_TRUNC('month', date)
            ORDER BY date
        ) as mtd_no_show_appointments
    FROM AppointmentMetrics
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
    
    CASE 
        WHEN am.total_appointments > 0 
        THEN (am.new_patient_appointments::float / am.total_appointments) * 100 
        ELSE 0 
    END as new_patient_rate,
    
    -- Utilization metrics
    su.schedule_utilization,
    ctu.chair_time_utilization,
    
    -- Rolling averages
    am.rolling_7d_total_appointments,
    am.rolling_7d_completed_appointments,
    am.rolling_7d_cancelled_appointments,
    am.rolling_7d_no_show_appointments,
    am.rolling_7d_avg_appointment_length,
    am.rolling_7d_avg_wait_time,
    
    -- Month-to-date metrics
    am.mtd_total_appointments,
    am.mtd_completed_appointments,
    am.mtd_cancelled_appointments,
    am.mtd_no_show_appointments,
    
    -- Additional metrics
    am.average_appointment_length,
    am.average_wait_time,
    am.new_patient_appointments,
    
    -- Metadata
    current_timestamp as dbt_created_at,
    '{{ invocation_id }}' as dbt_pipeline_id,
    '{{ this.name }}' as dbt_model,
    '{{ this.schema }}' as dbt_schema

FROM RollingMetrics am
LEFT JOIN ScheduleUtilization su
    ON COALESCE(am.provider_id, -1) = COALESCE(su.provider_id, -1)
    AND am.date = su.date
LEFT JOIN ChairTimeUtilization ctu
    ON COALESCE(am.provider_id, -1) = COALESCE(ctu.provider_id, -1)
    AND am.date = ctu.date

{% if is_incremental() %}
WHERE am.date > (SELECT MAX(date) FROM {{ this }})
{% endif %}
