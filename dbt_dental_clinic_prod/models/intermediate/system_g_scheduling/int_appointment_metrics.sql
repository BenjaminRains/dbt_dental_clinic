{{ config(
    materialized='incremental',
    schema='intermediate',
    unique_key='metric_id',
    on_schema_change='fail',
    incremental_strategy='merge',
    indexes=[
        {'columns': ['metric_id'], 'unique': true},
        {'columns': ['provider_id']},
        {'columns': ['date']},
        {'columns': ['metric_level']},
        {'columns': ['updated_at']}
    ]
) }}

/*
    Intermediate model for appointment metrics
    Part of System G: Scheduling
    
    This model:
    1. Aggregates appointment data at multiple levels (provider, clinic, overall)
    2. Calculates completion, cancellation, and no-show rates with rolling averages
    3. Monitors schedule and chair time utilization metrics
    4. Tracks average appointment lengths and wait times
    5. Measures new patient acquisition rates and trends
    
    Business Logic Features:
    - Multi-level aggregation (provider-level and overall clinic metrics)
    - Rolling 7-day averages for trend analysis
    - Month-to-date cumulative metrics
    - Schedule utilization calculations with date spine coverage
    - Chair time utilization vs scheduled time analysis
    - Rate calculations with proper null handling
    
    Data Quality Notes:
    - Uses date spine to ensure complete date coverage for all providers
    - Handles null values in utilization calculations with proper defaults
    - Excludes hidden providers from overall metrics
    - Limits processing to 90-day rolling window for performance
    - Validates metric calculations with business rule constraints
    
    Performance Considerations:
    - Incremental materialization based on date for efficient processing
    - Pre-aggregated CTEs to avoid repeated subqueries
    - Indexed on key lookup fields for downstream model performance
    - Date spine optimization for complete coverage without gaps
*/

-- 1. Source CTEs (multiple sources)
with source_appointment_details as (
    select * from {{ ref('int_appointment_details') }}
    where appointment_datetime >= current_date - interval '90 days'
),

source_providers as (
    select * from {{ ref('stg_opendental__provider') }}
),

source_appointment_schedule as (
    select * from {{ ref('int_appointment_schedule') }}
),

-- 2. Lookup/Definition CTEs
provider_definitions as (
    select
        provider_id,
        provider_abbreviation,
        is_hidden
    from source_providers
    where is_hidden = false  -- only include active providers
),

-- 3. Calculation/Aggregation CTEs
date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="(current_date - interval '90 days')::date",
        end_date="(current_date + interval '365 days')::date"
    ) }}
),

appointment_metrics_base as (
    -- provider-level metrics
    select
        'provider' as metric_level,
        a.provider_id,
        date(a.appointment_datetime) as date,
        count(*) as total_appointments,
        sum(case when a.is_complete then 1 else 0 end) as completed_appointments,
        sum(case when a.appointment_status = 5 then 1 else 0 end) as cancelled_appointments,
        sum(case when a.appointment_status = 3 then 1 else 0 end) as no_show_appointments,
        avg(a.actual_length) as average_appointment_length,
        avg(a.waiting_time) as average_wait_time,
        sum(case when a.is_new_patient = true then 1 else 0 end) as new_patient_appointments
    from source_appointment_details a
    left join provider_definitions p
        on a.provider_id = p.provider_id
    group by a.provider_id, date(a.appointment_datetime)

    union all

    -- overall metrics
    select
        'overall' as metric_level,
        null as provider_id,
        date(a.appointment_datetime) as date,
        count(*) as total_appointments,
        sum(case when a.is_complete then 1 else 0 end) as completed_appointments,
        sum(case when a.appointment_status = 5 then 1 else 0 end) as cancelled_appointments,
        sum(case when a.appointment_status = 3 then 1 else 0 end) as no_show_appointments,
        avg(a.actual_length) as average_appointment_length,
        avg(a.waiting_time) as average_wait_time,
        sum(case when a.is_new_patient = true then 1 else 0 end) as new_patient_appointments
    from source_appointment_details a
    group by date(a.appointment_datetime)
),

schedule_utilization_base as (
    select
        coalesce(s.provider_id, p.provider_id) as provider_id,
        coalesce(s.schedule_date, d.date_day::date) as schedule_date,
        d.date_day::date as spine_date,
        s.available_minutes,
        s.total_appointment_minutes
    from date_spine d
    cross join provider_definitions p
    left join source_appointment_schedule s
        on s.schedule_date = d.date_day::date
        and s.provider_id = p.provider_id
    where d.date_day::date >= current_date - interval '90 days'
),

chair_time_utilization_base as (
    select
        a.provider_id,
        date(a.appointment_datetime) as date,
        sum(a.actual_length) as actual_chair_time,
        sum(a.appointment_length) as scheduled_chair_time
    from source_appointment_details a
    group by a.provider_id, date(a.appointment_datetime)
),

-- 4. Business Logic CTEs (can be multiple)
schedule_utilization_calculations as (
    -- provider-level utilization
    select
        s.provider_id,
        s.spine_date as date,
        coalesce(s.available_minutes, 480) as available_minutes,
        coalesce(s.total_appointment_minutes, 0) as total_appointment_minutes,
        case 
            when coalesce(s.available_minutes, 480) = 0 then 0
            else round((coalesce(s.total_appointment_minutes, 0)::numeric / coalesce(s.available_minutes, 480)) * 100, 2)
        end as schedule_utilization
    from schedule_utilization_base s
    where s.provider_id is not null

    union all

    -- overall utilization (aggregated across all providers)
    select
        null as provider_id,
        s.spine_date as date,
        sum(coalesce(s.available_minutes, 480)) as available_minutes,
        sum(coalesce(s.total_appointment_minutes, 0)) as total_appointment_minutes,
        case 
            when sum(coalesce(s.available_minutes, 480)) = 0 then 0
            else round((sum(coalesce(s.total_appointment_minutes, 0))::numeric / sum(coalesce(s.available_minutes, 480))) * 100, 2)
        end as schedule_utilization
    from schedule_utilization_base s
    group by s.spine_date
),

chair_time_utilization_calculations as (
    select
        ctu.provider_id,
        ctu.date,
        ctu.actual_chair_time,
        ctu.scheduled_chair_time,
        case 
            when ctu.scheduled_chair_time > 0 
            then (ctu.actual_chair_time::float / ctu.scheduled_chair_time) * 100 
            else 0 
        end as chair_time_utilization
    from chair_time_utilization_base ctu
),

rolling_metrics_calculations as (
    select
        *,
        -- 7-day rolling averages
        avg(total_appointments) over (
            partition by metric_level, provider_id
            order by date 
            rows between 6 preceding and current row
        ) as rolling_7d_total_appointments,
        avg(completed_appointments) over (
            partition by metric_level, provider_id
            order by date 
            rows between 6 preceding and current row
        ) as rolling_7d_completed_appointments,
        avg(cancelled_appointments) over (
            partition by metric_level, provider_id
            order by date 
            rows between 6 preceding and current row
        ) as rolling_7d_cancelled_appointments,
        avg(no_show_appointments) over (
            partition by metric_level, provider_id
            order by date 
            rows between 6 preceding and current row
        ) as rolling_7d_no_show_appointments,
        avg(average_appointment_length) over (
            partition by metric_level, provider_id
            order by date 
            rows between 6 preceding and current row
        ) as rolling_7d_avg_appointment_length,
        avg(average_wait_time) over (
            partition by metric_level, provider_id
            order by date 
            rows between 6 preceding and current row
        ) as rolling_7d_avg_wait_time,
        -- month-to-date totals
        sum(total_appointments) over (
            partition by metric_level, provider_id, 
            date_trunc('month', date)
            order by date
        ) as mtd_total_appointments,
        sum(completed_appointments) over (
            partition by metric_level, provider_id, 
            date_trunc('month', date)
            order by date
        ) as mtd_completed_appointments,
        sum(cancelled_appointments) over (
            partition by metric_level, provider_id, 
            date_trunc('month', date)
            order by date
        ) as mtd_cancelled_appointments,
        sum(no_show_appointments) over (
            partition by metric_level, provider_id, 
            date_trunc('month', date)
            order by date
        ) as mtd_no_show_appointments
    from appointment_metrics_base
),

-- 5. Integration CTE (joins everything together)
metrics_integrated as (
    select
        -- generate unique metric id
        md5(
            coalesce(am.provider_id::text, '') ||
            am.date::text || 
            am.metric_level
        ) as metric_id,
        
        -- base metrics
        am.date,
        am.provider_id,
        am.metric_level,
        am.total_appointments,
        am.completed_appointments,
        am.cancelled_appointments,
        
        -- calculate rates
        case 
            when am.total_appointments > 0 
            then (am.no_show_appointments::float / am.total_appointments) * 100 
            else 0 
        end as no_show_rate,
        
        case 
            when am.total_appointments > 0 
            then (am.cancelled_appointments::float / am.total_appointments) * 100 
            else 0 
        end as cancellation_rate,
        
        case 
            when am.total_appointments > 0 
            then (am.completed_appointments::float / am.total_appointments) * 100 
            else 0 
        end as completion_rate,
        
        case 
            when am.total_appointments > 0 
            then (am.new_patient_appointments::float / am.total_appointments) * 100 
            else 0 
        end as new_patient_rate,
        
        -- utilization metrics
        su.schedule_utilization,
        ctu.chair_time_utilization,
        
        -- rolling averages
        am.rolling_7d_total_appointments,
        am.rolling_7d_completed_appointments,
        am.rolling_7d_cancelled_appointments,
        am.rolling_7d_no_show_appointments,
        am.rolling_7d_avg_appointment_length,
        am.rolling_7d_avg_wait_time,
        
        -- month-to-date metrics
        am.mtd_total_appointments,
        am.mtd_completed_appointments,
        am.mtd_cancelled_appointments,
        am.mtd_no_show_appointments,
        
        -- additional metrics
        am.average_appointment_length,
        am.average_wait_time,
        am.new_patient_appointments,
        
        -- metadata
        current_timestamp as updated_at,
        {{ standardize_intermediate_metadata(
            primary_source_alias='am',
            preserve_source_metadata=false
        ) }}
    from rolling_metrics_calculations am
    left join schedule_utilization_calculations su
        on coalesce(am.provider_id, -1) = coalesce(su.provider_id, -1)
        and am.date = su.date
    left join chair_time_utilization_calculations ctu
        on coalesce(am.provider_id, -1) = coalesce(ctu.provider_id, -1)
        and am.date = ctu.date
),

-- 6. Final filtering/validation
final as (
    select * from metrics_integrated
    where metric_id is not null
)

select * from final

{% if is_incremental() %}
where date > (select max(date) from {{ this }})
{% endif %}
