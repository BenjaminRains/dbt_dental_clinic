{{
    config(
        materialized='incremental',
        schema='intermediate',
        unique_key='schedule_id',
        on_schema_change='fail',
        incremental_strategy='merge',
        indexes=[
            {'columns': ['schedule_id'], 'unique': true},
            {'columns': ['provider_id']},
            {'columns': ['schedule_date']},
            {'columns': ['_transformed_at']}
        ]
    )
}}

/*
    Intermediate model for appointment scheduling.
    Part of System G: Scheduling
    
    This model:
    1. Creates comprehensive daily schedule views for all providers
    2. Calculates appointment metrics and utilization rates
    3. Integrates provider availability with appointment data
    4. Provides foundation for scheduling analytics and capacity planning
    
    Business Logic Features:
    - Daily schedule spine with complete date coverage
    - Provider availability integration with appointment metrics
    - Utilization rate calculations with proper null handling
    - Schedule optimization and capacity planning support
    
    Data Quality Notes:
    - Uses date spine to ensure complete coverage without gaps
    - Handles null values in utilization calculations with default 480 minutes (8 hours)
    - Excludes hidden providers from schedule calculations
    - Validates schedule time windows and availability
    
    Performance Considerations:
    - Incremental processing with date-based filtering
    - Optimized joins with proper indexing strategy
    - Limited to configurable rolling window for performance
*/

-- 1. Source CTEs (multiple sources)
WITH source_appointments AS (
    SELECT * FROM {{ ref('stg_opendental__appointment') }}
),

source_providers AS (
    SELECT * FROM {{ ref('stg_opendental__provider') }}
),

source_provider_availability AS (
    SELECT * FROM {{ ref('int_provider_availability') }}
),

-- 2. Lookup/Definition CTEs
date_spine AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="(CURRENT_DATE - INTERVAL '90 days')::date",
        end_date="CURRENT_DATE::date"
    ) }}
),

-- 3. Calculation/Aggregation CTEs
appointment_metrics AS (
    SELECT
        DATE(apt.appointment_datetime) as schedule_date,
        apt.provider_id,
        COUNT(*) as total_appointments,
        COUNT(CASE WHEN apt.appointment_status = 2 THEN 1 END) as completed_appointments,
        COUNT(CASE WHEN apt.appointment_status = 5 THEN 1 END) as cancelled_appointments,
        COUNT(CASE WHEN apt.appointment_status = 3 THEN 1 END) as no_show_appointments,
        COUNT(CASE WHEN apt.confirmation_status = 0 THEN 1 END) as unconfirmed_appointments,
        SUM({{ calculate_pattern_length('apt.pattern') }}) as total_appointment_minutes
    FROM source_appointments apt
    WHERE apt.appointment_datetime >= CURRENT_DATE - INTERVAL '90 days'
    GROUP BY DATE(apt.appointment_datetime), apt.provider_id
),

-- 4. Business Logic CTEs (can be multiple)
provider_schedule_base AS (
    SELECT
        p.provider_id,
        p.provider_abbreviation as provider_name,
        {{ convert_opendental_boolean('p.is_hidden') }} as is_hidden,
        p.specialty_id as specialty
    FROM source_providers p
    WHERE {{ convert_opendental_boolean('p.is_hidden') }} = false  -- Only active providers
),

schedule_date_spine AS (
    SELECT date_day::date as schedule_date
    FROM date_spine
),

provider_availability_enhanced AS (
    SELECT
        pa.provider_id,
        pa.schedule_date,
        pa.start_time,
        pa.end_time,
        CASE 
            WHEN pa.start_time IS NULL OR pa.end_time IS NULL THEN true
            ELSE false
        END as is_day_off,
        COALESCE(pa.available_minutes, 0) as available_minutes
    FROM source_provider_availability pa
),

-- 5. Integration CTE (joins everything together)
schedule_integrated AS (
    SELECT
        -- Core identification
        md5(COALESCE(ps.provider_id::text, '') || COALESCE(ds.schedule_date::text, '')) as schedule_id,
        ds.schedule_date,
        ps.provider_id,
        ps.provider_name,
        
        -- Appointment metrics
        COALESCE(am.total_appointments, 0) as total_appointments,
        COALESCE(am.completed_appointments, 0) as completed_appointments,
        COALESCE(am.cancelled_appointments, 0) as cancelled_appointments,
        COALESCE(am.no_show_appointments, 0) as no_show_appointments,
        COALESCE(am.unconfirmed_appointments, 0) as unconfirmed_appointments,
        COALESCE(am.total_appointment_minutes, 0) as total_appointment_minutes,
        
        -- Availability metrics
        COALESCE(pa.available_minutes, 480) as available_minutes,  -- Default to 8 hours (480 minutes) if NULL
        pa.start_time,
        pa.end_time,
        COALESCE(pa.is_day_off, true) as is_day_off,
        
        -- Calculated metrics
        CASE 
            WHEN COALESCE(pa.available_minutes, 480) = 0 THEN 0
            ELSE ROUND((COALESCE(am.total_appointment_minutes, 0)::numeric / COALESCE(pa.available_minutes, 480)) * 100, 2)
        END as utilization_rate,
        
        -- Window functions for provider-level metrics
        COUNT(*) OVER (PARTITION BY ps.provider_id) as days_scheduled,
        COUNT(*) FILTER (WHERE NOT COALESCE(pa.is_day_off, true)) OVER (PARTITION BY ps.provider_id) as days_worked
    FROM provider_schedule_base ps
    CROSS JOIN schedule_date_spine ds
    LEFT JOIN appointment_metrics am
        ON ps.provider_id = am.provider_id
        AND ds.schedule_date = am.schedule_date
    LEFT JOIN provider_availability_enhanced pa
        ON ps.provider_id = pa.provider_id
        AND ds.schedule_date = pa.schedule_date
),

-- 6. Final filtering/validation
final AS (
    SELECT * FROM schedule_integrated
    WHERE schedule_date >= CURRENT_DATE - INTERVAL '90 days'
)

SELECT
    -- Core identification
    schedule_id,
    schedule_date,
    provider_id,
    provider_name,
    
    -- Appointment metrics
    total_appointments,
    completed_appointments,
    cancelled_appointments,
    no_show_appointments,
    unconfirmed_appointments,
    total_appointment_minutes,
    
    -- Availability metrics
    available_minutes,
    utilization_rate,
    start_time,
    end_time,
    is_day_off,
    
    -- Provider-level metrics
    days_scheduled,
    days_worked,
    
    -- Standardized metadata
    {{ standardize_intermediate_metadata() }}

FROM final

{% if is_incremental() %}
WHERE schedule_date >= (SELECT MAX(schedule_date) FROM {{ this }})
{% endif %}
