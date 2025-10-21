{{
    config(
        materialized='table',
        schema='marts',
        unique_key='provider_id',
        on_schema_change='fail',
        indexes=[
            {'columns': ['provider_id'], 'unique': true},
            {'columns': ['_updated_at']},
            {'columns': ['provider_status']},
            {'columns': ['specialty_id']},
            {'columns': ['is_hidden']}
        ]
    )
}}

/*
    Dimension model for provider data
    Part of System G: Scheduling
    
    This model:
    1. Provides comprehensive provider information for scheduling, billing, and clinical operations
    2. Enhances provider data with availability metrics and business logic categorizations
    3. Supports provider performance analysis and practice management reporting
    
    Business Logic Features:
    - Provider Status Categorization: Active, Inactive, Terminated status classification
    - Provider Type Classification: Primary, Secondary, Instructor, Non-Person categorization
    - Availability Metrics: 90-day rolling availability calculations for scheduling optimization
    - Professional Credentials: Comprehensive credential tracking for compliance and billing
    
    Key Metrics:
    - Scheduled Days: Number of days provider was scheduled in last 90 days
    - Availability Percentage: Percentage of scheduled days provider was available
    - Average Daily Minutes: Average available minutes per scheduled day
    
    Data Quality Notes:
    - Provider ID 0 represents system-generated communications, not actual providers
    - Hidden providers are excluded from current UI but retained for historical data
    - Availability metrics are calculated over 90-day rolling window for performance
    
    Performance Considerations:
    - Uses table materialization for fast lookups across all mart models
    - Indexed on key lookup fields (provider_id, status, specialty)
    - Availability calculations limited to 90-day window for optimal performance
    
    Dependencies:
    - int_provider_profile: Intermediate model with provider data, business logic flags, and definition lookups
    - int_provider_availability: Provider scheduling and availability metrics
*/

-- 1. Source data retrieval from intermediate layer
with source_provider as (
    select * from {{ ref('int_provider_profile') }}
),


-- 3. Provider availability metrics
provider_availability as (
    select
        provider_id,
        count(distinct schedule_date) as scheduled_days,
        sum(available_minutes) as total_available_minutes,
        avg(available_minutes) as avg_daily_available_minutes,
        count(distinct case when is_day_off then schedule_date end) as days_off_count
    from {{ ref('int_provider_availability') }}
    where schedule_date >= current_date - interval '90 days'
    group by provider_id
),

-- 4. Business logic enhancement
provider_enhanced as (
    select
        -- Primary identification
        p.provider_id,
        
        -- Provider identifiers
        p.provider_abbreviation,
        p.last_name as provider_last_name,
        p.first_name as provider_first_name,
        p.middle_initial as provider_middle_initial,
        p.name_suffix as provider_suffix,
        p.preferred_name as provider_preferred_name,
        p.custom_id as provider_custom_id,
        
        -- Provider classifications
        p.fee_schedule_id,
        p.specialty_id,
        p.specialty_description,
        p.specialty_color,
        p.provider_status,
        p.provider_status_description,
        p.anesthesia_provider_type,
        p.anesthesia_type_description,
        
        -- Provider credentials
        p.state_license_number as state_license,
        p.dea_number,
        p.blue_cross_id,
        p.medicaid_id,
        p.national_provider_id,
        p.state_rx_id,
        p.state_where_licensed,
        p.taxonomy_code_override,
        
        -- Provider flags (already converted to boolean in staging)
        p.is_secondary,
        p.is_hidden,
        p.is_using_tin,
        p.has_signature_on_file,
        p.is_cdanet,
        p.is_not_person,
        p.is_instructor,
        p.is_hidden_report,
        p.is_erx_enabled,
        
        -- Provider display properties
        p.provider_color,
        p.outline_color,
        p.schedule_note,
        p.web_schedule_description,
        p.web_schedule_image_location,
        
        -- Financial and goals
        p.hourly_production_goal_amount,
        
        -- Availability metrics
        pa.scheduled_days,
        pa.total_available_minutes,
        pa.avg_daily_available_minutes,
        pa.days_off_count,
        
        -- Calculated availability metrics
        case 
            when pa.scheduled_days > 0 
            then round(pa.total_available_minutes::numeric / pa.scheduled_days, 2)
            else 0 
        end as avg_minutes_per_scheduled_day,
        
        case 
            when pa.scheduled_days > 0 
            then round((pa.scheduled_days - pa.days_off_count)::numeric / pa.scheduled_days * 100, 2)
            else 0 
        end as availability_percentage,
        
        -- Provider status categorization
        case
            when p.termination_date is not null then 'Terminated'
            when p.provider_status = 0 then 'Active'
            when p.provider_status = 1 then 'Inactive'
            else 'Unknown'
        end as provider_status_category,
        
        -- Provider type categorization
        case
            when p.is_instructor then 'Instructor'
            when p.is_secondary then 'Secondary'
            when p.is_not_person then 'Non-Person'
            else 'Primary'
        end as provider_type_category,
        
        -- Provider specialty categorization
        case
            when p.specialty_description = 'General' then 'General Practice'
            when p.specialty_description in ('Orthodontics', 'Oral Surgery', 'Endodontics', 'Periodontics') then 'Specialist'
            when p.specialty_description = 'Hygiene' then 'Hygiene'
            else 'Other'
        end as provider_specialty_category,
        
        -- Provider performance tier (based on availability)
        case
            when (case 
                when pa.scheduled_days > 0 
                then round((pa.scheduled_days - pa.days_off_count)::numeric / pa.scheduled_days * 100, 2)
                else 0 
            end) >= 95 then 'Excellent'
            when (case 
                when pa.scheduled_days > 0 
                then round((pa.scheduled_days - pa.days_off_count)::numeric / pa.scheduled_days * 100, 2)
                else 0 
            end) >= 90 then 'Good'
            when (case 
                when pa.scheduled_days > 0 
                then round((pa.scheduled_days - pa.days_off_count)::numeric / pa.scheduled_days * 100, 2)
                else 0 
            end) >= 80 then 'Fair'
            when (case 
                when pa.scheduled_days > 0 
                then round((pa.scheduled_days - pa.days_off_count)::numeric / pa.scheduled_days * 100, 2)
                else 0 
            end) > 0 then 'Poor'
            else 'No Data'
        end as availability_performance_tier,
        
        -- Metadata
        {{ standardize_mart_metadata(
            primary_source_alias='p',
            source_metadata_fields=['_loaded_at', '_created_at', '_updated_at']
        ) }}
        
    from source_provider p
    left join provider_availability pa
        on p.provider_id = pa.provider_id
),

-- 5. Final validation and filtering
final as (
    select * from provider_enhanced
    where provider_id is not null  -- Ensure valid provider IDs
)

select * from final