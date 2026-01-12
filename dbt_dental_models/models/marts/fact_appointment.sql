{{
    config(
        materialized='table',
        schema='marts',
        unique_key='appointment_id',
        on_schema_change='fail',
        indexes=[
            {'columns': ['appointment_id'], 'unique': true},
            {'columns': ['patient_id']},
            {'columns': ['provider_id']},
            {'columns': ['appointment_date']},
            {'columns': ['_updated_at']}
        ]
    )
}}

/*
    Fact model for appointment scheduling and tracking
    Part of System G: Scheduling and Appointment Management
    
    This model:
    1. Captures all appointment-related activities including scheduling, confirmations, cancellations, and no-shows
    2. Provides comprehensive practice analytics for scheduling efficiency and patient flow
    3. Enables revenue and time utilization analysis through appointment lifecycle tracking
    
    Business Logic Features:
    - Complete appointment lifecycle tracking from scheduling to completion
    - Provider and patient relationship management with timing metrics
    - Scheduling efficiency metrics including wait times and treatment duration
    - Appointment outcome categorization (completed, no-show, cancelled, etc.)
    
    Key Metrics:
    - Arrival delay minutes: Time difference between scheduled and actual arrival
    - Wait time minutes: Time from arrival to being seated for treatment
    - Treatment time minutes: Duration of actual treatment session
    - Appointment status categorization with business rule logic
    
    Data Quality Notes:
    - Appointment confirmation functionality is not currently used by the clinic (confirmation fields set to defaults)
    - Procedure details are not available at appointment level (set to null/zero values)
    - Some appointment types may have legacy or non-standard status codes
    - Time calculations include data quality validation to handle illogical time sequences
    - Negative time values are converted to NULL to prevent calculation errors in downstream models
    - Appointment duration calculations require dismissed_datetime >= appointment_datetime for validity
    
    Performance Considerations:
    - Indexed on appointment_id (unique), patient_id, provider_id, and appointment_date for query optimization
    - Date-based partitioning considerations for large appointment volumes
    - Complex CASE statements for status categorization may impact query performance
    
    Dependencies:
    - int_appointment_details: Intermediate model with all appointment data including clinic, hygienist, priority, 
      confirmation status, seated datetime, pattern secondary, and color override fields
*/

-- 1. Source data retrieval from intermediate layer
with source_appointment as (
    select * from {{ ref('int_appointment_details') }}
),


-- 2. Business logic and calculations
appointment_calculated as (
    select
        -- Primary Key
        ab.appointment_id,

        -- Foreign Keys
        ab.patient_id,
        ab.provider_id,
        ab.operatory as operatory_id,
        ab.appointment_type_id,
        ab.clinic_id,
        ab.hygienist_id,

        -- Date and Time Details
        date(ab.appointment_datetime) as appointment_date,
        ab.appointment_datetime,
        '' as time_pattern,  -- pattern not available in intermediate
        case 
            when ab.check_out_time is not null and ab.appointment_datetime is not null
            and ab.check_out_time >= ab.appointment_datetime  -- Ensure logical sequence
            then extract(epoch from (ab.check_out_time - ab.appointment_datetime))/60
            else ab.actual_length  -- Use pre-calculated length from intermediate
        end as appointment_length_minutes,
        extract(hour from ab.appointment_datetime) as appointment_hour,
        extract(dow from date(ab.appointment_datetime)) as day_of_week,
        case extract(dow from date(ab.appointment_datetime))
            when 0 then 'Sunday'
            when 1 then 'Monday'
            when 2 then 'Tuesday'
            when 3 then 'Wednesday'
            when 4 then 'Thursday'
            when 5 then 'Friday'
            when 6 then 'Saturday'
        end as day_of_week_name,

        -- Appointment Status and Type (using intermediate model fields)
        ab.appointment_status_desc as appointment_status,
        coalesce(ab.appointment_type_name, 'Unknown') as appointment_type,

        ab.priority,
        ab.is_confirmed as confirmation_status,
        ab.is_hygiene as is_hygiene_appointment,
        ab.is_new_patient,

        -- Scheduling Details
        ab.created_at as date_scheduled,
        ab.check_in_time as date_time_arrived,
        ab.seated_datetime as date_time_seated,
        ab.check_out_time as date_time_dismissed,
        ab.seated_datetime as date_time_in_operatory,
        
        -- Calculate timing metrics with data quality validation
        case 
            when ab.check_in_time is not null and ab.appointment_datetime is not null
            and ab.check_in_time >= ab.appointment_datetime - interval '1 day'  -- Arrival can't be more than 1 day before appointment
            and ab.check_in_time <= ab.appointment_datetime + interval '2 days'  -- Arrival can't be more than 2 days after appointment
            then extract(epoch from (ab.check_in_time - ab.appointment_datetime))/60
            else null
        end as arrival_delay_minutes,
        
        case 
            when ab.seated_datetime is not null and ab.check_in_time is not null
            and ab.seated_datetime >= ab.check_in_time  -- Ensure logical sequence
            then extract(epoch from (ab.seated_datetime - ab.check_in_time))/60
            else null
        end as wait_time_minutes,
        
        case 
            when ab.check_out_time is not null and ab.seated_datetime is not null
            and ab.check_out_time >= ab.seated_datetime  -- Ensure logical sequence
            then extract(epoch from (ab.check_out_time - ab.seated_datetime))/60
            else null
        end as treatment_time_minutes,

        -- Financial Information
        -- Calculate scheduled production from linked procedures, with fallback to appointment type estimates
        -- Note: production_goal and total_scheduled_production are unused placeholder fields
        -- - production_goal: Real goals come from dim_provider.hourly_production_goal_amount
        -- - total_scheduled_production: Downstream models use scheduled_production_amount instead
        0.00 as production_goal,
        0.00 as total_scheduled_production,
        coalesce(
            -- PRIMARY: Get actual production from linked procedures (used for real clinic data)
            -- Real clinic data: Broken appointments typically have procedures (Status 1: Treatment Planned) with fees
            -- This subquery will find them and return the actual sum, so the fallback below is NOT used for real data
            (select sum(pc.procedure_fee)
             from {{ ref('int_procedure_complete') }} pc
             where pc.appointment_id = ab.appointment_id
               and pc.procedure_date = date(ab.appointment_datetime)),
            -- FALLBACK: Estimate from appointment type (primarily for SYNTHETIC DATA)
            -- Synthetic data: Broken appointments don't have procedures created, so this fallback provides realistic estimates
            -- This fallback is rarely (if ever) used for real clinic data since procedures typically exist
            case 
                when ab.appointment_type_name = 'Comprehensive Exam' then 250.00
                when ab.appointment_type_name = 'Periodic Exam' then 150.00
                when ab.appointment_type_name = 'Emergency' then 200.00
                when ab.appointment_type_name = 'Hygiene Adult' then 120.00
                when ab.appointment_type_name = 'Hygiene Child' then 100.00
                when ab.appointment_type_name = 'Crown Prep' then 1200.00
                when ab.appointment_type_name = 'Crown Seat' then 800.00
                when ab.appointment_type_name = 'Filling' then 300.00
                when ab.appointment_type_name = 'Root Canal' then 1500.00
                when ab.appointment_type_name = 'Extraction' then 250.00
                when ab.appointment_type_name = 'Denture Delivery' then 600.00
                when ab.appointment_type_name = 'Consultation' then 100.00
                when ab.appointment_type_name = 'Follow-up' then 75.00
                when ab.appointment_type_name = 'SRP Quad' then 400.00
                when ab.appointment_type_name = 'Perio Maintenance' then 120.00
                else 200.00  -- Default estimate for unknown appointment types
            end
        ) as scheduled_production_amount,
        
        -- Procedure Details (removed - no procedure data available)
        0 as procedure_count,
        null::text[] as procedure_codes,
        null::text[] as procedure_ids,

        -- Confirmation Details (not used by clinic)
        0 as confirmation_count,
        null::timestamp as last_confirmation_date,
        null::text[] as confirmation_statuses,
        false as has_confirmations,

        -- Appointment Outcome Flags (using intermediate model logic)
        ab.is_complete as is_completed,
        case when ab.appointment_status = 5 then true else false end as is_broken,  -- 5 = Broken in intermediate
        case when ab.appointment_status = 4 then true else false end as is_asap,     -- 4 = ASAP in intermediate
        case when ab.check_in_time is null and date(ab.appointment_datetime) < current_date and not ab.is_complete then true else false end as is_no_show,
        case when date(ab.appointment_datetime) > current_date then true else false end as is_future_appointment,

        -- Business Logic Enhancement - Appointment Categorization
        case 
            when ab.is_complete then 'Completed'
            when ab.appointment_status = 5 then 'Cancelled'  -- 5 = Broken
            when ab.appointment_status = 4 then 'ASAP'       -- 4 = ASAP
            when ab.check_in_time is null and date(ab.appointment_datetime) < current_date and not ab.is_complete then 'No Show'
            else 'Scheduled'
        end as appointment_outcome_status,
        
        case 
            when ab.check_out_time is not null and ab.appointment_datetime is not null
            and ab.check_out_time >= ab.appointment_datetime then
                case 
                    when extract(epoch from (ab.check_out_time - ab.appointment_datetime))/60 <= 30 then 'Short'
                    when extract(epoch from (ab.check_out_time - ab.appointment_datetime))/60 <= 60 then 'Standard'
                    when extract(epoch from (ab.check_out_time - ab.appointment_datetime))/60 <= 120 then 'Long'
                    else 'Extended'
                end
            else 'Unknown'
        end as appointment_duration_category,

        -- Notes and Communication
        ab.note as appointment_note,
        ab.pattern_secondary,
        ab.color_override,

        -- Metadata (using intermediate model metadata)
        {{ standardize_mart_metadata(
            primary_source_alias='ab',
            source_metadata_fields=['_loaded_at', '_updated_at', '_created_by']
        ) }}

    from source_appointment ab
),

-- 3. Final validation
final as (
    select * from appointment_calculated
)

select * from final