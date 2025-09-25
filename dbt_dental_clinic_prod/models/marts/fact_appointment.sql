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
    - stg_opendental__appointment: Primary source data for appointment details
*/

-- 1. Source data retrieval
with source_appointment as (
    select * from {{ ref('stg_opendental__appointment') }}
),

-- 2. Business logic and calculations
appointment_calculated as (
    select
        -- Primary Key
        ab.appointment_id,

        -- Foreign Keys
        ab.patient_id,
        ab.provider_id,
        ab.operatory_id,
        ab.appointment_type_id,
        ab.clinic_id,
        ab.hygienist_id,

        -- Date and Time Details
        date(ab.appointment_datetime) as appointment_date,
        ab.appointment_datetime,
        ab.pattern as time_pattern,
        case 
            when ab.dismissed_datetime is not null and ab.appointment_datetime is not null
            and ab.dismissed_datetime >= ab.appointment_datetime  -- Ensure logical sequence
            then extract(epoch from (ab.dismissed_datetime - ab.appointment_datetime))/60
            else null
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

        -- Appointment Status and Type
        case ab.appointment_status
            when 1 then 'Scheduled'
            when 2 then 'Complete'
            when 3 then 'ASAP'
            when 4 then 'UnschedList'
            when 5 then 'Planned'
            when 6 then 'PtNote'
            when 7 then 'PtNoteCompleted'
            when 8 then 'Broken'
            when 9 then 'None'
            else 'Unknown'
        end as appointment_status,
        
        case ab.appointment_type_id
            when 0 then 'Patient'
            when 1 then 'NewPatient'
            when 2 then 'Hygiene'
            when 3 then 'Prophy'
            when 4 then 'Perio'
            when 5 then 'Restorative'
            when 6 then 'Crown'
            when 7 then 'SRP'
            when 8 then 'Denture'
            when 9 then 'Other'
            else 'Unknown'
        end as appointment_type,

        ab.priority,
        ab.confirmation_status,
        ab.is_hygiene as is_hygiene_appointment,
        ab.is_new_patient,

        -- Scheduling Details (using actual staging column names)
        ab.sec_date_t_entry as date_scheduled,
        ab.arrival_datetime as date_time_arrived,
        ab.seated_datetime as date_time_seated,
        ab.dismissed_datetime as date_time_dismissed,
        ab.seated_datetime as date_time_in_operatory,
        
        -- Calculate timing metrics with data quality validation
        case 
            when ab.arrival_datetime is not null and ab.appointment_datetime is not null
            then extract(epoch from (ab.arrival_datetime - ab.appointment_datetime))/60
            else null
        end as arrival_delay_minutes,
        
        case 
            when ab.seated_datetime is not null and ab.arrival_datetime is not null
            and ab.seated_datetime >= ab.arrival_datetime  -- Ensure logical sequence
            then extract(epoch from (ab.seated_datetime - ab.arrival_datetime))/60
            else null
        end as wait_time_minutes,
        
        case 
            when ab.dismissed_datetime is not null and ab.seated_datetime is not null
            and ab.dismissed_datetime >= ab.seated_datetime  -- Ensure logical sequence
            then extract(epoch from (ab.dismissed_datetime - ab.seated_datetime))/60
            else null
        end as treatment_time_minutes,

        -- Financial Information (not available at appointment level)
        0.00 as production_goal,
        0.00 as total_scheduled_production,
        0.00 as scheduled_production_amount,
        
        -- Procedure Details (removed - no procedure data available)
        0 as procedure_count,
        null::text[] as procedure_codes,
        null::text[] as procedure_ids,

        -- Confirmation Details (not used by clinic)
        0 as confirmation_count,
        null::timestamp as last_confirmation_date,
        null::text[] as confirmation_statuses,
        false as has_confirmations,

        -- Appointment Outcome Flags
        case when ab.appointment_status in (2, 7) then true else false end as is_completed,
        case when ab.appointment_status = 8 then true else false end as is_broken,
        case when ab.appointment_status = 3 then true else false end as is_asap,
        case when ab.arrival_datetime is null and date(ab.appointment_datetime) < current_date then true else false end as is_no_show,
        case when date(ab.appointment_datetime) > current_date then true else false end as is_future_appointment,

        -- Business Logic Enhancement - Appointment Categorization
        case 
            when ab.appointment_status in (2, 7) then 'Completed'
            when ab.appointment_status = 8 then 'Cancelled'
            when ab.appointment_status = 3 then 'ASAP'
            when ab.arrival_datetime is null and date(ab.appointment_datetime) < current_date then 'No Show'
            else 'Scheduled'
        end as appointment_outcome_status,
        
        case 
            when ab.dismissed_datetime is not null and ab.appointment_datetime is not null
            and ab.dismissed_datetime >= ab.appointment_datetime then
                case 
                    when extract(epoch from (ab.dismissed_datetime - ab.appointment_datetime))/60 <= 30 then 'Short'
                    when extract(epoch from (ab.dismissed_datetime - ab.appointment_datetime))/60 <= 60 then 'Standard'
                    when extract(epoch from (ab.dismissed_datetime - ab.appointment_datetime))/60 <= 120 then 'Long'
                    else 'Extended'
                end
            else 'Unknown'
        end as appointment_duration_category,

        -- Notes and Communication
        ab.note as appointment_note,
        ab.pattern_secondary,
        ab.color_override

    from source_appointment ab
),

-- 3. Final validation and metadata
final as (
    select
        *,
        -- Metadata
        {{ standardize_mart_metadata() }}
    from appointment_calculated
)

select * from final