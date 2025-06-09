{{
    config(
        materialized='table',
        schema='marts',
        unique_key='appointment_id'
    )
}}

/*
Fact table for appointment scheduling and tracking.
This model captures all appointment-related activities including scheduling,
confirmations, cancellations, and no-shows for comprehensive practice analytics.

Key features:
- Complete appointment lifecycle tracking
- Provider and patient relationships
- Scheduling efficiency metrics
- Confirmation and reminder tracking
- Revenue and time utilization analysis

TODO - APPOINTMENT CONFIRMATION ENHANCEMENT:
When stg_opendental__confirmrequest staging model is created, add back:
1. AppointmentConfirmations CTE to aggregate confirmation data
2. Left join to AppointmentConfirmations in Final CTE
3. Replace default confirmation columns with actual data:
   - confirmation_count (currently defaults to 0)
   - last_confirmation_date (currently null)
   - confirmation_statuses (currently null array)
   - has_confirmations (currently false)
*/

with AppointmentBase as (
    select * from {{ ref('stg_opendental__appointment') }}
),

Final as (
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
        ab.appointment_date,
        ab.appointment_datetime,
        ab.time_pattern,
        ab.appointment_length_minutes,
        extract(hour from ab.appointment_datetime) as appointment_hour,
        extract(dow from ab.appointment_date) as day_of_week,
        case extract(dow from ab.appointment_date)
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
        
        case ab.appointment_type
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
        ab.confirmed_status,
        ab.is_hygiene_appointment,
        ab.is_new_patient,

        -- Scheduling Details
        ab.date_scheduled,
        ab.date_time_arrived,
        ab.date_time_seated,
        ab.date_time_dismissed,
        ab.date_time_in_operatory,
        
        -- Calculate timing metrics
        case 
            when ab.date_time_arrived is not null and ab.appointment_datetime is not null
            then extract(epoch from (ab.date_time_arrived - ab.appointment_datetime))/60
        end as arrival_delay_minutes,
        
        case 
            when ab.date_time_seated is not null and ab.date_time_arrived is not null
            then extract(epoch from (ab.date_time_seated - ab.date_time_arrived))/60
        end as wait_time_minutes,
        
        case 
            when ab.date_time_dismissed is not null and ab.date_time_seated is not null
            then extract(epoch from (ab.date_time_dismissed - ab.date_time_seated))/60
        end as treatment_time_minutes,

        -- Financial Information
        ab.production_goal,
        0.00 as total_scheduled_production,
        0.00 as scheduled_production_amount,
        
        -- Procedure Details (removed - no procedure data available)
        0 as procedure_count,
        null::text[] as procedure_codes,
        null::text[] as procedure_ids,

        -- Confirmation Details (TODO: restore when stg_opendental__confirmrequest is available)
        0 as confirmation_count,                    -- TODO: Replace with ac.confirmation_count
        null::timestamp as last_confirmation_date,  -- TODO: Replace with ac.last_confirmation_date
        null::text[] as confirmation_statuses,      -- TODO: Replace with ac.confirmation_statuses
        false as has_confirmations,                 -- TODO: Replace with case when ac.confirmation_count > 0

        -- Appointment Outcome Flags
        case when ab.appointment_status in (2, 7) then true else false end as is_completed,
        case when ab.appointment_status = 8 then true else false end as is_broken,
        case when ab.appointment_status = 3 then true else false end as is_asap,
        case when ab.date_time_arrived is null and ab.appointment_date < current_date then true else false end as is_no_show,
        case when ab.appointment_date > current_date then true else false end as is_future_appointment,

        -- Notes and Communication
        ab.appointment_note,
        ab.pattern_secondary,
        ab.color_override,

        -- Metadata
        ab._created_at,
        ab._updated_at,
        current_timestamp as _loaded_at

    from AppointmentBase ab
)

select * from Final
