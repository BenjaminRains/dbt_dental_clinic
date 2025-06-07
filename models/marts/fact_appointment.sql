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
*/

with AppointmentBase as (
    select * from {{ ref('stg_opendental__appointment') }}
),

AppointmentConfirmations as (
    select 
        appointment_id,
        count(*) as confirmation_count,
        max(date_time_confirmed) as last_confirmation_date,
        array_agg(distinct confirmation_status::text) as confirmation_statuses
    from {{ ref('stg_opendental__confirmrequest') }}
    group by appointment_id
),

AppointmentProcedures as (
    select 
        appointment_id,
        count(*) as procedure_count,
        sum(procedure_fee) as total_scheduled_production,
        array_agg(procedure_code::text) as procedure_codes,
        array_agg(procedure_id::text) as procedure_ids
    from {{ ref('stg_opendental__appointmentprocedure') }}
    group by appointment_id
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
        ap.total_scheduled_production,
        coalesce(ap.total_scheduled_production, 0) as scheduled_production_amount,
        
        -- Procedure Details
        ap.procedure_count,
        ap.procedure_codes,
        ap.procedure_ids,

        -- Confirmation Details
        ac.confirmation_count,
        ac.last_confirmation_date,
        ac.confirmation_statuses,
        case when ac.confirmation_count > 0 then true else false end as has_confirmations,

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
    left join AppointmentConfirmations ac
        on ab.appointment_id = ac.appointment_id
    left join AppointmentProcedures ap
        on ab.appointment_id = ap.appointment_id
)

select * from Final
