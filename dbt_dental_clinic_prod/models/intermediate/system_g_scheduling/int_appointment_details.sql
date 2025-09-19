{{ config(
    materialized='incremental',
    schema='intermediate',
    unique_key='appointment_id',
    on_schema_change='fail',
    incremental_strategy='merge',
    indexes=[
        {'columns': ['appointment_id'], 'unique': true},
        {'columns': ['patient_id']},
        {'columns': ['provider_id']},
        {'columns': ['appointment_datetime']},
        {'columns': ['_updated_at']}
    ]
) }}

/*
    Intermediate model for appointment details
    Part of System G: Scheduling
    
    This model:
    1. Provides comprehensive appointment data with patient and provider information
    2. Calculates appointment timing metrics and business logic
    3. Tracks appointment status and confirmation details
    4. Integrates historical appointment data for complete context
    
    Business Logic Features:
    - Appointment duration calculations using OpenDental pattern format
    - Actual length calculation based on check-in/check-out times
    - Waiting time calculation for patient experience tracking
    - Status standardization and business rule validation
    - Historical appointment tracking for rescheduling and cancellations
    
    Data Quality Notes:
    - Handles placeholder times (00:00:00) in check-in/check-out data
    - Validates appointment end time is after start time
    - Ensures actual length calculations account for business hours only
    - Manages missing or invalid appointment type data (type_id = 0)
    
    Performance Considerations:
    - Incremental materialization based on appointment_datetime
    - Complex business hours calculation optimized with recursive CTE
    - Indexed on key lookup fields for downstream model performance
*/

-- 1. Source CTEs (multiple sources)
with source_appointments as (
    select * from {{ ref('stg_opendental__appointment') }}
    {% if is_incremental() %}
    where appointment_datetime > (select max(appointment_datetime) from {{ this }})
    {% endif %}
),

source_appointment_types as (
    select * from {{ ref('stg_opendental__appointmenttype') }}
),

source_providers as (
    select * from {{ ref('stg_opendental__provider') }}
),

source_patients as (
    select * from {{ ref('stg_opendental__patient') }}
),

source_historical_appointments as (
    select * from {{ ref('stg_opendental__histappointment') }}
),

-- 2. Lookup/Definition CTEs
appointment_type_definitions as (
    select
        appointment_type_id,
        appointment_type_name,
        {{ calculate_pattern_length('pattern') }} as appointment_length,
        appointment_type_color as color
    from source_appointment_types
),

provider_definitions as (
    select
        provider_id,
        provider_abbreviation as provider_abbr,
        provider_color,
        is_hidden,
        specialty_id as specialty
    from source_providers
),

patient_definitions as (
    select
        patient_id,
        preferred_name,
        patient_status,
        first_visit_date
    from source_patients
),

-- 3. Calculation/Aggregation CTEs
appointment_timing_calculations as (
    select
        appointment_id,
        patient_id,
        provider_id,
        appointment_datetime,
        appointment_datetime + 
            interval '1 minute' * (
                {{ calculate_pattern_length('pattern') }}
            ) as appointment_end_datetime,
        appointment_type_id,
        appointment_status,
        confirmation_status as confirmed,
        operatory_id as operatory,
        pattern,
        {{ calculate_pattern_length('pattern') }} as pattern_length,
        note,
        is_hygiene,
        is_new_patient,
        arrival_datetime as check_in_time,
        dismissed_datetime as check_out_time,
        sec_date_t_entry as created_at,
        -- Preserve metadata from primary source
        _loaded_at,
        _created_at,
        _updated_at
    from source_appointments
),

-- 4. Business Logic CTEs (can be multiple)
appointment_actual_length_calculation as (
    select
        *,
        case
            when check_in_time is not null 
            and check_out_time is not null
            and check_out_time > check_in_time
            and check_out_time::time != '00:00:00'::time  -- exclude placeholder times
            and check_in_time::time != '00:00:00'::time    -- exclude placeholder times
            then (
                -- count business hours between check-in and check-out
                with recursive dates as (
                    select 
                        check_in_time as dt,
                        least(
                            check_out_time,
                            date(check_in_time) + interval '1 day' - interval '1 second'
                        ) as end_dt
                    union all
                    select 
                        dt + interval '1 day',
                        least(
                            check_out_time,
                            dt + interval '2 days' - interval '1 second'
                        )
                    from dates
                    where dt + interval '1 day' < check_out_time
                )
                select 
                    case 
                        when sum(
                            case 
                                -- skip sundays
                                when extract(dow from dt) = 0 then 0
                                -- business hours only (9:00-17:00)
                                else least(
                                    extract(epoch from (
                                        least(end_dt, dt + interval '17:00:00') - 
                                        greatest(dt, dt + interval '09:00:00')
                                    ))/60,
                                    480  -- 8 hours in minutes
                                )
                            end
                        ) > 0 then round(sum(
                            case 
                                -- skip sundays
                                when extract(dow from dt) = 0 then 0
                                -- business hours only (9:00-17:00)
                                else least(
                                    extract(epoch from (
                                        least(end_dt, dt + interval '17:00:00') - 
                                        greatest(dt, dt + interval '09:00:00')
                                    ))/60,
                                    480  -- 8 hours in minutes
                                )
                            end
                        ))::integer
                        else null
                    end
                from dates
            )
            else null
        end as actual_length
    from appointment_timing_calculations
),

appointment_status_standardization as (
    select
        *,
        case
            when appointment_status = 1 then 'Scheduled'
            when appointment_status = 2 then 'Complete'
            when appointment_status = 3 then 'UnschedList'
            when appointment_status = 4 then 'ASAP'
            when appointment_status = 5 then 'Broken'
            when appointment_status = 6 then 'Planned'
            when appointment_status = 7 then 'PtNote'
            when appointment_status = 8 then 'PtNoteCompleted'
            else 'Unknown'
        end as appointment_status_desc,
        case when appointment_status = 2 then true else false end as is_complete,
        case
            when check_in_time is not null 
            and appointment_datetime is not null
            and check_in_time::time != '00:00:00'::time  -- exclude placeholder times
            and check_in_time >= appointment_datetime
            then round(extract(epoch from (check_in_time - appointment_datetime))/60)::integer
            else 0  -- default to 0 for waiting time when no valid check-in time
        end as waiting_time
    from appointment_actual_length_calculation
),

historical_appointment_processing as (
    select
        patient_id,
        appointment_id,
        appointment_status,
        history_action as action_type,
        case
            when history_action = 1 then 'Rescheduled'
            when history_action = 2 then 'Confirmed'
            when history_action = 3 then 'Failed Appointment'
            when history_action = 4 then 'Cancelled'
            when history_action = 5 then 'ASAP'
            when history_action = 6 then 'Complete'
            else 'Unknown'
        end as action_description,
        note as action_note,
        case
            when history_action = 4 and note is not null
            then note
            else null
        end as cancellation_reason,
        case
            when history_action = 1 and note like '%new appt:#%'
            then regexp_replace(
                substring(note from 'new appt:#([0-9]+)'),
                '[^0-9]', '', 'g'
            )::integer
            else null
        end as rescheduled_appointment_id,
        row_number() over (
            partition by appointment_id 
            order by history_timestamp desc
        ) as history_rank
    from source_historical_appointments
),

-- 5. Integration CTE (joins everything together)
appointment_integrated as (
    select
        -- core appointment fields
        a.appointment_id,
        a.patient_id,
        a.provider_id,
        a.appointment_datetime,
        a.appointment_end_datetime,
        a.appointment_type_id,
        a.appointment_status,
        a.appointment_status_desc,
        a.confirmed as is_confirmed,
        a.is_complete,
        a.is_hygiene,
        a.is_new_patient,
        a.note,
        a.operatory,
        a.check_in_time,
        a.check_out_time,
        a.actual_length,
        a.waiting_time,
        
        -- appointment type information
        atd.appointment_type_name,
        atd.appointment_length,
        
        -- provider information
        pd.provider_abbr as provider_name,
        pd.specialty as provider_specialty,
        pd.provider_color,
        
        -- patient information
        patd.preferred_name as patient_name,
        patd.patient_status,
        patd.first_visit_date,
        
        -- historical appointment information
        ha.cancellation_reason,
        ha.rescheduled_appointment_id,
        
        -- metadata
        a.created_at,
        current_timestamp as updated_at,
        
        -- Standardized metadata using macro (preserves primary source metadata)
        {{ standardize_intermediate_metadata(
            primary_source_alias='a',
            preserve_source_metadata=true,
            source_metadata_fields=['_loaded_at', '_created_at', '_updated_at']
        ) }}
    from appointment_status_standardization a
    left join appointment_type_definitions atd
        on a.appointment_type_id = atd.appointment_type_id
    left join provider_definitions pd
        on a.provider_id = pd.provider_id
    left join patient_definitions patd
        on a.patient_id = patd.patient_id
    left join historical_appointment_processing ha
        on a.appointment_id = ha.appointment_id
        and ha.history_rank = 1  -- only get the latest history record
        and ha.action_type in (1, 4) -- rescheduled or cancelled
),

-- 6. Final filtering/validation
final as (
    select * from appointment_integrated
    where appointment_id is not null
)

select * from final
