{{
    config(
        materialized='view'
    )
}}

/*
Appointment Status Codes:
2 = Most common status (~77% of appointments)
3, 5, 6, 1 = Less common statuses (~6% each)

Data Quality Notes:
- Provider bar text is consistently empty (100%)
- ~45% missing values in timing fields (chair_time, wait_time, arrival_difference)
- ~42% missing procedure descriptions and colored procedures
- Appointment durations typically between 50-100 minutes
- Valid appointment dates (none before 2000)
*/

with source as (
    select * from {{ source('opendental', 'appointment') }}
),

appointment_types as (
    select * from {{ ref('stg_appointmenttype') }}
),

renamed as (
    select
        -- Keys
        AptNum as appointment_id,
        PatNum as patient_id,
        ProvNum as provider_id,
        ProvHyg as hygienist_id,
        Assistant as assistant_id,
        ClinicNum as clinic_id,
        Op as operatory_id,
        AppointmentTypeNum as appointment_type_id,
        NextAptNum as next_appointment_id,
        UnschedStatus as unscheduled_status_id,
        InsPlan1 as primary_insurance_plan_id,
        InsPlan2 as secondary_insurance_plan_id,
        
        -- Appointment timing
        AptDateTime as appointment_datetime,
        DATE(AptDateTime) as appointment_date,
        TIME(AptDateTime) as appointment_time,
        DateTimeAskedToArrive as asked_to_arrive_datetime,
        DateTimeArrived as arrived_datetime,
        DateTimeSeated as seated_datetime,
        DateTimeDismissed as dismissed_datetime,
        
        -- Patient flow calculations with data quality checks
        CASE 
            WHEN DateTimeArrived > '0001-01-01 00:00:00' 
            AND DateTimeAskedToArrive > '0001-01-01 00:00:00'
            THEN TIMESTAMPDIFF(MINUTE, DateTimeAskedToArrive, DateTimeArrived) 
            ELSE NULL 
        END as minutes_arrival_difference,
        
        CASE 
            WHEN DateTimeSeated > '0001-01-01 00:00:00' 
            AND DateTimeArrived > '0001-01-01 00:00:00'
            AND DateTimeSeated >= DateTimeArrived  -- Added validation
            THEN TIMESTAMPDIFF(MINUTE, DateTimeArrived, DateTimeSeated) 
            ELSE NULL 
        END as wait_time_minutes,
        
        CASE 
            WHEN DateTimeDismissed > '0001-01-01 00:00:00' 
            AND DateTimeSeated > '0001-01-01 00:00:00'
            AND DateTimeDismissed >= DateTimeSeated  -- Added validation
            THEN TIMESTAMPDIFF(MINUTE, DateTimeSeated, DateTimeDismissed) 
            ELSE NULL 
        END as chair_time_minutes,
        
        -- Appointment details with type validation
        a.AptStatus as appointment_status,
        a.Pattern as time_pattern,
        a.PatternSecondary as secondary_time_pattern,
        CASE 
            WHEN at.pattern_status = 'Valid Pattern' THEN at.duration_minutes
            ELSE LENGTH(a.Pattern) * 5
        END as duration_minutes,
        
        -- Add appointment type context
        at.type_name as appointment_type_name,
        at.pattern_status as type_pattern_status,
        at.appointment_type_status as type_status,
        
        -- Flag problematic appointments
        CASE 
            WHEN at.is_hidden = 1 THEN TRUE
            ELSE FALSE
        END as is_hidden_type,
        
        CASE 
            WHEN at.pattern_status != 'Valid Pattern' THEN TRUE
            ELSE FALSE
        END as has_invalid_type_pattern,
        
        CASE
            WHEN LENGTH(a.Pattern) * 5 != at.duration_minutes 
            AND at.pattern_status = 'Valid Pattern' THEN TRUE
            ELSE FALSE
        END as has_duration_mismatch,
        
        -- Appointment details
        TimeLocked as is_time_locked,
        Priority as priority,
        
        -- Confirmation
        Confirmed as confirmation_status,
        
        -- Classification flags
        IsHygiene as is_hygiene_appointment,
        IsNewPatient as is_new_patient,
        
        -- Display settings (known to have high null rates)
        ColorOverride as color_override,
        ProcsColored as colored_procedures,
        ProvBarText as provider_bar_text,
        
        -- Procedure information
        ProcDescript as procedure_description,
        Note as appointment_note,
        
        -- Metadata
        DateTStamp as updated_at,
        SecUserNumEntry as created_by_user_id,
        SecDateTEntry as created_at,
        ItemOrderPlanned as planned_order,
        
        -- Added derived fields based on analysis
        CASE 
            WHEN LENGTH(a.Pattern) * 5 > 120 THEN 'Long'
            WHEN LENGTH(a.Pattern) * 5 > 60 THEN 'Standard'
            ELSE 'Short'
        END as appointment_length_category,
        
        CASE
            WHEN DateTimeArrived > '0001-01-01 00:00:00' 
            AND DateTimeArrived > AptDateTime THEN 'Late'
            WHEN DateTimeArrived > '0001-01-01 00:00:00' 
            AND DateTimeArrived < AptDateTime THEN 'Early'
            ELSE 'Unknown'
        END as arrival_status,
        
        -- Data quality flags
        CASE 
            WHEN DateTimeArrived > DateTimeSeated 
            OR DateTimeSeated > DateTimeDismissed 
            THEN TRUE 
            ELSE FALSE 
        END as has_invalid_timestamp_sequence
        
    from source a
    left join appointment_types at 
        on a.AppointmentTypeNum = at.appointment_type_id
)

select * from renamed