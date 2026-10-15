with source as (
    select * from {{ source('opendental', 'histappointment') }}
),

renamed as (
    select
        -- Primary keys and history tracking
        HistApptNum as appointment_history_id,
        AptNum as appointment_id,
        HistUserNum as history_user_id,
        HistDateTStamp as history_timestamp,
        HistApptAction as history_action_type,
        ApptSource as appointment_source,

        -- Core appointment relationships
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
        DateTimeAskedToArrive as asked_to_arrive_datetime,
        DateTimeArrived as arrived_datetime,
        DateTimeSeated as seated_datetime,
        DateTimeDismissed as dismissed_datetime,

        -- Appointment details
        AptStatus as appointment_status,
        Pattern as time_pattern,
        PatternSecondary as secondary_time_pattern,
        LENGTH(Pattern) * 5 as duration_minutes,
        TimeLocked as is_time_locked,
        Priority as priority,

        -- Classification flags
        IsHygiene as is_hygiene_appointment,
        IsNewPatient as is_new_patient,

        -- Display settings
        ColorOverride as color_override,
        ProvBarText as provider_bar_text,
        ItemOrderPlanned as planned_order,

        -- Confirmation
        Confirmed as confirmation_status,

        -- Metadata
        DateTStamp as updated_at,
        SecUserNumEntry as created_by_user_id,
        SecDateTEntry as created_at,

        -- Excluded fields with potential PHI:
        Note, 
        ProcDescript, 
        ProcsColored, 
        SecurityHash
    from source
)

select * from renamed