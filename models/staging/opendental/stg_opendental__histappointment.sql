{{ config(
    materialized='incremental',
    unique_key='hist_appointment_id',
    schema='staging'
) }}

with source as (
    select * 
    from {{ source('opendental', 'histappointment') }}
    where "HistDateTStamp" >= '2023-01-01'::timestamp
        AND "HistDateTStamp" <= CURRENT_TIMESTAMP
    {% if is_incremental() %}
        AND "HistDateTStamp" > (SELECT max(_created_at) FROM {{ this }})
    {% endif %}
),

renamed as (
    select
        -- Primary key
        "HistApptNum"::integer as hist_appointment_id,
        
        -- Foreign keys
        "AptNum"::integer as appointment_id,
        "PatNum"::bigint as patient_id,
        "ProvNum"::bigint as provider_id,
        "ProvHyg"::bigint as hygienist_id,
        "Assistant"::bigint as assistant_id,
        "ClinicNum"::bigint as clinic_id,
        "NextAptNum"::bigint as next_appointment_id,
        "AppointmentTypeNum"::bigint as appointment_type_id,
        "Op"::bigint as operatory_id,
        "HistUserNum"::bigint as history_user_id,
        "SecUserNumEntry"::bigint as entry_user_id,
        "InsPlan1"::bigint as insurance_plan_1_id,
        "InsPlan2"::bigint as insurance_plan_2_id,
        "UnschedStatus"::bigint as unscheduled_status_id,
        "Confirmed"::bigint as confirmation_id,
        
        -- Timestamps
        "HistDateTStamp"::timestamp with time zone as history_timestamp,
        "DateTStamp"::timestamp as created_timestamp,
        "AptDateTime"::timestamp as appointment_datetime,
        "DateTimeArrived"::timestamp as arrived_datetime,
        "DateTimeSeated"::timestamp as seated_datetime,
        "DateTimeDismissed"::timestamp as dismissed_datetime,
        "DateTimeAskedToArrive"::timestamp as asked_to_arrive_datetime,
        "SecDateTEntry"::timestamp as entry_datetime,
        
        -- Flags and status indicators
        "AptStatus"::smallint as appointment_status,
        "HistApptAction"::smallint as history_action,
        "ApptSource"::smallint as appointment_source,
        CASE WHEN COALESCE("TimeLocked", 0) = 1 THEN TRUE ELSE FALSE END as is_time_locked,
        "IsNewPatient"::smallint as is_new_patient,
        "IsHygiene"::smallint as is_hygiene,
        "Priority"::smallint as priority,
        
        -- Text fields and descriptors
        NULLIF(TRIM("Pattern"), '') as pattern,
        NULLIF(TRIM("PatternSecondary"), '') as pattern_secondary,
        NULLIF(TRIM("Note"), '') as note,
        NULLIF(TRIM("ProcDescript"), '') as procedure_description,
        NULLIF(TRIM("ProcsColored"), '') as procedures_colored,
        NULLIF(TRIM("ProvBarText"), '') as provider_bar_text,
        
        -- Other attributes
        "ColorOverride"::integer as color_override,
        NULLIF(TRIM("SecurityHash"), '') as security_hash,
        "ItemOrderPlanned"::integer as item_order_planned,
        
        -- Required metadata columns
        current_timestamp as _loaded_at,
        "HistDateTStamp"::timestamp as _created_at,
        coalesce("DateTStamp", "HistDateTStamp")::timestamp as _updated_at

    from source
)

select * from renamed
