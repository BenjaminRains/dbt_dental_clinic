{{ config(
    materialized='incremental',
    unique_key='appointment_id',
    schema='staging'
) }}

with source as (
    
    select * from {{ source('opendental', 'appointment') }}
    where "AptDateTime" >= '2023-01-01'::timestamp  -- Only include appointments from 2023 onwards
        AND "AptDateTime" <= CURRENT_TIMESTAMP
    {% if is_incremental() %}
        AND "DateTStamp" > (SELECT max(date_timestamp) FROM {{ this }})
    {% endif %}

),

renamed as (

    select
        -- Primary Key
        "AptNum"::integer as appointment_id,
        
        -- Foreign Keys
        "PatNum"::bigint as patient_id,
        "ProvNum"::bigint as provider_id,
        "ProvHyg"::bigint as hygienist_id,
        "Assistant"::bigint as assistant_id,
        "ClinicNum"::bigint as clinic_id,
        "Op"::bigint as operatory_id,
        "NextAptNum"::bigint as next_appointment_id,
        "AppointmentTypeNum"::bigint as appointment_type_id,
        "InsPlan1"::bigint as insurance_plan_1_id,
        "InsPlan2"::bigint as insurance_plan_2_id,
        "UnschedStatus"::bigint as unscheduled_status_id,
        "SecUserNumEntry"::bigint as entered_by_user_id,
        
        -- Timestamps
        "AptDateTime"::timestamp as appointment_datetime,
        "DateTStamp"::timestamp as date_timestamp,
        "DateTimeArrived"::timestamp as arrival_datetime,
        "DateTimeSeated"::timestamp as seated_datetime, 
        "DateTimeDismissed"::timestamp as dismissed_datetime,
        "DateTimeAskedToArrive"::timestamp as asked_to_arrive_datetime,
        "SecDateTEntry"::timestamp as entry_datetime,
        
        -- Status Fields
        "AptStatus"::smallint as appointment_status,
        "Confirmed"::bigint as confirmation_status,
        "IsNewPatient"::smallint as is_new_patient,
        "IsHygiene"::smallint as is_hygiene,
        "Priority"::smallint as priority,
        
        -- Boolean Fields
        "TimeLocked"::boolean as is_time_locked,
        
        -- Text Fields
        "Pattern"::varchar as pattern,
        "PatternSecondary"::varchar as pattern_secondary,
        "Note"::text as note,
        "ProcDescript"::text as procedure_description,
        "ProcsColored"::text as procedures_colored,
        "ProvBarText"::varchar as provider_bar_text,
        
        -- Additional Fields
        "ColorOverride"::integer as color_override,
        "ItemOrderPlanned"::integer as item_order_planned,
        "SecurityHash"::varchar as security_hash,
        
        -- Metadata
        current_timestamp as _loaded_at

    from source

)

select * from renamed
