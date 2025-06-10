{{ config(
    materialized='incremental',
    unique_key='hist_appointment_id',
    schema='staging'
) }}

with source_data as (
    select * 
    from {{ source('opendental', 'histappointment') }}
    where "HistDateTStamp" >= '2023-01-01'::timestamp
        AND "HistDateTStamp" <= CURRENT_TIMESTAMP
    {% if is_incremental() %}
        AND "HistDateTStamp" > (SELECT max(_created_at) FROM {{ this }})
    {% endif %}
),

renamed_columns as (
    select
        -- Primary and Foreign Key transformations using macro
        {{ transform_id_columns([
            {'source': '"HistApptNum"', 'target': 'hist_appointment_id'},
            {'source': '"AptNum"', 'target': 'appointment_id'},
            {'source': '"PatNum"', 'target': 'patient_id'},
            {'source': 'NULLIF("ProvNum", 0)', 'target': 'provider_id'},
            {'source': 'NULLIF("ProvHyg", 0)', 'target': 'hygienist_id'},
            {'source': 'NULLIF("Assistant", 0)', 'target': 'assistant_id'},
            {'source': 'NULLIF("ClinicNum", 0)', 'target': 'clinic_id'},
            {'source': 'NULLIF("NextAptNum", 0)', 'target': 'next_appointment_id'},
            {'source': 'NULLIF("AppointmentTypeNum", 0)', 'target': 'appointment_type_id'},
            {'source': 'NULLIF("Op", 0)', 'target': 'operatory_id'},
            {'source': '"HistUserNum"', 'target': 'history_user_id'},
            {'source': '"SecUserNumEntry"', 'target': 'entry_user_id'},
            {'source': 'NULLIF("InsPlan1", 0)', 'target': 'insurance_plan_1_id'},
            {'source': 'NULLIF("InsPlan2", 0)', 'target': 'insurance_plan_2_id'},
            {'source': 'NULLIF("UnschedStatus", 0)', 'target': 'unscheduled_status_id'},
            {'source': 'NULLIF("Confirmed", 0)', 'target': 'confirmation_id'}
        ]) }},
        
        -- Timestamp fields
        "HistDateTStamp"::timestamp with time zone as history_timestamp,
        "AptDateTime"::timestamp as appointment_datetime,
        "DateTimeArrived"::timestamp as arrived_datetime,
        "DateTimeSeated"::timestamp as seated_datetime,
        "DateTimeDismissed"::timestamp as dismissed_datetime,
        "DateTimeAskedToArrive"::timestamp as asked_to_arrive_datetime,
        
        -- Status and classification fields
        "AptStatus"::smallint as appointment_status,
        "HistApptAction"::smallint as history_action,
        "ApptSource"::smallint as appointment_source,
        "Priority"::smallint as priority,
        
        -- Boolean fields using macro
        {{ convert_opendental_boolean('"TimeLocked"') }} as is_time_locked,
        {{ convert_opendental_boolean('"IsNewPatient"') }} as is_new_patient,
        {{ convert_opendental_boolean('"IsHygiene"') }} as is_hygiene,
        
        -- Text and description fields
        nullif(trim("Pattern"), '') as pattern,
        nullif(trim("PatternSecondary"), '') as pattern_secondary,
        nullif(trim("Note"), '') as note,
        nullif(trim("ProcDescript"), '') as procedure_description,
        nullif(trim("ProcsColored"), '') as procedures_colored,
        nullif(trim("ProvBarText"), '') as provider_bar_text,
        nullif(trim("SecurityHash"), '') as security_hash,
        
        -- Other attributes
        "ColorOverride"::integer as color_override,
        "ItemOrderPlanned"::integer as item_order_planned,
        
        -- Standardized metadata using macro
        {{ standardize_metadata_columns(
            created_at_column='"HistDateTStamp"',
            updated_at_column='"DateTStamp"',
            created_by_column='"SecUserNumEntry"'
        ) }}

    from source_data
)

select * from renamed_columns
