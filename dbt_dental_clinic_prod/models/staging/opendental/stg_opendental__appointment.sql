{{ config(
    materialized='incremental',
    unique_key='appointment_id' 
) }}

with source_data as (
    select * from {{ source('opendental', 'appointment') }}
    where "AptDateTime" >= '2023-01-01'::timestamp  -- Only include appointments from 2023 onwards
    {% if is_incremental() %}
        AND {{ clean_opendental_date('"DateTStamp"') }} > (SELECT max(_loaded_at) FROM {{ this }})
    {% endif %}
),

renamed_columns as (
    select
        -- Primary and Foreign Key transformations using macro
        {{ transform_id_columns([
            {'source': '"AptNum"', 'target': 'appointment_id'},
            {'source': '"PatNum"', 'target': 'patient_id'},
            {'source': '"ProvNum"', 'target': 'provider_id'},
            {'source': '"ProvHyg"', 'target': 'hygienist_id'},
            {'source': '"Assistant"', 'target': 'assistant_id'},
            {'source': '"ClinicNum"', 'target': 'clinic_id'},
            {'source': '"Op"', 'target': 'operatory_id'},
            {'source': '"NextAptNum"', 'target': 'next_appointment_id'},
            {'source': '"AppointmentTypeNum"', 'target': 'appointment_type_id'},
            {'source': '"InsPlan1"', 'target': 'insurance_plan_1_id'},
            {'source': '"InsPlan2"', 'target': 'insurance_plan_2_id'},
            {'source': '"UnschedStatus"', 'target': 'unscheduled_status_id'},
            {'source': '"SecUserNumEntry"', 'target': 'entered_by_user_id'}
        ]) }},
        
        -- Date fields using macro
        {{ clean_opendental_date('"AptDateTime"') }} as appointment_datetime,
        {{ clean_opendental_date('"DateTimeArrived"') }} as arrival_datetime,
        {{ clean_opendental_date('"DateTimeSeated"') }} as seated_datetime, 
        {{ clean_opendental_date('"DateTimeDismissed"') }} as dismissed_datetime,
        {{ clean_opendental_date('"DateTimeAskedToArrive"') }} as asked_to_arrive_datetime,
        
        -- Status and Enum Fields (keep as-is for business logic)
        "AptStatus"::smallint as appointment_status,
        "Confirmed"::bigint as confirmation_status,
        case when "Priority" then 1 else 0 end::smallint as priority,
        
        -- Boolean Fields using macro
        {{ convert_opendental_boolean('"IsNewPatient"') }} as is_new_patient,
        {{ convert_opendental_boolean('"IsHygiene"') }} as is_hygiene,
        {{ convert_opendental_boolean('"TimeLocked"') }} as is_time_locked,
        
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
        
        -- Raw metadata columns (preserved from source)
        {{ clean_opendental_date('"SecDateTEntry"') }} as sec_date_t_entry,
        {{ clean_opendental_date('"DateTStamp"') }} as date_t_stamp,
        
        -- Standardized metadata using macro
        {{ standardize_metadata_columns(
            created_at_column='"SecDateTEntry"',
            updated_at_column='"DateTStamp"',
            created_by_column='"SecUserNumEntry"') }}

    from source_data
)

select * from renamed_columns
