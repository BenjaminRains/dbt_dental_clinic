{{ config(
    materialized='incremental',
    unique_key='schedule_id',
    schema='staging'
) }}

with source_data as (
    select * from {{ source('opendental', 'schedule') }}
    where {{ clean_opendental_date('"SchedDate"') }} >= '2023-01-01'
    {% if is_incremental() %}
        and "DateTStamp" > (select max(_updated_at) from {{ this }})
    {% endif %}
),

renamed_columns as (
    select
        -- Primary key
        {{ transform_id_columns([
            {'source': '"ScheduleNum"', 'target': 'schedule_id'}
        ]) }},
        
        -- Foreign Keys
        {{ transform_id_columns([
            {'source': '"ProvNum"', 'target': 'provider_id'},
            {'source': '"BlockoutType"', 'target': 'blockout_type_id'},
            {'source': '"EmployeeNum"', 'target': 'employee_id'},
            {'source': '"ClinicNum"', 'target': 'clinic_id'}
        ]) }},
        
        -- Schedule attributes
        {{ clean_opendental_date('"SchedDate"') }} as schedule_date,
        "StartTime" as start_time,
        "StopTime" as stop_time,
        "SchedType" as schedule_type,
        "Note" as note,
        "Status" as status,
        
        -- Metadata columns
        {{ standardize_metadata_columns(
            created_at_column='"DateTStamp"',
            updated_at_column='"DateTStamp"',
            created_by_column=none
        ) }}
        
    from source_data
)

select * from renamed_columns
