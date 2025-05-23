{{ config(
    materialized = 'incremental',
    unique_key = 'schedule_id'
) }}

with source as (
    select * from {{ source('opendental', 'schedule') }}
    {% if is_incremental() %}
        where "DateTStamp" > (select max(_updated_at) from {{ this }})
    {% endif %}
),

renamed as (
    select
        -- Primary key
        "ScheduleNum" as schedule_id,
        
        -- Schedule attributes
        "SchedDate" as schedule_date,
        "StartTime" as start_time,
        "StopTime" as stop_time,
        "SchedType" as schedule_type,
        "ProvNum" as provider_id,
        "BlockoutType" as blockout_type_id,
        "Note" as note,
        "Status" as status,
        "EmployeeNum" as employee_id,
        "ClinicNum" as clinic_id,
        
        -- Metadata
        current_timestamp as _loaded_at,  -- When ETL pipeline loaded the data
        "DateTStamp" as _created_at,     -- When the record was created in source
        "DateTStamp" as _updated_at      -- Last update timestamp
    from source
)

select * from renamed
where schedule_date >= '2023-01-01'
