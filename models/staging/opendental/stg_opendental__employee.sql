{{
    config(
        materialized='view',
        schema='staging'
    )
}}

with source_data as (
    select * from {{ source('opendental', 'employee') }}
),

employee_entry_logs as (
    select 
        "FKey" as employee_id,
        min("EntryDateTime") as first_entry_datetime
    from {{ source('opendental', 'entrylog') }}
    where "FKeyType" = 0  -- Assuming 0 is the type for employee records
    group by "FKey"
),

renamed_columns as (
    select
        -- Primary Key
        "EmployeeNum" as employee_id,
        
        -- Employee information
        "LName" as last_name,
        "FName" as first_name,
        "MiddleI" as middle_initial,
        "ClockStatus" as clock_status,
        "PhoneExt" as phone_extension,
        "PayrollID" as payroll_id,
        "WirelessPhone" as wireless_phone,
        "EmailWork" as work_email,
        "EmailPersonal" as personal_email,
        "ReportsTo" as reports_to_employee_id,

        -- Boolean fields
        {{ convert_opendental_boolean('"IsHidden"') }} as is_hidden,
        {{ convert_opendental_boolean('"IsFurloughed"') }} as is_furloughed,
        {{ convert_opendental_boolean('"IsWorkingHome"') }} as is_working_home,

        -- Metadata columns (custom implementation due to entrylog join)
        current_timestamp as _loaded_at,
        el.first_entry_datetime as _created_at,
        current_timestamp as _updated_at,
        null as _created_by_user_id

    from source_data s
    left join employee_entry_logs el
        on s."EmployeeNum" = el.employee_id
)

select * from renamed_columns
