{{ config(
    materialized='view'
) }}

with source_data as (
    select * from {{ source('opendental', 'employee') }}
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

        -- ETL and dbt tracking columns
        {{ standardize_metadata_columns() }}

    from source_data
)

select * from renamed_columns
