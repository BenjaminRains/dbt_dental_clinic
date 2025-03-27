with source as (
    select * from {{ source('opendental', 'employee') }}
),

renamed as (
    select
        -- Primary Key
        "EmployeeNum" as employee_id,
        
        -- Employee information
        "LName" as last_name,
        "FName" as first_name,
        "MiddleI" as middle_initial,
        "IsHidden"::boolean as is_hidden,
        "ClockStatus" as clock_status,
        "PhoneExt" as phone_extension,
        "PayrollID" as payroll_id,
        "WirelessPhone" as wireless_phone,
        "EmailWork" as work_email,
        "EmailPersonal" as personal_email,
        "IsFurloughed"::boolean as is_furloughed,
        "IsWorkingHome"::boolean as is_working_home,
        "ReportsTo" as reports_to_employee_id
    from source
)

select * from renamed
