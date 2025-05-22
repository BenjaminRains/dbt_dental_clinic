with source as (
    select * from {{ source('opendental', 'employee') }}
),

entry_logs as (
    select 
        "FKey" as employee_id,
        min("EntryDateTime") as first_entry_datetime
    from {{ source('opendental', 'entrylog') }}
    where "FKeyType" = 0  -- Assuming 0 is the type for employee records
    group by "FKey"
),

renamed as (
    select
        -- Primary Key
        "EmployeeNum" as employee_id,
        
        -- Employee information
        "LName" as last_name,
        "FName" as first_name,
        "MiddleI" as middle_initial,
        CASE 
            WHEN "IsHidden" = 1 THEN true
            WHEN "IsHidden" = 0 THEN false
            ELSE null 
        END as is_hidden,
        "ClockStatus" as clock_status,
        "PhoneExt" as phone_extension,
        "PayrollID" as payroll_id,
        "WirelessPhone" as wireless_phone,
        "EmailWork" as work_email,
        "EmailPersonal" as personal_email,
        CASE 
            WHEN "IsFurloughed" = 1 THEN true
            WHEN "IsFurloughed" = 0 THEN false
            ELSE null 
        END as is_furloughed,
        CASE 
            WHEN "IsWorkingHome" = 1 THEN true
            WHEN "IsWorkingHome" = 0 THEN false
            ELSE null 
        END as is_working_home,
        "ReportsTo" as reports_to_employee_id,

        -- Required metadata columns
        current_timestamp as _loaded_at,
        el.first_entry_datetime as _created_at,
        current_timestamp as _updated_at

    from source s
    left join entry_logs el
        on s."EmployeeNum" = el.employee_id
)

select * from renamed
