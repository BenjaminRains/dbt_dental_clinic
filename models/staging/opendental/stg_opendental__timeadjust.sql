with source as (
    select * from {{ source('opendental', 'timeadjust') }}
),

renamed as (
    select
        -- Primary key
        "TimeAdjustNum" as time_adjust_id,
        
        -- Foreign keys
        "EmployeeNum" as employee_id,
        "ClinicNum" as clinic_id,
        "PtoDefNum" as pto_def_id,
        "SecuUserNumEntry" as secu_user_entry_id,
        
        -- Timestamps
        "TimeEntry" as time_entry_ts,
        
        -- Time durations
        "RegHours" as regular_hours,
        "OTimeHours" as overtime_hours,
        "PtoHours" as pto_hours,
        
        -- Flags and attributes
        "IsAuto" as is_auto,
        "IsUnpaidProtectedLeave" as is_unpaid_protected_leave,
        "Note" as note,
        
        -- Metadata
        {{ current_timestamp() }} as loaded_at
    from source
)

select * from renamed
where time_entry_ts >= '2023-01-01'
