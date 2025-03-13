with source as (
    select * from {{ source('opendental', 'timeadjust') }}
),

renamed as (
    select
        -- Primary key
        TimeAdjustNum as time_adjustment_id,

        -- Relationships
        EmployeeNum as employee_id,
        ClinicNum as clinic_id,
        PtoDefNum as pto_definition_id,
        SecuUserNumEntry as created_by_user_id,

        -- Hours
        RegHours as regular_hours,
        OTimeHours as overtime_hours,
        PtoHours as pto_hours,

        -- Status flags
        IsAuto as is_automatic_flag,
        IsUnpaidProtectedLeave as is_unpaid_protected_leave_flag,

        -- Timing
        TimeEntry as entry_datetime,

        -- Notes (may contain sensitive HR information)
        Note as adjustment_notes
    from source
)

select * from renamed