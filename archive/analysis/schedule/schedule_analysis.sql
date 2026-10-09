with source as (
    select * from {{ source('opendental', 'schedule') }}
),

renamed as (
    select
        -- Primary key
        ScheduleNum as schedule_id,

        -- Relationships
        ProvNum as provider_id,
        EmployeeNum as employee_id,
        ClinicNum as clinic_id,

        -- Schedule configuration
        SchedType as schedule_type,
        BlockoutType as blockout_type_id,
        Status as schedule_status,

        -- Timing
        SchedDate as schedule_date,
        StartTime as start_time,
        StopTime as stop_time,

        -- Additional information
        Note as schedule_notes,

        -- Metadata
        DateTStamp as updated_at
    from source
)

select * from renamed