with source as (
    select * from {{ source('opendental', 'scheduleop') }}
),

renamed as (
    select
        -- Primary key
        ScheduleOpNum as schedule_operatory_id,

        -- Relationships
        ScheduleNum as schedule_id,
        OperatoryNum as operatory_id
    from source
)

select * from renamed