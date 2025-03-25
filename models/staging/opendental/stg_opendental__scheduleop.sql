-- depends_on: {{ ref('stg_opendental__schedule') }}
{{ config(
    materialized = 'incremental',
    unique_key = 'schedule_op_id'
) }}

with source as (
    select * from {{ source('opendental', 'scheduleop') }}
    {% if is_incremental() %}
        -- No DateTStamp in this table, so we'll join to schedule to get incremental data
        where "ScheduleNum" in (
            select "ScheduleNum" 
            from {{ source('opendental', 'schedule') }}
            where "DateTStamp" > (select max(created_at) from {{ ref('stg_opendental__schedule') }})
        )
    {% endif %}
),

schedule_dates as (
    select
        "ScheduleNum",
        "SchedDate" as schedule_date
    from {{ source('opendental', 'schedule') }}
),

renamed as (
    select
        -- Primary key
        s."ScheduleOpNum" as schedule_op_id,
        
        -- Foreign keys
        s."ScheduleNum" as schedule_id,
        s."OperatoryNum" as operatory_id,
        
        -- Join to get the date
        sd.schedule_date
    from source s
    left join schedule_dates sd on s."ScheduleNum" = sd."ScheduleNum"
)

select
    schedule_op_id,
    schedule_id,
    operatory_id
from renamed
where schedule_date >= '2023-01-01'
