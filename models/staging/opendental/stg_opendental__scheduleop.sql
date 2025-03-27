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

renamed as (
    select
        -- Primary key
        "ScheduleOpNum" as schedule_op_id,
        -- Foreign keys
        "ScheduleNum" as schedule_id,
        "OperatoryNum" as operatory_id
    from source
)

select * from renamed
