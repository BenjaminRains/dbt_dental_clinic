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
            where "DateTStamp" > (select max(_updated_at) from {{ ref('stg_opendental__schedule') }})
        )
    {% endif %}
),

renamed as (
    select
        -- Primary key
        sop."ScheduleOpNum" as schedule_op_id,
        
        -- Foreign keys
        sop."ScheduleNum" as schedule_id,
        sop."OperatoryNum" as operatory_id,
        
        -- Metadata
        current_timestamp as _loaded_at,  -- When ETL pipeline loaded the data
        s."DateTStamp" as _created_at,   -- When the record was created in source
        s."DateTStamp" as _updated_at    -- Last update timestamp
    from source sop
    left join {{ source('opendental', 'schedule') }} s
        on sop."ScheduleNum" = s."ScheduleNum"
)

select * from renamed
