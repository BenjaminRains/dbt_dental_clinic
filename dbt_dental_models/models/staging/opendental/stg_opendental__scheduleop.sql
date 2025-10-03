-- depends_on: {{ ref('stg_opendental__schedule') }}
{{ config(
    materialized='incremental',
    unique_key='schedule_op_id'
) }}

with source_data as (
    select * from {{ source('opendental', 'scheduleop') }}
    {% if is_incremental() %}
        -- No DateTStamp in this table, so we'll join to schedule to get incremental data
        where "ScheduleNum" in (
            select "ScheduleNum" 
            from {{ source('opendental', 'schedule') }}
            where {{ clean_opendental_date('"DateTStamp"') }} > (select max(_loaded_at) from {{ ref('stg_opendental__schedule') }})
        )
    {% endif %}
),

schedule_data as (
    select * from {{ source('opendental', 'schedule') }}
),

renamed_columns as (
    select
        -- Primary key
        {{ transform_id_columns([
            {'source': 'sop."ScheduleOpNum"', 'target': 'schedule_op_id'}
        ]) }},
        
        -- Foreign keys
        {{ transform_id_columns([
            {'source': 'sop."ScheduleNum"', 'target': 'schedule_id'},
            {'source': 'sop."OperatoryNum"', 'target': 'operatory_id'}
        ]) }},
        
        -- Source Metadata
        {{ clean_opendental_date('"DateTStamp"') }} as date_tstamp,

        -- Metadata columns (using joined schedule table for timestamps)
        {{ standardize_metadata_columns(
            created_at_column='s."DateTStamp"',
            updated_at_column='s."DateTStamp"',
            created_by_column=none
        ) }}
        
    from source_data sop
    left join schedule_data s
        on sop."ScheduleNum" = s."ScheduleNum"
)

select * from renamed_columns
