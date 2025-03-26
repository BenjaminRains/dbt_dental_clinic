{{ config(
    materialized='incremental',
    unique_key='procgroup_item_id'
) }}

with source as (
    select * from {{ source('opendental', 'procgroupitem') }}
    {% if is_incremental() %}
        where "DateTStamp" > (select max(date_timestamp) from {{ this }})
    {% endif %}
),

renamed as (
    select
        -- Primary key
        "ProcGroupItemNum" as procgroup_item_id,
        
        -- Foreign keys
        "ProcNum" as procedure_id,
        "GroupNum" as group_id,
        
        -- Metadata
        '{{ invocation_id }}' as _airbyte_ab_id,
        current_timestamp as _airbyte_loaded_at
    from source
)

select * from renamed
