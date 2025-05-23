{{ config(
    materialized='incremental',
    unique_key='procgroup_item_id'
) }}

with source as (
    select * from {{ source('opendental', 'procgroupitem') }}
    {% if is_incremental() %}
        where current_timestamp > (select max(_loaded_at) from {{ this }})
    {% endif %}
),

renamed as (
    select
        -- Primary key
        "ProcGroupItemNum" as procgroup_item_id,
        
        -- Foreign keys
        "ProcNum" as procedure_id,
        "GroupNum" as group_id,
        
        -- Required metadata columns
        current_timestamp as _loaded_at,                    -- When ETL pipeline loaded the data
        current_timestamp as _created_at,                   -- When record was created in source system
        current_timestamp as _updated_at,                   -- When record was last updated
        
        -- Optional metadata
        '{{ invocation_id }}' as _invocation_id,           -- dbt invocation ID for lineage tracking
        current_timestamp as _extract_timestamp            -- When data was extracted from source
    from source
)

select * from renamed
