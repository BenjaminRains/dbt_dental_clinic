{{ config(
    materialized='incremental',
    unique_key='recall_trigger_id',
    schema='staging'
) }}

with Source as (
    select * from {{ source('opendental', 'recalltrigger') }}
    {% if is_incremental() %}
        where current_timestamp > (select max(_updated_at) from {{ this }})
    {% endif %}
),

Renamed as (
    select
        -- Primary key
        "RecallTriggerNum" as recall_trigger_id,
        
        -- Relationships
        "RecallTypeNum" as recall_type_id,
        "CodeNum" as code_id,
        
        -- Required metadata columns
        current_timestamp as _loaded_at,
        current_timestamp as _created_at,
        current_timestamp as _updated_at

    from Source
)

select * from Renamed
