{{ config(
    materialized='incremental',
    unique_key='recall_type_id',
    schema='staging'
) }}

with Source as (
    select * from {{ source('opendental', 'recalltype') }}
    {% if is_incremental() %}
        where current_timestamp > (select max(_updated_at) from {{ this }})
    {% endif %}
),

Renamed as (
    select
        -- Primary key
        "RecallTypeNum" as recall_type_id,
        
        -- Description and configuration
        "Description" as description,
        "DefaultInterval" as default_interval,
        "TimePattern" as time_pattern,
        "Procedures" as procedures,
        "AppendToSpecial" as append_to_special,
        
        -- Required metadata columns
        current_timestamp as _loaded_at,
        current_timestamp as _created_at,
        current_timestamp as _updated_at
    
    from Source
)

select * from Renamed
