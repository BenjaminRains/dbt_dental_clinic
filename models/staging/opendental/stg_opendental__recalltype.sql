{{ config(
    materialized='incremental',
    unique_key='recall_type_id',
    schema='staging'
) }}

with source_data as (
    select * from {{ source('opendental', 'recalltype') }}
    {% if is_incremental() %}
        where current_timestamp > (select max(_updated_at) from {{ this }})
    {% endif %}
),

renamed_columns as (
    select
        -- Primary and Foreign Key transformations using macro
        {{ transform_id_columns([
            {'source': '"RecallTypeNum"', 'target': 'recall_type_id'}
        ]) }},
        
        -- Description and configuration
        {{ clean_opendental_string('"Description"') }} as description,
        "DefaultInterval"::integer as default_interval,
        {{ clean_opendental_string('"TimePattern"') }} as time_pattern,
        {{ clean_opendental_string('"Procedures"') }} as procedures,
        
        -- Boolean fields using macro
        {{ convert_opendental_boolean('"AppendToSpecial"') }} as append_to_special,
        
        -- Standardized metadata using macro
        {{ standardize_metadata_columns(
            created_at_column=none,
            updated_at_column=none,
            created_by_column=none
        ) }}
    
    from source_data
)

select * from renamed_columns
