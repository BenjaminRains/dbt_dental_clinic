{{ config(
    materialized='incremental',
    unique_key='recall_trigger_id'
) }}

with source_data as (
    select * from {{ source('opendental', 'recalltrigger') }}
    {% if is_incremental() %}
        where current_timestamp > (select max(_updated_at) from {{ this }})
    {% endif %}
),

renamed_columns as (
    select
        -- Primary Key
        {{ transform_id_columns([
            {'source': '"RecallTriggerNum"', 'target': 'recall_trigger_id'}
        ]) }},
        
        -- Foreign Keys  
        {{ transform_id_columns([
            {'source': '"RecallTypeNum"', 'target': 'recall_type_id'},
            {'source': '"CodeNum"', 'target': 'code_id'}
        ]) }},
        
        -- Metadata
        {{ standardize_metadata_columns(
            created_at_column=none,
            updated_at_column=none,
            created_by_column=none
        ) }}

    from source_data
)

select * from renamed_columns
