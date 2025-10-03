{{ config(
    materialized='incremental',
    unique_key='procgroup_item_id'
) }}

with source_data as (
    select * from {{ source('opendental', 'procgroupitem') }}
    {% if is_incremental() %}
        where current_timestamp > (select max(_loaded_at) from {{ this }})
    {% endif %}
),

renamed_columns as (
    select
        -- ID Columns (with safe conversion)
        {{ transform_id_columns([
            {'source': '"ProcGroupItemNum"', 'target': 'procgroup_item_id'},
            {'source': '"ProcNum"', 'target': 'procedure_id'},
            {'source': '"GroupNum"', 'target': 'group_id'}
        ]) }},
        
        -- Standardized metadata columns
        {{ standardize_metadata_columns() }}
    from source_data
)

select * from renamed_columns
