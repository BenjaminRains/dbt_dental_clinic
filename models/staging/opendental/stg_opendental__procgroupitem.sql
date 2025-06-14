{{ config(
    materialized='incremental',
    unique_key='procgroup_item_id',
    schema='staging'
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
        {{ standardize_metadata_columns(
            created_at_column=none,
            updated_at_column=none,
            created_by_column=none
        ) }}
    from source_data
)

select * from renamed_columns
