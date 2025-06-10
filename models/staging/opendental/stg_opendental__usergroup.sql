{{ config(
    materialized='view',
    schema='staging'
) }}

with source_data as (
    select * from {{ source('opendental', 'usergroup') }}
),

renamed_columns as (
    select
        -- Primary and Foreign Key transformations using macro
        {{ transform_id_columns([
            {'source': '"UserGroupNum"', 'target': 'usergroup_id'},
            {'source': 'NULLIF("UserGroupNumCEMT", 0)', 'target': 'usergroup_num_cemt'}
        ]) }},
        
        -- Attributes
        {{ clean_opendental_string('"Description"') }} as description,
        
        -- Standardized metadata using macro
        {{ standardize_metadata_columns(
            created_at_column=none,
            updated_at_column=none,
            created_by_column=none
        ) }}
        
    from source_data
)

select * from renamed_columns
