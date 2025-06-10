{{ config(
    materialized='view',
    schema='staging'
) }}

with source_data as (
    select * from {{ source('opendental', 'usergroupattach') }}
),

renamed_columns as (
    select
        -- Primary and Foreign Key transformations using macro
        {{ transform_id_columns([
            {'source': '"UserGroupAttachNum"', 'target': 'user_group_attach_id'},
            {'source': 'NULLIF("UserNum", 0)', 'target': 'user_id'},
            {'source': 'NULLIF("UserGroupNum", 0)', 'target': 'user_group_id'}
        ]) }},
        
        -- Standardized metadata using macro
        {{ standardize_metadata_columns(
            created_at_column=none,
            updated_at_column=none,
            created_by_column=none
        ) }}

    from source_data
)

select * from renamed_columns
