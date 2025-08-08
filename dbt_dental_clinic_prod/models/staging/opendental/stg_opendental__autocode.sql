{{ config(
    materialized='view'
) }}

with source_data as (
    select * from {{ source('opendental', 'autocode') }}
),

renamed_columns as (
    select
        -- Primary Key
        {{ transform_id_columns([
            {'source': '"AutoCodeNum"', 'target': 'autocode_id'}
        ]) }},
        
        -- Attributes
        "Description" as description,
        {{ convert_opendental_boolean('"IsHidden"') }} as is_hidden,
        {{ convert_opendental_boolean('"LessIntrusive"') }} as is_less_intrusive,
        
        -- Metadata columns
        {{ standardize_metadata_columns() }}
    
    from source_data
)

select * from renamed_columns
