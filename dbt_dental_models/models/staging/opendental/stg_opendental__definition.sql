{{ config(
    materialized='view'
) }}

with source_data as (
    select * from {{ source('opendental', 'definition') }}
),

renamed_columns as (
    select
        -- Primary Key and Foreign Keys
        {{ transform_id_columns([
            {'source': '"DefNum"', 'target': 'definition_id'},
            {'source': '"Category"', 'target': 'category_id'}
        ]) }},
        
        -- Attributes
        "ItemOrder" as item_order,
        {{ clean_opendental_string('"ItemName"') }} as item_name,
        {{ clean_opendental_string('"ItemValue"') }} as item_value,
        "ItemColor" as item_color,
        
        -- Boolean Fields
        {{ convert_opendental_boolean('"IsHidden"') }} as is_hidden,

        -- Metadata columns
        {{ standardize_metadata_columns() }}

    from source_data
)

select * from renamed_columns
