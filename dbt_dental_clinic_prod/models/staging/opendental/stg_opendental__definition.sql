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
        "ItemName" as item_name,
        "ItemValue" as item_value,
        "ItemColor" as item_color,
        
        -- Boolean Fields
        {{ convert_opendental_boolean('"IsHidden"') }} as is_hidden,

        -- Metadata columns
        {{ standardize_metadata_columns(
            created_at_column=none,
            updated_at_column=none,
            created_by_column=none
        ) }}

    from source_data
)

select * from renamed_columns
