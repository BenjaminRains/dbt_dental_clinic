{{
    config(
        materialized='view'
    )
}}

with source as (
    select * from {{ source('opendental', 'definition') }}
),

renamed as (
    select
        -- Primary Key
        "DefNum" as definition_id,
        
        -- Attributes
        "Category" as category_id,
        "ItemOrder" as item_order,
        "ItemName" as item_name,
        "ItemValue" as item_value,
        "ItemColor" as item_color,
        "IsHidden" as is_hidden,

        -- Metadata
        current_timestamp as _loaded_at

    from source
)

select * from renamed
