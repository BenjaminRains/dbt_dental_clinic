with source as (
    select * from {{ source('opendental', 'definition') }}
),

renamed as (
    select
        -- Primary key
        DefNum as definition_id,
        
        -- Definition details
        Category as category_id,
        ItemName as item_name,
        ItemValue as item_value,
        
        -- Display settings
        ItemOrder as display_order,
        ItemColor as color_value,
        IsHidden as is_hidden
    from source
)

select * from renamed