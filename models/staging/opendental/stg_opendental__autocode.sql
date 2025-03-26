{{
    config(
        materialized='view'
    )
}}

with source as (
    select * from {{ source('opendental', 'autocode') }}
),

renamed as (
    select
        -- Primary Key
        "AutoCodeNum" as autocode_num,
        
        -- Attributes
        "Description" as description,
        "IsHidden" = 1 as is_hidden,
        "LessIntrusive" = 1 as is_less_intrusive,
        
        -- Metadata
        current_timestamp as _loaded_at
    
    from source
)

select * from renamed
