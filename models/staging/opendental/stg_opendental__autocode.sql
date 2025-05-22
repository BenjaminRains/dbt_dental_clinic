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
        "AutoCodeNum" as autocode_id,
        
        -- Attributes
        "Description" as description,
        CASE WHEN "IsHidden" = 1 THEN true WHEN "IsHidden" = 0 THEN false ELSE null END as is_hidden,
        CASE WHEN "LessIntrusive" = 1 THEN true WHEN "LessIntrusive" = 0 THEN false ELSE null END as is_less_intrusive,
        
        -- Metadata
        current_timestamp as _loaded_at
    
    from Source
)

select * from renamed
