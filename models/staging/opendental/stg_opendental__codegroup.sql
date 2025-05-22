{{
    config(
        materialized='view'
    )
}}

with source as (
    select * from {{ source('opendental', 'codegroup') }}
),

renamed as (
    select
        -- Primary Key
        "CodeGroupNum" as codegroup_id,
        
        -- Attributes
        "GroupName" as group_name,
        "ProcCodes" as proc_codes,
        "ItemOrder" as item_order,
        "CodeGroupFixed" = 1 as is_fixed,
        "IsHidden" = 1 as is_hidden,
        "ShowInAgeLimit" = 1 as show_in_age_limit,
        
        -- Metadata
        current_timestamp as _loaded_at
    
    from Source
)

select * from renamed
