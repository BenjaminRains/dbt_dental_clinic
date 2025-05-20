{{
    config(
        materialized='view'
    )
}}

with source as (
    select * from {{ source('opendental', 'entrylog') }}
),

renamed as (
    select
        -- Primary Key
        "EntryLogNum" as entry_log_id,
        
        -- Foreign Keys
        "UserNum" as user_id,
        "FKeyType" as foreign_key_type,
        "FKey" as foreign_key,
        
        -- Attributes
        "LogSource" as log_source,
        "EntryDateTime" as entry_datetime

    from source
)

select * from renamed
