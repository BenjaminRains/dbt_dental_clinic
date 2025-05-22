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
        "EntryDateTime" as entry_datetime,

        -- Required metadata columns
        current_timestamp as _loaded_at,
        "EntryDateTime" as _created_at,  -- Entry logs are created when they're written
        "EntryDateTime" as _updated_at   -- Entry logs are immutable, so creation = update

    from source
)

select * from renamed
