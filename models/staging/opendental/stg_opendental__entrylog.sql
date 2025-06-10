{{
    config(
        materialized='view'
    )
}}

with source_data as (
    select * from {{ source('opendental', 'entrylog') }}
),

renamed_columns as (
    select
        -- Primary Key
        "EntryLogNum" as entry_log_id,
        
        -- Foreign Keys
        "UserNum" as user_id,
        "FKeyType" as foreign_key_type,
        "FKey" as foreign_key,
        
        -- Attributes
        "LogSource" as log_source,
        {{ clean_opendental_date('"EntryDateTime"') }} as entry_datetime,

        -- Standardized metadata columns
        {{ standardize_metadata_columns(
            created_at_column='"EntryDateTime"',
            updated_at_column='"EntryDateTime"',
            created_by_column='"UserNum"'
        ) }}

    from source_data
)

select * from renamed_columns
