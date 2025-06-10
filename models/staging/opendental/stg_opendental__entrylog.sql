{{
    config(
        materialized='view',
        schema='staging'
    )
}}

with source_data as (
    select * from {{ source('opendental', 'entrylog') }}
),

renamed_columns as (
    select
        -- Primary Key and Foreign Keys
        {{ transform_id_columns([
            {'source': '"EntryLogNum"', 'target': 'entry_log_id'},
            {'source': '"UserNum"', 'target': 'user_id'},
            {'source': '"FKey"', 'target': 'foreign_key'}
        ]) }},
        
        -- Attributes
        "FKeyType" as foreign_key_type,
        
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
