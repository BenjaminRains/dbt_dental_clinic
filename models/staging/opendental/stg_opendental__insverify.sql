{{ config(
    materialized='view'
) }}

with source_data as (
    select * 
    from {{ source('opendental', 'insverify') }}
    where "SecDateTEdit" >= '2023-01-01'
),

renamed_columns as (
    select
        -- ID Columns (with safe conversion)
        {{ transform_id_columns([
            {'source': '"InsVerifyNum"', 'target': 'insurance_verify_id'},
            {'source': '"UserNum"', 'target': 'user_id'},
            {'source': '"FKey"', 'target': 'foreign_key_id'},
            {'source': '"DefNum"', 'target': 'definition_id'}
        ]) }},
        
        -- Attributes
        "VerifyType" as verify_type,
        "Note" as note,
        "HoursAvailableForVerification" as hours_available_for_verification,
        
        -- Date Fields
        {{ clean_opendental_date('"DateLastVerified"') }} as last_verified_date,
        {{ clean_opendental_date('"DateLastAssigned"') }} as last_assigned_date,
        {{ clean_opendental_date('"DateTimeEntry"') }} as date_created,
        {{ clean_opendental_date('"SecDateTEdit"') }} as date_updated,

        -- Standardized metadata columns
        {{ standardize_metadata_columns(
            created_at_column='"DateTimeEntry"',
            updated_at_column='"SecDateTEdit"',
            created_by_column=none
        ) }}

    from source_data
)

select * from renamed_columns
