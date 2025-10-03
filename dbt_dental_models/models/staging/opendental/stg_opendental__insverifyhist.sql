{{ config(
    materialized='view'
) }}

with source_data as (
    select * 
    from {{ source('opendental', 'insverifyhist') }}
    where "SecDateTEdit" >= '2023-01-01'
),

renamed_columns as (
    select
        -- Primary and Foreign Key transformations using macro
        {{ transform_id_columns([
            {'source': '"InsVerifyHistNum"', 'target': 'insurance_verify_history_id'},
            {'source': '"InsVerifyNum"', 'target': 'insurance_verify_id'},
            {'source': 'NULLIF("UserNum", 0)', 'target': 'user_id'},
            {'source': 'NULLIF("VerifyUserNum", 0)', 'target': 'verify_user_id'},
            {'source': 'NULLIF("FKey", 0)', 'target': 'foreign_key_id'},
            {'source': 'NULLIF("DefNum", 0)', 'target': 'definition_id'}
        ]) }},
        
        -- Date fields using macro
        {{ clean_opendental_date('"DateLastVerified"') }} as last_verified_date,
        {{ clean_opendental_date('"DateLastAssigned"') }} as last_assigned_date,
        {{ clean_opendental_date('"DateTimeEntry"') }} as date_created,
        {{ clean_opendental_date('"SecDateTEdit"') }} as date_updated,
        
        -- Attributes
        "VerifyType"::smallint as verify_type,
        nullif(trim("Note"), '') as note,
        "HoursAvailableForVerification"::double precision as hours_available_for_verification,

        -- Standardized metadata using macro
        {{ standardize_metadata_columns(
            created_at_column='"DateTimeEntry"',
            updated_at_column='"SecDateTEdit"'
        ) }}

    from source_data
)

select * from renamed_columns
