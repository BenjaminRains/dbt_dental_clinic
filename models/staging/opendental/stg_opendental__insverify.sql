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
        -- Primary Key
        "InsVerifyNum" as insurance_verify_id,
        
        -- Foreign Keys
        "UserNum" as user_id,
        "FKey" as foreign_key_id,
        "DefNum" as definition_id,
        
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
