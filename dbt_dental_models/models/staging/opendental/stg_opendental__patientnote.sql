{{ config(
    materialized='view'
) }}

with source_data as (
    select * from {{ source('opendental', 'patientnote') }}
    where {{ clean_opendental_date('"SecDateTEntry"') }} >= '2023-01-01' -- Filter to include only notes from 2023 onwards
),

renamed_columns as (
    select
        -- ID Columns (with safe conversion)
        {{ transform_id_columns([
            {'source': '"PatNum"', 'target': 'patient_id'},
            {'source': '"UserNumOrthoLocked"', 'target': 'user_id_ortho_locked'}
        ]) }},
        
        -- Text Attributes
        "FamFinancial" as family_financial,
        "ApptPhone" as appointment_phone,
        "Medical" as medical,
        "Service" as service,
        "MedicalComp" as medical_comp,
        "Treatment" as treatment,
        "ICEName" as ice_name,
        "ICEPhone" as ice_phone,
        "Consent" as consent,
        "Pronoun" as pronoun,
        
        -- Numeric Attributes
        "OrthoMonthsTreatOverride" as ortho_months_treat_override,
        
        -- Date Fields
        {{ clean_opendental_date('"DateOrthoPlacementOverride"') }} as date_ortho_placement_override,
        {{ clean_opendental_date('"SecDateTEntry"') }} as date_created,
        {{ clean_opendental_date('"SecDateTEdit"') }} as date_updated,

        -- Standardized metadata columns
        {{ standardize_metadata_columns(
            created_at_column='"SecDateTEntry"',
            updated_at_column='"SecDateTEdit"'
        ) }}

    from source_data
)

select * from renamed_columns
