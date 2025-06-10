{{ config(
    materialized='view',
    schema='staging'
) }}

with source_data as (
    select * from {{ source('opendental', 'patientlink') }}
),

renamed_columns as (
    select
        -- ID Columns (with safe conversion)
        {{ transform_id_columns([
            {'source': '"PatientLinkNum"', 'target': 'patient_link_id'},
            {'source': '"PatNumFrom"', 'target': 'patient_id_from'},
            {'source': '"PatNumTo"', 'target': 'patient_id_to'}
        ]) }},
        
        -- Attributes
        "LinkType" as link_type,
        
        -- Date Fields
        {{ clean_opendental_date('"DateTimeLink"') }} as linked_at,
        
        -- Standardized metadata columns
        {{ standardize_metadata_columns(
            created_at_column='"DateTimeLink"',
            updated_at_column='"DateTimeLink"',
            created_by_column=none
        ) }}

    from source_data
)

select * from renamed_columns
