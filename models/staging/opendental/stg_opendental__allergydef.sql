{{ config(
    materialized='view',
    schema='staging'
) }}

with source_data as (
    select * from {{ source('opendental', 'allergydef') }}
),

renamed_columns as (
    select
        -- Primary and Foreign Key transformations using macro
        {{ transform_id_columns([
            {'source': '"AllergyDefNum"', 'target': 'allergy_def_id'},
            {'source': 'NULLIF("MedicationNum", 0)', 'target': 'medication_id'}
        ]) }},
        
        -- Definition attributes
        nullif("Description", '')::text as allergy_description,
        nullif("SnomedType", 0)::integer as snomed_type,
        nullif("UniiCode", '')::text as unii_code,
        
        -- Boolean fields using macro
        {{ convert_opendental_boolean('"IsHidden"') }} as is_hidden,
        
        -- Standardized metadata using macro
        {{ standardize_metadata_columns(
            created_at_column='"DateTStamp"',
            updated_at_column='"DateTStamp"',
            created_by_column=none
        ) }}

    from source_data
)

select * from renamed_columns
