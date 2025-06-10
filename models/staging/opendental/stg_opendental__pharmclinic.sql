{{
    config(
        materialized='table'
    )
}}

with source_data as (
    select * from {{ source('opendental', 'pharmclinic') }}
),

renamed_columns as (
    select
        -- ID Columns (with safe conversion)
        {{ transform_id_columns([
            {'source': '"PharmClinicNum"', 'target': 'pharm_clinic_id'},
            {'source': '"PharmacyNum"', 'target': 'pharmacy_id'},
            {'source': '"ClinicNum"', 'target': 'clinic_id'}
        ]) }},
        
        -- Standardized metadata columns
        {{ standardize_metadata_columns(
            created_at_column=none,
            updated_at_column=none,
            created_by_column=none
        ) }}
        
    from source_data
)

select * from renamed_columns
