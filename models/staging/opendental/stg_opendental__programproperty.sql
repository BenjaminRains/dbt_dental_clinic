{{
    config(
        materialized='view'
    )
}}

with source_data as (
    select * from {{ source('opendental', 'programproperty') }}
),

renamed_columns as (
    select
        -- ID Columns (with safe conversion)
        {{ transform_id_columns([
            {'source': '"ProgramPropertyNum"', 'target': 'program_property_id'},
            {'source': '"ProgramNum"', 'target': 'program_id'},
            {'source': '"ClinicNum"', 'target': 'clinic_id'}
        ]) }},
        
        -- Boolean Fields
        {{ convert_opendental_boolean('"IsMasked"') }} as is_masked,
        {{ convert_opendental_boolean('"IsHighSecurity"') }} as is_high_security,
        
        -- Property Configuration
        "PropertyDesc" as property_desc,
        "PropertyValue" as property_value,
        "ComputerName" as computer_name,

        -- Standardized metadata columns
        {{ standardize_metadata_columns(
            created_at_column=none,
            updated_at_column=none,
            created_by_column=none
        ) }}

    from source_data
)

select * from renamed_columns
