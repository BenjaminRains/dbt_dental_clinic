{{ config(
    materialized='view'
) }}

with source_data as (
    select * from {{ source('opendental', 'operatory') }}
),

renamed_columns as (
    select
        -- Primary and Foreign Key transformations using macro
        {{ transform_id_columns([
            {'source': '"OperatoryNum"', 'target': 'operatory_id'},
            {'source': 'NULLIF("ProvDentist", 0)', 'target': 'primary_dentist_id'},
            {'source': 'NULLIF("ProvHygienist", 0)', 'target': 'hygienist_id'},
            {'source': 'NULLIF("ClinicNum", 0)', 'target': 'clinic_id'},
            {'source': 'NULLIF("OperatoryType", 0)', 'target': 'operatory_type_id'}
        ]) }},
        
        -- Business field transformations
        nullif(trim("OpName"), '') as operatory_name,
        nullif(trim("Abbrev"), '') as abbreviation,
        "ItemOrder"::smallint as display_order,
        
        -- Boolean fields using macro
        {{ convert_opendental_boolean('"IsHidden"') }} as is_hidden,
        {{ convert_opendental_boolean('"IsHygiene"') }} as is_hygiene_operatory,
        {{ convert_opendental_boolean('"SetProspective"') }} as is_prospective_scheduling,
        {{ convert_opendental_boolean('"IsWebSched"') }} as is_web_scheduling_enabled,
        {{ convert_opendental_boolean('"IsNewPatAppt"') }} as is_new_patient_appointment_enabled,
        
        -- Standardized metadata using macro
        {{ standardize_metadata_columns(
            updated_at_column='"DateTStamp"'
        ) }}

    from source_data
)

select * from renamed_columns
