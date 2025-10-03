{{ config(
    materialized='view'
) }}

with source_data as (
    select * from {{ source('opendental', 'laboratory') }}
),

renamed_columns as (
    select
        -- Primary Key transformation using macro
        {{ transform_id_columns([
            {'source': '"LaboratoryNum"', 'target': 'laboratory_id'}
        ]) }},
        
        -- Business field transformations
        nullif(trim("Description"), '') as laboratory_name,
        nullif(trim("Phone"), '') as phone_number,
        nullif(trim("Notes"), '') as notes,
        "Slip"::bigint as slip_number,
        nullif(trim("Address"), '') as address,
        nullif(trim("City"), '') as city,
        nullif(trim("State"), '') as state,
        nullif(trim("Zip"), '') as zip_code,
        nullif(trim("Email"), '') as email_address,
        nullif(trim("WirelessPhone"), '') as mobile_phone,
        
        -- Boolean fields using macro
        {{ convert_opendental_boolean('"IsHidden"') }} as is_hidden,
        
        -- Standardized metadata using macro
        {{ standardize_metadata_columns() }}

    from source_data
)

select * from renamed_columns
