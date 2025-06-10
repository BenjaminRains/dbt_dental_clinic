{{ config(
    materialized='view',
    schema='staging'
) }}

with source_data as (
    select * from {{ source('opendental', 'zipcode') }}
),

renamed_columns as (
    select
        -- Primary and Foreign Key transformations using macro
        {{ transform_id_columns([
            {'source': '"ZipCodeNum"', 'target': 'zipcode_id'}
        ]) }},
        
        -- Business Columns with enhanced data cleaning
        -- Clean zipcode: ensure it's exactly 5 digits, pad with leading zeros if needed
        LPAD(REGEXP_REPLACE({{ clean_opendental_string('"ZipCodeDigits"') }}, '[^0-9]', '', 'g'), 5, '0') as zipcode,
        {{ clean_opendental_string('"City"') }} as city,
        -- Clean state: ensure it's exactly 2 uppercase letters  
        UPPER(REGEXP_REPLACE({{ clean_opendental_string('"State"') }}, '[^A-Za-z]', '', 'g')) as state,
        
        -- Boolean fields using macro
        {{ convert_opendental_boolean('"IsFrequent"') }} as is_frequent,
        
        -- Standardized metadata using macro
        {{ standardize_metadata_columns(
            created_at_column=none,
            updated_at_column=none,
            created_by_column=none
        ) }}
        
    from source_data
)

select * from renamed_columns 