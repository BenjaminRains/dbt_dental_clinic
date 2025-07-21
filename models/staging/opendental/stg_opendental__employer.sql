{{ config(
    materialized='view'
) }}

with source_data as (
    select * from {{ source('opendental', 'employer') }}
),

renamed_columns as (
    select
        -- Primary Key transformations using macro
        {{ transform_id_columns([
            {'source': '"EmployerNum"', 'target': 'employer_id'}
        ]) }},
        
        -- Employer attributes
        nullif("EmpName", '')::text as employer_name,
        nullif("Address", '')::text as address,
        nullif("Address2", '')::text as address2,
        nullif("City", '')::text as city,
        nullif("State", '')::text as state,
        nullif("Zip", '')::text as zip,
        nullif("Phone", '')::text as phone,

        -- Standardized metadata using macro
        {{ standardize_metadata_columns(
            created_at_column=none,
            updated_at_column=none,
            created_by_column=none
        ) }}

    from source_data
)

select * from renamed_columns
