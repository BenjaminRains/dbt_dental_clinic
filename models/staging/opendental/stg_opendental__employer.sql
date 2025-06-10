{{
    config(
        materialized='view'
    )
}}

with source_data as (
    select * from {{ source('opendental', 'employer') }}
),

renamed_columns as (
    select
        -- Primary Key
        "EmployerNum" as employer_id,
        
        -- Attributes
        "EmpName" as employer_name,
        "Address" as address,
        "Address2" as address2,
        "City" as city,
        "State" as state,
        "Zip" as zip,
        "Phone" as phone,

        -- Standardized metadata columns
        {{ standardize_metadata_columns(
            created_at_column=none,
            updated_at_column=none,
            created_by_column=none
        ) }}

    from source_data
)

select * from renamed_columns
