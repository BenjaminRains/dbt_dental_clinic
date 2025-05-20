{{
    config(
        materialized='view'
    )
}}

with source as (
    select * from {{ source('opendental', 'employer') }}
),

renamed as (
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
        "Phone" as phone

    from source
)

select * from renamed
