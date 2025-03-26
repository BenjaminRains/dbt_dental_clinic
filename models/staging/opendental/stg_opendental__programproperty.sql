{{
    config(
        materialized='view'
    )
}}

with source as (
    select * from {{ source('opendental', 'programproperty') }}
),

renamed as (
    select
        -- Primary Key
        "ProgramPropertyNum" as program_property_num,
        
        -- Foreign Keys
        "ProgramNum" as program_num,
        "ClinicNum" as clinic_num,
        
        -- Regular columns
        "PropertyDesc" as property_desc,
        "PropertyValue" as property_value,
        "ComputerName" as computer_name,
        
        -- Boolean/Flag columns
        "IsMasked" = 1 as is_masked,
        "IsHighSecurity" = 1 as is_high_security

    from source
)

select * from renamed
