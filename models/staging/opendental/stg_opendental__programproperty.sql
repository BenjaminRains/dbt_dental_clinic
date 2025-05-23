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
        "ProgramPropertyNum" as program_property_id,
        
        -- Foreign Keys
        "ProgramNum" as program_id,
        "ClinicNum" as clinic_id,
        
        -- Regular columns
        "PropertyDesc" as property_desc,
        "PropertyValue" as property_value,
        "ComputerName" as computer_name,
        
        -- Boolean/Flag columns
        "IsMasked" = 1 as is_masked,
        "IsHighSecurity" = 1 as is_high_security,

        -- Metadata
        current_timestamp as _loaded_at,
        current_timestamp as _created_at,  -- Since this is a view of reference data, creation time is same as load time
        current_timestamp as _updated_at   -- Since this is a view of reference data, update time is same as load time

    from source
)

select * from renamed
