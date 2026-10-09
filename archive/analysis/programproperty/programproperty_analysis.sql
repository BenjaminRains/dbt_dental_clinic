with source as (
    select * from {{ source('opendental', 'programproperty') }}
),

renamed as (
    select
        -- Primary key
        ProgramPropertyNum as program_property_id,

        -- Relationships
        ProgramNum as program_id,
        ClinicNum as clinic_id,

        -- Property details
        PropertyDesc as property_description,
        PropertyValue as property_value, -- May contain sensitive configuration data
        ComputerName as computer_name,

        -- Security flags
        IsMasked as is_masked_flag,
        IsHighSecurity as is_high_security_flag
    from source
)

select * from renamed