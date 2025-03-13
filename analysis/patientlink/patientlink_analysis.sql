with source as (
    select * from {{ source('opendental', 'patientlink') }}
),

renamed as (
    select
        -- Primary key
        PatientLinkNum as patient_link_id,
        
        -- Patient relationships
        PatNumFrom as from_patient_id,
        PatNumTo as to_patient_id,
        
        -- Link details
        LinkType as link_type,
        
        -- Timing
        DateTimeLink as link_created_datetime
    from source
)

select * from renamed
