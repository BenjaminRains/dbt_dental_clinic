with source as (
    select * from {{ source('opendental', 'pharmclinic') }}
),

renamed as (
    select
        -- Primary key
        PharmClinicNum as pharmacy_clinic_id,

        -- Relationships
        PharmacyNum as pharmacy_id,
        ClinicNum as clinic_id
    from source
)

select * from renamed