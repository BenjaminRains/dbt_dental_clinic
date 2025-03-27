with source as (
    select * from {{ source('opendental', 'pharmclinic') }}
),

renamed as (
    select
        -- Primary Key
        "PharmClinicNum" as pharm_clinic_id,
        
        -- Foreign Keys
        "PharmacyNum" as pharmacy_id,
        "ClinicNum" as clinic_id
        
    from source
)

select * from renamed
