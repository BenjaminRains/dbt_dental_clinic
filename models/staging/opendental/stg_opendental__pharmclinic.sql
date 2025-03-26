with source as (
    select * from {{ source('opendental', 'pharmclinic') }}
),

renamed as (
    select
        -- Primary Key
        "PharmClinicNum" as pharm_clinic_num,
        
        -- Foreign Keys
        "PharmacyNum" as pharmacy_num,
        "ClinicNum" as clinic_num
        
    from source
)

select * from renamed
