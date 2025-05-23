{{
    config(
        materialized='table'
    )
}}

with source as (
    select * from {{ source('opendental', 'pharmclinic') }}
),

renamed as (
    select
        -- Primary Key
        "PharmClinicNum" as pharm_clinic_id,
        
        -- Foreign Keys
        "PharmacyNum" as pharmacy_id,
        "ClinicNum" as clinic_id,
        
        -- Required metadata columns
        current_timestamp as _loaded_at,
        current_timestamp as _created_at,  -- Using current_timestamp as no source timestamp available
        current_timestamp as _updated_at   -- Using current_timestamp as no source timestamp available
        
    from source
)

select * from renamed
