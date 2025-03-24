with source as (
    select * from {{ source('opendental', 'patientlink') }}
),

staged as (
    select
        -- primary key
        "PatientLinkNum" as patient_link_id,
        
        -- foreign keys
        "PatNumFrom" as patient_id_from,
        "PatNumTo" as patient_id_to,
        
        -- attributes
        "LinkType" as link_type,
        "DateTimeLink" as linked_at,
        
        -- metadata
        current_timestamp as dbt_loaded_at

    from source
)

select * from staged
