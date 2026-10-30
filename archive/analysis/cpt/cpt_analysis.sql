with source as (
    select * from {{ source('opendental', 'cpt') }}
),

renamed as (
    select
        -- Primary key
        CptNum as cpt_id,
        
        -- CPT code details
        CptCode as cpt_code,
        Description as description,
        VersionIDs as version_ids
    from source
)

select * from renamed