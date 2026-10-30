with source as (
    select * from {{ source('opendental', 'feeschedgroup') }}
),

renamed as (
    select
        -- Primary key
        FeeSchedGroupNum as fee_schedule_group_id,
        
        -- Relationships
        FeeSchedNum as fee_schedule_id,
        
        -- Description
        Description as group_description,
        
        -- Clinic assignments (stored as comma-separated list)
        ClinicNums as clinic_ids
    from source
)

select * from renamed