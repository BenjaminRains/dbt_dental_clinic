with source as (
    select * from {{ source('opendental', 'treatplanattach') }}
),

renamed as (
    select
        -- Primary key
        TreatPlanAttachNum as treatment_plan_attachment_id,

        -- Relationships
        TreatPlanNum as treatment_plan_id,
        ProcNum as procedure_id,

        -- Configuration
        Priority as priority_id
    from source
)

select * from renamed