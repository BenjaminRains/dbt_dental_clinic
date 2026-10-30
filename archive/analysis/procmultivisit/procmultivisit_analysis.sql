with source as (
    select * from {{ source('opendental', 'procmultivisit') }}
),

renamed as (
    select
        -- Primary key
        ProcMultiVisitNum as procedure_multi_visit_id,

        -- Relationships
        GroupProcMultiVisitNum as group_multi_visit_id,
        ProcNum as procedure_id,
        PatNum as patient_id,

        -- Status tracking
        ProcStatus as procedure_status,
        IsInProcess as is_in_process_flag,

        -- Metadata
        SecDateTEntry as created_at,
        SecDateTEdit as updated_at
    from source
)

select * from renamed