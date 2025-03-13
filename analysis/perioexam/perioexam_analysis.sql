with source as (
    select * from {{ source('opendental', 'perioexam') }}
),

renamed as (
    select
        -- Primary key
        PerioExamNum as periodontal_exam_id,

        -- Relationships
        PatNum as patient_id,
        ProvNum as provider_id,

        -- Dates
        ExamDate as exam_date,
        DateTMeasureEdit as measurements_updated_at,

        -- Notes (may contain PHI)
        Note as exam_notes
    from source
)

select * from renamed