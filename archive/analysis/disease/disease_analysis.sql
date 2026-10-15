with source as (
    select * from {{ source('opendental', 'disease') }}
),

renamed as (
    select
        -- Primary key
        DiseaseNum as disease_id,

        -- Relationships
        PatNum as patient_id,
        DiseaseDefNum as disease_definition_id,

        -- Disease details
        ProbStatus as problem_status,
        SnomedProblemType as snomed_problem_type,
        FunctionStatus as function_status,

        -- Dates
        DateStart as start_date,
        DateStop as stop_date,
        DateTStamp as updated_at

        -- Excluded fields that may contain PHI:
        -- PatNote
    from source
)

select * from renamed
