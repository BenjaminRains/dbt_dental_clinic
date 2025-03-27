{{
    config(
        materialized='view'
    )
}}

with source as (
    select * from {{ source('opendental', 'disease') }}
),

renamed as (
    select
        -- Primary Key
        "DiseaseNum" as disease_id,
        
        -- Foreign Keys
        "PatNum" as patient_id,
        "DiseaseDefNum" as disease_def_id,
        
        -- String/Text fields
        "PatNote" as patient_note,
        "SnomedProblemType" as snomed_problem_type,
        
        -- Timestamps/Dates
        "DateTStamp" as date_timestamp,
        "DateStart" as date_start,
        "DateStop" as date_stop,
        
        -- Numeric fields
        "ProbStatus" as problem_status,
        "FunctionStatus" as function_status

    from source
)

select * from renamed
