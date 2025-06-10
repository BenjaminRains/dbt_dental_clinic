{{
    config(
        materialized='view'
    )
}}

with source_data as (
    select * from {{ source('opendental', 'disease') }}
),

renamed_columns as (
    select
        -- Primary Key
        "DiseaseNum" as disease_id,
        
        -- Foreign Keys  
        "PatNum" as patient_id,
        "DiseaseDefNum" as disease_def_id,
        
        -- String/Text fields
        "PatNote" as patient_note,
        "SnomedProblemType" as snomed_problem_type,
        
        -- Date fields with cleaning
        {{ clean_opendental_date('"DateTStamp"') }} as date_timestamp,
        {{ clean_opendental_date('"DateStart"') }} as date_start,
        {{ clean_opendental_date('"DateStop"') }} as date_stop,
        
        -- Status fields (potentially boolean)
        {{ convert_opendental_boolean('"ProbStatus"') }} as problem_status,
        {{ convert_opendental_boolean('"FunctionStatus"') }} as function_status,

        -- Standardized metadata columns (using improved macro)
        {{ standardize_metadata_columns(
            created_at_column='"DateStart"',
            updated_at_column='"DateTStamp"',
            created_by_column=none
        ) }}

    from source_data
)

select * from renamed_columns
