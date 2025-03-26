{{
    config(
        materialized='incremental',
        unique_key='perioexam_id'
    )
}}

with source as (
    select * from {{ source('opendental', 'perioexam') }}
    where "ExamDate" >= '2023-01-01'
    {% if is_incremental() %}
        and "DateTMeasureEdit" > (select max(measure_edit_timestamp) from {{ this }})
    {% endif %}
),

renamed as (
    select
        "PerioExamNum" as perioexam_id,
        "PatNum" as patient_id,
        "ExamDate" as exam_date,
        "ProvNum" as provider_id,
        "DateTMeasureEdit" as measure_edit_timestamp,
        "Note" as note
    from source
)

select * from renamed
