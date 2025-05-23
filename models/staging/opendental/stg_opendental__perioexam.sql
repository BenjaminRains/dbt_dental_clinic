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
        "Note" as note,
        -- Required metadata columns
        current_timestamp as _loaded_at,
        "ExamDate" as _created_at,  -- Using ExamDate as creation timestamp
        "DateTMeasureEdit" as _updated_at  -- Using DateTMeasureEdit as update timestamp
    from source
)

select * from renamed
