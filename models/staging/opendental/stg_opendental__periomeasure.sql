{{
    config(
        materialized='incremental',
        unique_key='periomeasure_id'
    )
}}

with source as (
    select m.* 
    from {{ source('opendental', 'periomeasure') }} m
    join {{ source('opendental', 'perioexam') }} e
        on m."PerioExamNum" = e."PerioExamNum"
    where e."ExamDate" >= '2023-01-01'
    {% if is_incremental() %}
        and m."SecDateTEdit" > (select max(edit_timestamp) from {{ this }})
    {% endif %}
),

renamed as (
    select
        "PerioMeasureNum" as periomeasure_id,
        "PerioExamNum" as perioexam_id,
        "SequenceType" as sequence_type,
        "IntTooth" as tooth_number,
        "ToothValue" as tooth_value,
        "MBvalue" as mesial_buccal_value,
        "Bvalue" as buccal_value,
        "DBvalue" as distal_buccal_value,
        "MLvalue" as mesial_lingual_value,
        "Lvalue" as lingual_value,
        "DLvalue" as distal_lingual_value,
        "SecDateTEntry" as entry_timestamp,
        "SecDateTEdit" as edit_timestamp,
        -- Required metadata columns
        current_timestamp as _loaded_at,
        "SecDateTEntry" as _created_at,  -- Using SecDateTEntry as creation timestamp
        "SecDateTEdit" as _updated_at    -- Using SecDateTEdit as update timestamp
    from source
)

select * from renamed
