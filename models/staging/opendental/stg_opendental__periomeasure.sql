{{ config(
    materialized='incremental',
    unique_key='periomeasure_id'
) }}

with source_data as (
    select m.* 
    from {{ source('opendental', 'periomeasure') }} m
    join {{ source('opendental', 'perioexam') }} e
        on m."PerioExamNum" = e."PerioExamNum"
    where e."ExamDate" >= '2023-01-01'
    {% if is_incremental() %}
        and m."SecDateTEdit" > (select max(_updated_at) from {{ this }})
    {% endif %}
),

renamed_columns as (
    select
        -- ID Columns (with safe conversion)
        {{ transform_id_columns([
            {'source': '"PerioMeasureNum"', 'target': 'periomeasure_id'},
            {'source': '"PerioExamNum"', 'target': 'perioexam_id'}
        ]) }},
        
        -- Measurement Details
        "SequenceType" as sequence_type,
        "IntTooth" as tooth_number,
        "ToothValue" as tooth_value,
        "MBvalue" as mesial_buccal_value,
        "Bvalue" as buccal_value,
        "DBvalue" as distal_buccal_value,
        "MLvalue" as mesial_lingual_value,
        "Lvalue" as lingual_value,
        "DLvalue" as distal_lingual_value,
        
        -- Date Fields
        {{ clean_opendental_date('"SecDateTEntry"') }} as entry_timestamp,
        {{ clean_opendental_date('"SecDateTEdit"') }} as edit_timestamp,
        
        -- Standardized metadata columns
        {{ standardize_metadata_columns(
            created_at_column='"SecDateTEntry"',
            updated_at_column='"SecDateTEdit"',
            created_by_column=none
        ) }}
    from source_data
)

select * from renamed_columns
