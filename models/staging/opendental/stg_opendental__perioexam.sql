{{
    config(
        materialized='incremental',
        unique_key='perioexam_id',
        schema='staging'
    )
}}

with source_data as (
    select * from {{ source('opendental', 'perioexam') }}
    where "ExamDate" >= '2023-01-01'
    {% if is_incremental() %}
        and "DateTMeasureEdit" > (select max(_updated_at) from {{ this }})
    {% endif %}
),

renamed_columns as (
    select
        -- ID Columns (with safe conversion)
        {{ transform_id_columns([
            {'source': '"PerioExamNum"', 'target': 'perioexam_id'},
            {'source': '"PatNum"', 'target': 'patient_id'},
            {'source': '"ProvNum"', 'target': 'provider_id'}
        ]) }},
        
        -- Date Fields
        {{ clean_opendental_date('"ExamDate"') }} as exam_date,
        {{ clean_opendental_date('"DateTMeasureEdit"') }} as measure_edit_timestamp,
        
        -- Attributes
        "Note" as note,
        
        -- Standardized metadata columns
        {{ standardize_metadata_columns(
            created_at_column='"ExamDate"',
            updated_at_column='"DateTMeasureEdit"',
            created_by_column=none
        ) }}
    from source_data
)

select * from renamed_columns
