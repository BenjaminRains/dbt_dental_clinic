{{
    config(
        materialized='view'
    )
}}

with source_data as (
    select * from {{ source('opendental', 'diseasedef') }}
),

renamed_columns as (
    select
        -- Primary Key
        "DiseaseDefNum" as disease_def_id,
        
        -- Attributes
        "DiseaseName" as disease_name,
        "ItemOrder" as item_order,
        {{ convert_opendental_boolean('"IsHidden"') }} as is_hidden,
        {{ clean_opendental_date('"DateTStamp"') }} as date_timestamp,
        "ICD9Code" as icd9_code,
        "SnomedCode" as snomed_code,
        "Icd10Code" as icd10_code,

        -- Standardized metadata columns
        {{ standardize_metadata_columns(
            created_at_column='"DateTStamp"',
            updated_at_column='"DateTStamp"',
            created_by_column=none
        ) }}

    from source_data
)

select * from renamed_columns
