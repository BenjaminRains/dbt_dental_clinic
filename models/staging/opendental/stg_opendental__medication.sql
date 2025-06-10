{{ config(
    materialized='view'
) }}

with source_data as (
    select * from {{ source('opendental', 'medication') }}
),

renamed_columns as (
    select
        -- ID Columns (with safe conversion)
        {{ transform_id_columns([
            {'source': '"MedicationNum"', 'target': 'medication_id'},
            {'source': '"GenericNum"', 'target': 'generic_id'}
        ]) }},
        
        -- Attributes
        "MedName" as medication_name,
        "Notes" as notes,
        "RxCui" as rxcui,
        
        -- Date Fields
        {{ clean_opendental_date('"DateTStamp"') }} as date_updated,
        
        -- Standardized metadata columns
        {{ standardize_metadata_columns(
            created_at_column='"DateTStamp"',
            updated_at_column='"DateTStamp"',
            created_by_column=none
        ) }}

    from source_data
)

select * from renamed_columns
