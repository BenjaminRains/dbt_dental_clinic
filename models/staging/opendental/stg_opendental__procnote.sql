{{ config(
    materialized='incremental',
    unique_key='procnote_id',
    schema='staging'
) }}

with source_data as (
    select * from {{ source('opendental', 'procnote') }}
    where "EntryDateTime" >= '2023-01-01'
    {% if is_incremental() %}
        and "EntryDateTime" > (select max(_updated_at) from {{ this }})
    {% endif %}
),

renamed_columns as (
    select
        -- ID Columns (with safe conversion)
        {{ transform_id_columns([
            {'source': '"ProcNoteNum"', 'target': 'procnote_id'},
            {'source': '"PatNum"', 'target': 'patient_id'},
            {'source': '"ProcNum"', 'target': 'procedure_id'},
            {'source': '"UserNum"', 'target': 'user_id'}
        ]) }},
        
        -- Boolean Fields
        {{ convert_opendental_boolean('"SigIsTopaz"') }} as is_topaz_signature,
        
        -- Date Fields
        {{ clean_opendental_date('"EntryDateTime"') }} as entry_timestamp,
        
        -- Additional Attributes
        "Note" as note,
        "Signature" as signature,
        
        -- Standardized metadata columns
        {{ standardize_metadata_columns(
            created_at_column='"EntryDateTime"',
            updated_at_column='"EntryDateTime"',
            created_by_column=none
        ) }}

    from source_data
)

select * from renamed_columns
