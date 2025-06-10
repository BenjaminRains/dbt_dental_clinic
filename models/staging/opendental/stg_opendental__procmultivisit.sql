{{ config(
    materialized='incremental',
    unique_key='procmultivisit_id',
    schema='staging'
) }}

with source_data as (
    select * from {{ source('opendental', 'procmultivisit') }}
    where "SecDateTEntry" >= '2023-01-01'
    {% if is_incremental() %}
        and "SecDateTEdit" > (select max(_updated_at) from {{ this }})
    {% endif %}
),

renamed_columns as (
    select
        -- ID Columns (with safe conversion)
        {{ transform_id_columns([
            {'source': '"ProcMultiVisitNum"', 'target': 'procmultivisit_id'},
            {'source': '"GroupProcMultiVisitNum"', 'target': 'group_procmultivisit_id'},
            {'source': '"ProcNum"', 'target': 'procedure_id'},
            {'source': '"PatNum"', 'target': 'patient_id'}
        ]) }},
        
        -- Status Fields
        "ProcStatus" as procedure_status,
        
        -- Boolean Fields
        {{ convert_opendental_boolean('"IsInProcess"') }} as is_in_process,
        
        -- Date Fields
        {{ clean_opendental_date('"SecDateTEntry"') }} as entry_date,
        {{ clean_opendental_date('"SecDateTEdit"') }} as edit_date,
        
        -- Standardized metadata columns
        {{ standardize_metadata_columns(
            created_at_column='"SecDateTEntry"',
            updated_at_column='"SecDateTEdit"',
            created_by_column=none
        ) }}
    from source_data
)

select * from renamed_columns
