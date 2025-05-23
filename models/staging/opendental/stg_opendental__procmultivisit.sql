{{ config(
    materialized='incremental',
    unique_key='procmultivisit_id'
) }}

with source as (
    select * from {{ source('opendental', 'procmultivisit') }}
    where "SecDateTEntry" >= '2023-01-01'
    {% if is_incremental() %}
        and "SecDateTEdit" > (select max(_updated_at) from {{ this }})
    {% endif %}
),

renamed as (
    select
        -- Primary key
        "ProcMultiVisitNum" as procmultivisit_id,
        
        -- Foreign keys
        "GroupProcMultiVisitNum" as group_procmultivisit_id,
        "ProcNum" as procedure_id,
        "PatNum" as patient_id,
        
        -- Status fields
        "ProcStatus" as procedure_status,
        "IsInProcess" as is_in_process,
        
        -- Date and time fields
        "SecDateTEntry" as sec_date_entry,
        "SecDateTEdit" as sec_date_edit,
        
        -- Metadata
        current_timestamp as _loaded_at,
        "SecDateTEntry" as _created_at,
        "SecDateTEdit" as _updated_at
    from source
)

select * from renamed
