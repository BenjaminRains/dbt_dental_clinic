{{ config(
    materialized='incremental',
    unique_key='procnote_id'
) }}

with source as (
    select * from {{ source('opendental', 'procnote') }}
    where "EntryDateTime" >= '2023-01-01'
    {% if is_incremental() %}
        and "EntryDateTime" > (select max(entry_timestamp) from {{ this }})
    {% endif %}
),

renamed as (
    select
        -- Primary key
        "ProcNoteNum" as procnote_id,
        
        -- Foreign keys
        "PatNum" as patient_id,
        "ProcNum" as procedure_id,
        "UserNum" as user_id,
        
        -- Additional attributes
        "EntryDateTime" as entry_timestamp,
        "Note" as note,
        "SigIsTopaz" as is_topaz_signature,
        "Signature" as signature,
        
        -- Meta fields
        {{ current_timestamp() }} as _data_loaded_at

    from source
)

select * from renamed
