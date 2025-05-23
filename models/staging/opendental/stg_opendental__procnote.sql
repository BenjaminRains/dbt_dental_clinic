{{ config(
    materialized='incremental',
    unique_key='procnote_id'
) }}

with source as (
    select * from {{ source('opendental', 'procnote') }}
    where "EntryDateTime" >= '2023-01-01'
    {% if is_incremental() %}
        and "EntryDateTime" > (select max(_updated_at) from {{ this }})
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
        
        -- Metadata
        current_timestamp as _loaded_at,
        "EntryDateTime" as _created_at,
        "EntryDateTime" as _updated_at

    from source
)

select * from renamed
