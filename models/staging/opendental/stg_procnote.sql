with source as (
    select * from {{ source('opendental', 'procnote') }}
),

renamed as (
    select
        -- Primary key
        ProcNoteNum as procedure_note_id,

        -- Relationships
        PatNum as patient_id,
        ProcNum as procedure_id,
        UserNum as user_id,

        -- Timing
        EntryDateTime as entry_datetime,

        -- Note content (may contain PHI)
        Note as note_text,

        -- Signature information
        SigIsTopaz as is_topaz_signature_flag,
        Signature as signature_data
    from source
)

select * from renamed