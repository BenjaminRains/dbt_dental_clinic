with source as (
    select * from {{ source('opendental', 'tasknote') }}
),

renamed as (
    select
        "TaskNoteNum" as task_note_id,
        "TaskNum" as task_id,
        "UserNum" as user_id,
        "DateTimeNote" as note_datetime,
        "Note" as note
    from source
)

select * from renamed
