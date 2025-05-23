with source as (
    select * from {{ source('opendental', 'tasknote') }}
),

renamed as (
    select
        -- Primary Key
        "TaskNoteNum" as task_note_id,
        
        -- Foreign Keys
        "TaskNum" as task_id,
        "UserNum" as user_id,
        
        -- Timestamps
        "DateTimeNote" as note_datetime,
        
        -- Content
        "Note" as note,
        
        -- Required metadata columns
        current_timestamp as _loaded_at,
        "DateTimeNote" as _created_at,
        "DateTimeNote" as _updated_at
    from source
)

select * from renamed
