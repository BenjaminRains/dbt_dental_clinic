with source_data as (
    select * from {{ source('opendental', 'tasknote') }}
),

renamed_columns as (
    select
        -- Primary and Foreign Key transformations using macro
        {{ transform_id_columns([
            {'source': '"TaskNoteNum"', 'target': 'task_note_id'},
            {'source': 'NULLIF("TaskNum", 0)', 'target': 'task_id'},
            {'source': 'NULLIF("UserNum", 0)', 'target': 'user_id'}
        ]) }},
        
        -- Timestamps
        "DateTimeNote" as note_datetime,
        
        -- Content
        "Note" as note,
        
        -- Standardized metadata using macro
        {{ standardize_metadata_columns(
            created_at_column='"DateTimeNote"',
            updated_at_column='"DateTimeNote"',
            created_by_column=none
        ) }}
        
    from source_data
)

select * from renamed_columns
