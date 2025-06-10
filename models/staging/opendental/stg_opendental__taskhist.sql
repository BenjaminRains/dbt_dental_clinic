{{
    config(
        materialized='view'
    )
}}

with source_data as (
    select * from {{ source('opendental', 'taskhist') }}
    where "DateTStamp" >= '2023-01-01'
),

renamed_columns as (
    select
        -- Primary and Foreign Key transformations using macro
        {{ transform_id_columns([
            {'source': '"TaskHistNum"', 'target': 'task_hist_id'},
            {'source': 'NULLIF("UserNumHist", 0)', 'target': 'user_hist_id'},
            {'source': 'NULLIF("TaskNum", 0)', 'target': 'task_id'},
            {'source': 'NULLIF("TaskListNum", 0)', 'target': 'task_list_id'},
            {'source': 'NULLIF("KeyNum", 0)', 'target': 'key_id'},
            {'source': 'NULLIF("FromNum", 0)', 'target': 'from_id'},
            {'source': 'NULLIF("UserNum", 0)', 'target': 'user_id'},
            {'source': 'NULLIF("PriorityDefNum", 0)', 'target': 'priority_def_id'},
            {'source': 'NULLIF("TriageCategory", 0)', 'target': 'triage_category_id'}
        ]) }},
        
        -- Timestamps
        "DateTStamp" as timestamp,
        "DateTimeEntry" as entry_datetime,
        "DateTimeFinished" as finished_datetime,
        "DateTimeOriginal" as original_datetime,
        
        -- Date fields using macro
        {{ clean_opendental_date('"DateTask"') }} as task_date,
        
        -- Task attributes
        "Descript" as description,
        "TaskStatus" as task_status,
        "DateType" as date_type,
        "ObjectType" as object_type,
        "ReminderGroupId" as reminder_group_id,
        "ReminderType" as reminder_type,
        "ReminderFrequency" as reminder_frequency,
        "DescriptOverride" as description_override,
        
        -- Boolean fields - using macro for 0/1 integers, direct cast for already boolean columns
        {{ convert_opendental_boolean('"IsNoteChange"') }} as is_note_change,
        {{ convert_opendental_boolean('"IsRepeating"') }} as is_repeating,
        "IsReadOnly"::boolean as is_read_only,  -- Already boolean type
        
        -- Standardized metadata using macro
        {{ standardize_metadata_columns(
            created_at_column='"DateTStamp"',
            updated_at_column='"SecDateTEdit"',
            created_by_column=none
        ) }}
        
    from source_data
)

select * from renamed_columns
