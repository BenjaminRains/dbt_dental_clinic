{{
    config(
        materialized='view'
    )
}}

with source as (
    select * from {{ source('opendental', 'taskhist') }}
    where "DateTStamp" >= '2023-01-01'
),

renamed as (
    select
        -- Primary Key
        "TaskHistNum" as task_hist_id,
        
        -- Foreign Keys
        "UserNumHist" as user_hist_id,
        "TaskNum" as task_id,
        "TaskListNum" as task_list_id,
        "KeyNum" as key_id,
        "FromNum" as from_id,
        "UserNum" as user_id,
        "PriorityDefNum" as priority_def_id,
        "TriageCategory" as triage_category_id,
        
        -- Attributes
        "DateTStamp" as timestamp,
        "IsNoteChange" as is_note_change,
        "DateTask" as task_date,
        "Descript" as description,
        "TaskStatus" as task_status,
        "IsRepeating" as is_repeating,
        "DateType" as date_type,
        "ObjectType" as object_type,
        "DateTimeEntry" as entry_datetime,
        "DateTimeFinished" as finished_datetime,
        "ReminderGroupId" as reminder_group_id,
        "ReminderType" as reminder_type,
        "ReminderFrequency" as reminder_frequency,
        "DateTimeOriginal" as original_datetime,
        "DescriptOverride" as description_override,
        "IsReadOnly" as is_read_only,
        
        -- Required metadata columns
        current_timestamp as _loaded_at,
        "DateTStamp" as _created_at,
        "SecDateTEdit" as _updated_at
    from source
)

select * from renamed
