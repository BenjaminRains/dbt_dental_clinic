with source as (
    select * from {{ source('opendental', 'task') }}
    where "DateTimeOriginal" >= '2023-01-01'
),

renamed as (
    select
        -- Primary Key
        "TaskNum" as task_id,
        
        -- Foreign Keys
        "TaskListNum" as task_list_id,
        "KeyNum" as key_id,
        "FromNum" as from_id,
        "UserNum" as user_id,
        "PriorityDefNum" as priority_def_id,
        "TriageCategory" as triage_category_id,
        
        -- Date and Timestamps
        "DateTask" as task_date,
        "DateTimeEntry" as entry_timestamp,
        "DateTimeFinished" as finished_timestamp,
        "DateTimeOriginal" as original_timestamp,
        "SecDateTEdit" as last_edit_timestamp,
        
        -- Attributes
        "Descript" as description,
        "TaskStatus" as task_status,
        "IsRepeating" as is_repeating,
        "DateType" as date_type,
        "ObjectType" as object_type,
        "ReminderGroupId" as reminder_group_id,
        "ReminderType" as reminder_type,
        "ReminderFrequency" as reminder_frequency,
        "DescriptOverride" as description_override,
        "IsReadOnly" as is_readonly,
        
        -- Required metadata columns
        current_timestamp as _loaded_at,
        "DateTimeEntry" as _created_at,
        "SecDateTEdit" as _updated_at
    from source
)

select * from renamed
