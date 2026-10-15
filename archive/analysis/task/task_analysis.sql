with source as (
    select * from {{ source('opendental', 'task') }}
),

renamed as (
    select
        -- Primary key
        TaskNum as task_id,

        -- Relationships
        TaskListNum as task_list_id,
        UserNum as user_id,
        PriorityDefNum as priority_definition_id,
        KeyNum as key_id,
        FromNum as from_id,
        TriageCategory as triage_category_id,

        -- Task configuration
        TaskStatus as task_status,
        ObjectType as object_type,
        DateType as date_type,
        IsRepeating as is_repeating_flag,
        IsReadOnly as is_readonly_flag,

        -- Reminder settings
        ReminderGroupId as reminder_group_id,
        ReminderType as reminder_type,
        ReminderFrequency as reminder_frequency,

        -- Dates
        DateTask as task_date,
        DateTimeEntry as entry_datetime,
        DateTimeFinished as finished_datetime,
        DateTimeOriginal as original_datetime,
        SecDateTEdit as updated_at,

        -- Description (may contain PHI)
        Descript as task_description,
        DescriptOverride as description_override
    from source
)

select * from renamed