{{ config(
    materialized='incremental',
    unique_key='task_id'
) }}

with source_data as (
    select * from {{ source('opendental', 'task') }}
    where "DateTimeOriginal" >= '2023-01-01'
    {% if is_incremental() %}
        and {{ clean_opendental_date('"SecDateTEdit"') }} > (select max(_loaded_at) from {{ this }})
    {% endif %}
),

renamed_columns as (
    select
        -- Primary and Foreign Key transformations using macro
        {{ transform_id_columns([
            {'source': '"TaskNum"', 'target': 'task_id'},
            {'source': 'NULLIF("TaskListNum", 0)', 'target': 'task_list_id'},
            {'source': 'NULLIF("KeyNum", 0)', 'target': 'key_id'},
            {'source': 'NULLIF("FromNum", 0)', 'target': 'from_id'},
            {'source': 'NULLIF("UserNum", 0)', 'target': 'user_id'},
            {'source': 'NULLIF("PriorityDefNum", 0)', 'target': 'priority_def_id'},
            {'source': 'NULLIF("TriageCategory", 0)', 'target': 'triage_category_id'}
        ]) }},
        
        -- Date and Timestamps using macro
        {{ clean_opendental_date('"DateTask"') }} as task_date,
        {{ clean_opendental_date('"DateTimeEntry"') }} as entry_timestamp,
        {{ clean_opendental_date('"DateTimeFinished"') }} as finished_timestamp,
        {{ clean_opendental_date('"DateTimeOriginal"') }} as original_timestamp,
        {{ clean_opendental_date('"SecDateTEdit"') }} as last_edit_timestamp,
        
        -- Task attributes
        "Descript" as description,
        "TaskStatus" as task_status,
        {{ convert_opendental_boolean('"DateType"') }} as date_type,
        "ObjectType" as object_type,
        "ReminderGroupId" as reminder_group_id,
        "ReminderType" as reminder_type,
        "ReminderFrequency" as reminder_frequency,
        "DescriptOverride" as description_override,
        
        -- Boolean fields - using macro for 0/1 integers, direct cast for already boolean columns
        {{ convert_opendental_boolean('"IsRepeating"') }} as is_repeating,
        "IsReadOnly"::boolean as is_readonly,  -- Already boolean type
        
        -- Standardized metadata using macro
        {{ standardize_metadata_columns(
            created_at_column='"DateTimeEntry"',
            updated_at_column='"SecDateTEdit"',
            created_by_column=none
        ) }}
        
    from source_data
)

select * from renamed_columns
