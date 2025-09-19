{{
    config(
        materialized='incremental',
        schema='intermediate',
        unique_key='task_id',
        on_schema_change='fail',
        incremental_strategy='merge',
        indexes=[
            {'columns': ['task_id'], 'unique': true},
            {'columns': ['appointment_patient_id']},
            {'columns': ['appointment_id']},
            {'columns': ['assigned_user_id']},
            {'columns': ['_updated_at']}
        ]
    )
}}

/*
    Intermediate model for task management.
    Part of System G: Scheduling
    
    Systems Integration:
    - System G: Scheduling - Task management and workflow coordination
    - System F: Communications - Task-based communication triggers
    - System A: Fee Processing - Task integration with billing workflows
    
    This model:
    1. Provides comprehensive task management with history tracking
    2. Integrates task data with appointment scheduling system
    3. Manages task subscriptions and notification status
    4. Tracks task completion and due date management
    5. Supports task categorization and priority management
    
    Business Logic Features:
    - Task Type Classification: Categorizes tasks by object type (appointment, patient, lab, insurance, referral)
    - Due Date Management: Identifies tasks due within 24 hours for priority handling
    - Status Tracking: Maintains latest task status and history for audit trails
    - Notification Management: Tracks unread status and subscriptions for user notifications
    - Appointment Integration: Links tasks to specific appointments for scheduling context
    
    Data Quality Notes:
    - Task history may have gaps for older tasks - latest status is preserved
    - Some task notes may be missing for historical tasks
    - Appointment links only available for object_type = 1 tasks
    - Tasks with date_type = 0 may legitimately have null task_date and triage_category_id
    
    Performance Considerations:
    - Uses incremental materialization with metadata-based filtering (_updated_at)
    - Complex joins optimized with proper indexing on key fields
    - Array aggregation for unread status to minimize row count
    - Latest history/notes retrieved via window functions for efficiency
*/

-- 1. Source CTEs (multiple sources)
with source_task as (
    select * from {{ ref('stg_opendental__task') }}
    {% if is_incremental() %}
    where _updated_at > (select max(_updated_at) from {{ this }})
    {% endif %}
),

source_appointment as (
    select * from {{ ref('stg_opendental__appointment') }}
),

source_tasklist as (
    select * from {{ ref('stg_opendental__tasklist') }}
),

source_taskhist as (
    select * from {{ ref('stg_opendental__taskhist') }}
),

source_tasknote as (
    select * from {{ ref('stg_opendental__tasknote') }}
),

source_tasksubscription as (
    select * from {{ ref('stg_opendental__tasksubscription') }}
),

source_taskunread as (
    select * from {{ ref('stg_opendental__taskunread') }}
),

-- 2. Business Logic CTEs
task_categorization as (
    select
        task_id,
        task_list_id,
        key_id,
        from_id,
        user_id,
        priority_def_id,
        triage_category_id,
        task_date,
        entry_timestamp,
        finished_timestamp,
        original_timestamp,
        last_edit_timestamp,
        -- Task type classification based on object_type
        case
            when object_type = 1 then 'Appointment'
            when object_type = 2 then 'Patient'
            when object_type = 3 then 'Lab Case'
            when object_type = 4 then 'Insurance'
            when object_type = 5 then 'Referral'
            else 'Other'
        end as task_type,
        object_type,
        -- Metadata columns from staging model
        _loaded_at,
        _created_at,
        _updated_at,
        _transformed_at
    from source_task
),

task_validation as (
    select
        *,
        -- Due date management - tasks due within 24 hours
        case
            when task_date <= current_timestamp + interval '24 hours'
            and finished_timestamp is null
            then true
            else false
        end as is_due_soon,
        -- Task completion status
        case
            when finished_timestamp is not null then true
            else false
        end as is_completed,
        -- Overdue flag - tasks past due date and not completed
        case
            when task_date < current_timestamp
            and finished_timestamp is null
            then true
            else false
        end as is_overdue
    from task_categorization
),

-- 3. Lookup/Reference CTEs
appointment_tasks as (
    select
        t.task_id,
        t.key_id as appointment_id,
        a.appointment_datetime,
        a.appointment_status,
        a.provider_id,
        a.patient_id
    from source_task t
    inner join source_appointment a
        on t.key_id = a.appointment_id
    where t.object_type = 1  -- 1 represents appointment tasks
),

task_list_lookup as (
    select
        task_list_id,
        description as task_list_description,
        parent_id as parent_task_list_id,
        task_date as task_list_date,
        is_repeating,
        date_type,
        from_id as task_list_from_id,
        object_type,
        entry_datetime as task_list_entry_datetime,
        global_task_filter_type,
        task_status as task_list_status
    from source_tasklist
),

-- 4. Aggregation CTEs
latest_task_history as (
    select
        task_id,
        max(timestamp) as latest_history_timestamp
    from source_taskhist
    group by task_id
),

latest_task_notes as (
    select
        task_id,
        max(note_datetime) as latest_note_datetime
    from source_tasknote
    group by task_id
),

task_unread_aggregated as (
    select
        task_id as unread_task_id,
        array_agg(user_id) as unread_user_ids,
        array_agg(task_unread_id) as task_unread_ids
    from source_taskunread
    group by task_id
),

-- 5. Integration CTE (joins everything together)
task_integrated as (
    select
        -- Core task fields
        tv.task_id,
        tv.task_list_id,
        tv.key_id,
        tv.from_id,
        tv.user_id as assigned_user_id,
        tv.priority_def_id,
        tv.triage_category_id,
        tv.task_date,
        tv.entry_timestamp,
        tv.finished_timestamp,
        tv.original_timestamp,
        tv.last_edit_timestamp,
        tv.task_type,
        tv.is_due_soon,
        tv.is_completed,
        tv.is_overdue,
        
        -- Appointment task information (if applicable)
        at.appointment_id,
        at.appointment_datetime,
        at.appointment_status,
        at.provider_id as appointment_provider_id,
        at.patient_id as appointment_patient_id,
        
        -- Task list information
        tl.task_list_description,
        tl.parent_task_list_id,
        tl.task_list_date,
        tl.is_repeating,
        tl.date_type,
        tl.task_list_from_id,
        tl.object_type,
        tl.task_list_entry_datetime,
        tl.global_task_filter_type,
        tl.task_list_status,
        
        -- Latest task history
        th.task_hist_id as latest_history_id,
        th.timestamp as latest_history_timestamp,
        th.task_status as latest_task_status,
        th.description as latest_description,
        th.is_repeating as latest_is_repeating,
        th.date_type as latest_date_type,
        th.object_type as latest_object_type,
        th.entry_datetime as latest_entry_datetime,
        th.finished_datetime as latest_finished_datetime,
        th.reminder_group_id,
        th.reminder_type,
        th.reminder_frequency,
        th.original_datetime as latest_original_datetime,
        th.description_override as latest_description_override,
        th.is_read_only as latest_is_read_only,
        
        -- Latest task note
        tn.task_note_id as latest_note_id,
        tn.note_datetime as latest_note_datetime,
        tn.note as latest_note,
        tn.user_id as latest_note_user_id,
        
        -- Subscription information
        ts.task_subscription_id,
        ts.user_id as subscriber_user_id,
        
        -- Unread status (as arrays)
        tu.task_unread_ids,
        tu.unread_user_ids,
        
        -- Standardized metadata (manually added from staging model)
        tv._loaded_at,
        tv._created_at,
        tv._updated_at,
        current_timestamp as _transformed_at
        
    from task_validation tv
    left join appointment_tasks at
        on tv.task_id = at.task_id
    left join task_list_lookup tl
        on tv.task_list_id = tl.task_list_id
    left join latest_task_history lth
        on tv.task_id = lth.task_id
    left join source_taskhist th
        on tv.task_id = th.task_id
        and th.timestamp = lth.latest_history_timestamp
    left join latest_task_notes ltn
        on tv.task_id = ltn.task_id
    left join source_tasknote tn
        on tv.task_id = tn.task_id
        and tn.note_datetime = ltn.latest_note_datetime
    left join source_tasksubscription ts
        on tv.task_id = ts.task_id
    left join task_unread_aggregated tu
        on tv.task_id = tu.unread_task_id
),

-- 6. Final filtering/validation
final as (
    select * from task_integrated
    where task_id is not null  -- Ensure we have valid task records
)

select
    -- Core task fields
    task_id,
    task_list_id,
    key_id,
    from_id,
    assigned_user_id,
    priority_def_id,
    triage_category_id,
    task_date,
    entry_timestamp,
    finished_timestamp,
    original_timestamp,
    last_edit_timestamp,
    task_type,
    is_due_soon,
    is_completed,
    is_overdue,
    
    -- Appointment task information (if applicable)
    appointment_id,
    appointment_datetime,
    appointment_status,
    appointment_provider_id,
    appointment_patient_id,
    
    -- Task list information
    task_list_description,
    parent_task_list_id,
    task_list_date,
    is_repeating,
    date_type,
    task_list_from_id,
    object_type,
    task_list_entry_datetime,
    global_task_filter_type,
    task_list_status,
    
    -- Latest task history
    latest_history_id,
    latest_history_timestamp,
    latest_task_status,
    latest_description,
    latest_is_repeating,
    latest_date_type,
    latest_object_type,
    latest_entry_datetime,
    latest_finished_datetime,
    reminder_group_id,
    reminder_type,
    reminder_frequency,
    latest_original_datetime,
    latest_description_override,
    latest_is_read_only,
    
    -- Latest task note
    latest_note_id,
    latest_note_datetime,
    latest_note,
    latest_note_user_id,
    
    -- Subscription information
    task_subscription_id,
    subscriber_user_id,
    
    -- Unread status (as arrays)
    task_unread_ids,
    unread_user_ids,
    
    -- Standardized metadata (passed through from task_integrated CTE)
    _loaded_at,
    _created_at,
    _updated_at,
    _transformed_at

from final 
