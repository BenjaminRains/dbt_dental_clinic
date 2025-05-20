{{
    config(
        materialized='view'
    )
}}

with task_base as (
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
        last_edit_timestamp
    from {{ ref('stg_opendental__task') }}
),

task_list as (
    select
        task_id as task_list_id,
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
    from {{ ref('stg_opendental__tasklist') }}
),

task_history as (
    select
        task_hist_id,
        task_id,
        task_list_id as hist_task_list_id,
        user_hist_id,
        key_id as hist_key_id,
        from_id as hist_from_id,
        user_id as hist_user_id,
        priority_def_id as hist_priority_def_id,
        triage_category_id as hist_triage_category_id,
        timestamp as history_timestamp,
        is_note_change,
        task_date as hist_task_date,
        description as hist_description,
        task_status as hist_task_status,
        is_repeating as hist_is_repeating,
        date_type as hist_date_type,
        object_type as hist_object_type,
        entry_datetime as hist_entry_datetime,
        finished_datetime as hist_finished_datetime,
        reminder_group_id,
        reminder_type,
        reminder_frequency,
        original_datetime as hist_original_datetime,
        description_override,
        is_read_only
    from {{ ref('stg_opendental__taskhist') }}
),

task_notes as (
    select
        task_note_id,
        task_id,
        user_id as note_user_id,
        note_datetime,
        note
    from {{ ref('stg_opendental__tasknote') }}
),

task_subscriptions as (
    select
        task_subscription_id,
        user_id as subscriber_user_id,
        task_list_id as subscription_task_list_id,
        task_id as subscription_task_id
    from {{ ref('stg_opendental__tasksubscription') }}
),

task_unread as (
    select
        task_unread_id,
        task_id as unread_task_id,
        user_id as unread_user_id
    from {{ ref('stg_opendental__taskunread') }}
)

select
    -- Task Base Information
    t.task_id,
    t.task_list_id,
    t.key_id,
    t.from_id,
    t.user_id as assigned_user_id,
    t.priority_def_id,
    t.triage_category_id,
    t.task_date,
    t.entry_timestamp,
    t.finished_timestamp,
    t.original_timestamp,
    t.last_edit_timestamp,
    
    -- Task List Information
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
    
    -- Latest Task History
    th.task_hist_id as latest_history_id,
    th.history_timestamp as latest_history_timestamp,
    th.hist_task_status as latest_task_status,
    th.hist_description as latest_description,
    th.hist_is_repeating as latest_is_repeating,
    th.hist_date_type as latest_date_type,
    th.hist_object_type as latest_object_type,
    th.hist_entry_datetime as latest_entry_datetime,
    th.hist_finished_datetime as latest_finished_datetime,
    th.reminder_group_id,
    th.reminder_type,
    th.reminder_frequency,
    th.hist_original_datetime as latest_original_datetime,
    th.description_override as latest_description_override,
    th.is_read_only as latest_is_read_only,
    
    -- Latest Task Note
    tn.task_note_id as latest_note_id,
    tn.note_datetime as latest_note_datetime,
    tn.note as latest_note,
    
    -- Subscription Information
    ts.task_subscription_id,
    ts.subscriber_user_id,
    
    -- Unread Status
    tu.task_unread_id,
    tu.unread_user_id

from task_base t
left join task_list tl
    on t.task_list_id = tl.task_list_id
left join task_history th
    on t.task_id = th.task_id
    and th.history_timestamp = (
        select max(history_timestamp)
        from task_history th2
        where th2.task_id = t.task_id
    )
left join task_notes tn
    on t.task_id = tn.task_id
    and tn.note_datetime = (
        select max(note_datetime)
        from task_notes tn2
        where tn2.task_id = t.task_id
    )
left join task_subscriptions ts
    on t.task_id = ts.subscription_task_id
left join task_unread tu
    on t.task_id = tu.unread_task_id 