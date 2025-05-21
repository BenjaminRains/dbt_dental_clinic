{{
    config(
        materialized='view'
    )
}}

with entry_logs as (
    select
        entry_log_id,
        user_id,
        foreign_key_type,
        foreign_key,
        log_source,
        entry_datetime
    from {{ ref('stg_opendental__entrylog') }}
),

users as (
    select
        user_id,
        username,
        user_group_id
    from {{ ref('stg_opendental__userod') }}
),

tasks as (
    select
        task_id,
        task_list_id,
        description as task_description,
        task_date,
        task_status
    from {{ ref('stg_opendental__task') }}
)

select
    -- Entry Log Information
    el.entry_log_id,
    el.foreign_key_type,
    el.foreign_key,
    el.log_source,
    el.entry_datetime,
    
    -- User Information
    u.username,
    u.user_group_id,
    
    -- Task Information (when applicable)
    t.task_description,
    t.task_date,
    t.task_status

from entry_logs el
left join users u
    on el.user_id = u.user_id
left join tasks t
    on el.foreign_key_type = 'Task'
    and el.foreign_key = t.task_id::text 