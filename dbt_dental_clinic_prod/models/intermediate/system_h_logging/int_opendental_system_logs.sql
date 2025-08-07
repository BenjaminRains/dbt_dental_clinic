{{ config(materialized='table', database='opendental_analytics', tags=['intermediate', 'system_logging']) }}

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
)

select
    -- Entry Log Information
    el.entry_log_id,
    el.user_id,
    el.foreign_key_type,
    el.foreign_key,
    el.log_source,
    el.entry_datetime,
    
    -- User Information
    u.username,
    COALESCE(u.user_group_id, 0) as user_group_id  -- Default to 0 for system entries

from entry_logs el
left join users u
    on el.user_id = u.user_id

/*
Log Source Values:
- 0: Main system log (98.15% of records)
- 7: Specific module (1.48% of records)
- 16: Rare events (0.02% of records)
- 19: Another specific module (0.35% of records)

Note: All records have foreign_key_type = 0, indicating they are system entries.
The differentiation between different types of entries is handled by log_source.
*/ 
