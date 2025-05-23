{{ config(
    materialized='view'
) }}

with source as (
    select * from {{ source('opendental', 'taskunread') }}
),

renamed as (
    select
        -- Primary Key
        "TaskUnreadNum" as task_unread_id,
        
        -- Foreign Keys
        "TaskNum" as task_id,
        "UserNum" as user_id,
        
        -- Required metadata columns
        current_timestamp as _loaded_at,
        current_timestamp as _created_at,  -- Using current_timestamp as there's no creation timestamp in source
        current_timestamp as _updated_at   -- Using current_timestamp as there's no update timestamp in source
    from source
),

filtered as (
    select r.*
    from renamed r
    inner join public.task t
        on r.task_id = t."TaskNum"
    where t."DateTimeOriginal" >= '2023-01-01'
)

select * from filtered
