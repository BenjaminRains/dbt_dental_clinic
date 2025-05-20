{{ config(
    materialized='view'
) }}

with source as (
    select * from {{ source('opendental', 'taskunread') }}
),

renamed as (
    select
        "TaskUnreadNum" as task_unread_id,
        "TaskNum" as task_id,
        "UserNum" as user_id
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
