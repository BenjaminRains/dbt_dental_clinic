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
)

select * from renamed
