with source as (
    select * from {{ source('opendental', 'tasksubscription') }}
),

renamed as (
    select
        "TaskSubscriptionNum" as task_subscription_id,
        "UserNum" as user_id,
        "TaskListNum" as task_list_id,
        "TaskNum" as task_id
    from source
)

select * from renamed
