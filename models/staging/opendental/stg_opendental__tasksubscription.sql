with source as (
    select * from {{ source('opendental', 'tasksubscription') }}
),

renamed as (
    select
        -- Primary Key
        "TaskSubscriptionNum" as task_subscription_id,
        
        -- Foreign Keys
        "UserNum" as user_id,
        "TaskListNum" as task_list_id,
        "TaskNum" as task_id,
        
        -- Required metadata columns
        current_timestamp as _loaded_at,
        current_timestamp as _created_at,  -- Using current_timestamp as there's no creation timestamp in source
        current_timestamp as _updated_at   -- Using current_timestamp as there's no update timestamp in source
    from source
)

select * from renamed
