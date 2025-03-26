with source as (
    select * from {{ source('opendental', 'usergroupattach') }}
),

renamed as (
    select
        -- Primary Key
        "UserGroupAttachNum" as user_group_attach_num,
        
        -- Foreign Keys
        "UserNum" as user_num,
        "UserGroupNum" as user_group_num,
        
        -- Metadata
        {{ current_timestamp() }} as _loaded_at

    from source
)

select * from renamed
