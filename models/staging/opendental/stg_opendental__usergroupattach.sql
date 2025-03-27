with source as (
    select * from {{ source('opendental', 'usergroupattach') }}
),

renamed as (
    select
        -- Primary Key
        "UserGroupAttachNum" as user_group_attach_id,
        
        -- Foreign Keys
        "UserNum" as user_id,
        "UserGroupNum" as user_group_id,
        
        -- Metadata
        {{ current_timestamp() }} as _loaded_at

    from source
)

select * from renamed
