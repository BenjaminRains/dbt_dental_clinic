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
        
        -- Required metadata columns
        current_timestamp as _loaded_at,  -- When ETL pipeline loaded the data
        current_timestamp as _created_at, -- Since no creation timestamp exists in source
        current_timestamp as _updated_at  -- Since no update timestamp exists in source

    from source
)

select * from renamed
