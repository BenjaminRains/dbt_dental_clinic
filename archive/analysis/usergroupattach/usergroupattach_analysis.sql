with source as (
    select * from {{ source('opendental', 'usergroupattach') }}
),

renamed as (
    select
        -- Primary key
        UserGroupAttachNum as user_group_attachment_id,
        
        -- Relationships
        UserNum as user_id,
        UserGroupNum as user_group_id
    from source
)

select * from renamed