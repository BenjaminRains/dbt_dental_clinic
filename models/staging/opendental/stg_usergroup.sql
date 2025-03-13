with source as (
    select * from {{ source('opendental', 'usergroup') }}
),

renamed as (
    select
        -- Primary key
        UserGroupNum as user_group_id,
        
        -- Attributes
        Description as user_group_description,
        
        -- Relationships
        UserGroupNumCEMT as parent_user_group_id
    from source
)

select * from renamed