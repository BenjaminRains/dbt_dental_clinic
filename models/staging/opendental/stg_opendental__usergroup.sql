with source as (
    select * from {{ source('opendental', 'usergroup') }}
),

renamed as (
    select
        -- Primary Key
        "UserGroupNum" as usergroup_id,
        
        -- Attributes
        "Description" as description,
        "UserGroupNumCEMT" as usergroup_num_cemt,
        
        -- Required metadata columns
        current_timestamp as _loaded_at,  -- When ETL pipeline loaded the data
        current_timestamp as _created_at, -- Since no creation timestamp exists in source
        current_timestamp as _updated_at  -- Since no update timestamp exists in source
    from source
)

select * from renamed
