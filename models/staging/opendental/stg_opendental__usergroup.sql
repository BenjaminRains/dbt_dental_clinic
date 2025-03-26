with source as (
    select * from {{ source('opendental', 'usergroup') }}
),

renamed as (
    select
        -- Primary Key
        "UserGroupNum" as usergroup_num,
        
        -- Attributes
        "Description" as description,
        "UserGroupNumCEMT" as usergroup_num_cemt,
        
        -- Metadata
        '{{ invocation_id }}' as _airbyte_ab_id,
        '{{ run_started_at }}' as _airbyte_emitted_at,
        current_timestamp as _airbyte_normalized_at
    from source
)

select * from renamed
