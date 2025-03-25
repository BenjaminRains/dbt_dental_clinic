with source as (
    select * from {{ source('opendental', 'recalltype') }}
),

renamed as (
    select
        "RecallTypeNum" as recall_type_id,
        "Description" as description,
        "DefaultInterval" as default_interval,
        "TimePattern" as time_pattern,
        "Procedures" as procedures,
        "AppendToSpecial" as append_to_special,
        
        -- Meta fields
        current_timestamp as loaded_at
    
    from source
)

select * from renamed
