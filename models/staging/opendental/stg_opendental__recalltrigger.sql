with source as (
    
    select * from {{ source('opendental', 'recalltrigger') }}

),

renamed as (
    
    select
        "RecallTriggerNum" as recall_trigger_id,
        "RecallTypeNum" as recall_type_id,
        "CodeNum" as code_id,
        
        -- Add metadata fields
        {{ current_timestamp() }} as loaded_at

    from source

)

select * from renamed
