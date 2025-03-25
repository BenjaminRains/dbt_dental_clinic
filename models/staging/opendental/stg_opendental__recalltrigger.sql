with source as (
    
    select * from {{ source('opendental', 'recalltrigger') }}

),

renamed as (
    
    select
        "RecallTriggerNum" as recall_trigger_num,
        "RecallTypeNum" as recall_type_num,
        "CodeNum" as code_num,
        
        -- Add metadata fields
        {{ current_timestamp() }} as loaded_at

    from source

)

select * from renamed
