with source as (
    select * from {{ source('opendental', 'recalltrigger') }}
),

renamed as (
    select
        -- Primary key
        RecallTriggerNum as recall_trigger_id,

        -- Relationships
        RecallTypeNum as recall_type_id,
        CodeNum as procedure_code_id
    from source
)

select * from renamed