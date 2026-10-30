with source as (
    select * from {{ source('opendental', 'recalltype') }}
),

renamed as (
    select
        -- Primary key
        RecallTypeNum as recall_type_id,

        -- Description
        Description as recall_type_description,

        -- Configuration
        DefaultInterval as default_interval_days,
        TimePattern as time_pattern,
        Procedures as procedure_codes,

        -- Special handling
        AppendToSpecial as append_to_special_flag
    from source
)

select * from renamed