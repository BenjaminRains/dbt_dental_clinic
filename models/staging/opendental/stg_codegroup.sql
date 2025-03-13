with source as (
    select * from {{ source('opendental', 'codegroup') }}
),

renamed as (
    select
        -- Primary key
        CodeGroupNum as code_group_id,
        
        -- Group details
        GroupName as group_name,
        ProcCodes as procedure_codes,
        
        -- Configuration
        ItemOrder as display_order,
        CodeGroupFixed as is_fixed_group,
        IsHidden as is_hidden,
        ShowInAgeLimit as show_in_age_limit
    from source
)

select * from renamed