with source as (
    select * from {{ source('opendental', 'feesched') }}
),

renamed as (
    select
        -- Primary key
        FeeSchedNum as fee_schedule_id,
        
        -- Description
        Description as fee_schedule_name,
        
        -- Configuration
        FeeSchedType as fee_schedule_type,
        ItemOrder as display_order,
        IsHidden as is_hidden,
        IsGlobal as is_global,
        
        -- Derived field
        CASE
            WHEN IsHidden = 1 THEN 'Hidden'
            ELSE 'Active'
        END as status,
        
        -- Metadata
        SecUserNumEntry as created_by_user_id,
        SecDateEntry as created_at,
        SecDateTEdit as updated_at
    from source
)

select * from renamed