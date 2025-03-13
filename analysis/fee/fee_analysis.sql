with source as (
    select * from {{ source('opendental', 'fee') }}
),

renamed as (
    select
        -- Primary key
        FeeNum as fee_id,
        
        -- Relationships
        FeeSched as fee_schedule_id,
        CodeNum as procedure_code_id,
        ClinicNum as clinic_id,
        ProvNum as provider_id,
        
        -- Fee details
        Amount as fee_amount,
        OldCode as legacy_code,
        
        -- Configuration flags
        UseDefaultFee as use_default_fee,
        UseDefaultCov as use_default_coverage,
        
        -- Derived fields
        CASE
            WHEN UseDefaultFee = 1 THEN 'Default'
            ELSE 'Custom'
        END as fee_type,
        
        -- Metadata
        SecUserNumEntry as created_by_user_id,
        SecDateEntry as created_at,
        SecDateTEdit as updated_at
    from source
)

select * from renamed