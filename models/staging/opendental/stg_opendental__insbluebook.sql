{{ config(
    materialized='view'
) }}

with source as (
    select * from {{ source('opendental', 'insbluebook') }}
    where "DateTEntry" >= '2023-01-01'  -- Following pattern from benefit
),

renamed as (
    select
        -- Primary Key
        "InsBlueBookNum" as insbluebook_num,
        
        -- Foreign Keys
        "ProcCodeNum" as proccode_num,
        "CarrierNum" as carrier_num,
        "PlanNum" as plan_num,
        "ProcNum" as proc_num,
        "ClaimNum" as claim_num,
        
        -- String Fields
        "GroupNum" as group_num,
        "ClaimType" as claim_type,
        
        -- Numeric Fields
        "InsPayAmt" as insurance_payment_amount,
        "AllowedOverride" as allowed_override_amount,
        
        -- Timestamps and Dates
        "DateTEntry" as created_at,
        "ProcDate" as procedure_date
    
    from source
)

select * from renamed
