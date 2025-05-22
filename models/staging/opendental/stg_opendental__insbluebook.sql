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
        "InsBlueBookNum" as insbluebook_id,
        
        -- Foreign Keys
        "ProcCodeNum" as proccode_id,
        "CarrierNum" as carrier_id,
        "PlanNum" as plan_id,
        "ProcNum" as proc_id,
        "ClaimNum" as claim_id,
        
        -- String Fields
        "GroupNum" as group_id,
        "ClaimType" as claim_type,
        
        -- Numeric Fields
        "InsPayAmt" as insurance_payment_amount,
        "AllowedOverride" as allowed_override_amount,
        
        -- Timestamps and Dates
        "DateTEntry" as created_at,
        "ProcDate" as procedure_date,
        
        -- Required Metadata Columns
        current_timestamp as _loaded_at,
        "DateTEntry" as _created_at,
        coalesce("DateTStamp", "DateTEntry") as _updated_at
    
    from source
)

select * from renamed
