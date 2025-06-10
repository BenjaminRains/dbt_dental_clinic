{{ config(
    materialized='view'
) }}

with source_data as (
    select * from {{ source('opendental', 'insbluebook') }}
    where "DateTEntry" >= '2023-01-01'  -- Following pattern from benefit
),

renamed_columns as (
    select
        -- Primary Key
        "InsBlueBookNum" as insbluebook_id,
        
        -- Foreign Keys
        "ProcCodeNum" as procedure_code_id,
        "CarrierNum" as carrier_id,
        "PlanNum" as plan_id,
        "ProcNum" as procedure_id,
        "ClaimNum" as claim_id,
        
        -- String Fields
        "GroupNum" as group_id,
        "ClaimType" as claim_type,
        
        -- Numeric Fields
        "InsPayAmt" as insurance_payment_amount,
        "AllowedOverride" as allowed_override_amount,
        
        -- Date Fields
        {{ clean_opendental_date('"DateTEntry"') }} as date_created,
        {{ clean_opendental_date('"ProcDate"') }} as procedure_date,
        
        -- Standardized metadata columns
        {{ standardize_metadata_columns(
            created_at_column='"DateTEntry"',
            updated_at_column='"DateTEntry"',
            created_by_column=none
        ) }}
    
    from source_data
)

select * from renamed_columns
