{{ config(
    materialized='view'
) }}

with source_data as (
    select * from {{ source('opendental', 'insbluebook') }}
    where "DateTEntry" >= '2023-01-01'  -- Following pattern from benefit
),

renamed_columns as (
    select
        -- ID Columns (with safe conversion)
        {{ transform_id_columns([
            {'source': '"InsBlueBookNum"', 'target': 'insbluebook_id'},
            {'source': '"ProcCodeNum"', 'target': 'procedure_code_id'},
            {'source': '"CarrierNum"', 'target': 'carrier_id'},
            {'source': '"PlanNum"', 'target': 'plan_id'},
            {'source': '"ProcNum"', 'target': 'procedure_id'},
            {'source': '"ClaimNum"', 'target': 'claim_id'}
        ]) }},
        
        -- String Fields
        "GroupNum" as group_number,
        "ClaimType" as claim_type,
        
        -- Numeric Fields
        "InsPayAmt" as insurance_payment_amount,
        "AllowedOverride" as allowed_override_amount,
        
        -- Date Fields
        {{ clean_opendental_date('"DateTEntry"') }} as date_created,
        {{ clean_opendental_date('"ProcDate"') }} as procedure_date,
        
        -- Standardized metadata columns
        {{ standardize_metadata_columns(created_at_column='"DateTEntry"') }}
    
    from source_data
)

select * from renamed_columns
