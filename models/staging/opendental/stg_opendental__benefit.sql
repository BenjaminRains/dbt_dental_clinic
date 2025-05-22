{{ config(
    materialized='view'
) }}

with source as (
    select * from {{ source('opendental', 'benefit') }}
    -- Removed date filter to include all benefits
    -- This ensures we have access to all benefits that might be referenced by other models
    -- Benefit status is tracked via patient_plan_id (0 for template benefits)
),

renamed as (
    select
        -- Primary Key
        "BenefitNum" as benefit_id,
        
        -- Foreign Keys
        "PlanNum" as insurance_plan_id,
        "PatPlanNum" as patient_plan_id,
        "CovCatNum" as coverage_category_id,
        "CodeNum" as procedure_code_id,
        "CodeGroupNum" as code_group_id,
        
        -- Benefit Details
        "BenefitType" as benefit_type,
        "Percent" as coverage_percent,
        "MonetaryAmt" as monetary_amount,
        "TimePeriod" as time_period,
        "QuantityQualifier" as quantity_qualifier,
        "Quantity" as quantity,
        "CoverageLevel" as coverage_level,
        "TreatArea" as treatment_area,
        
        -- Metadata columns
        current_timestamp as _loaded_at,
        "SecDateTEntry" as _created_at,
        "SecDateTEdit" as _updated_at

    from source
)

select * from renamed
