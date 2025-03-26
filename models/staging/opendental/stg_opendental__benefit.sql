{{ config(
    materialized='view'
) }}

with source as (
    select * from {{ source('opendental', 'benefit') }}
    where "SecDateTEdit" >= '2023-01-01'  -- Following pattern from inssub
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
        
        -- Meta Fields
        "SecDateTEntry" as created_at,
        "SecDateTEdit" as updated_at

    from source
)

select * from renamed
