with source as (
    select * from {{ source('opendental', 'benefit') }}
),

renamed as (
    select
        -- Keys
        BenefitNum as benefit_id,
        PlanNum as insurance_plan_id,
        PatPlanNum as patient_plan_id,
        CovCatNum as coverage_category_id,
        CodeNum as procedure_code_id,
        CodeGroupNum as code_group_id,
        -- Benefit details
        BenefitType as benefit_type,
        Percent as coverage_percent,
        MonetaryAmt as monetary_amount,
        TimePeriod as time_period,
        -- Limitations
        QuantityQualifier as quantity_qualifier,
        Quantity as quantity_limit,
        CoverageLevel as coverage_level,
        TreatArea as treatment_area,
        -- Metadata
        SecDateTEntry as created_at,
        SecDateTEdit as updated_at,
        -- Calculated fields
        CASE
            WHEN MonetaryAmt > 0 THEN 'monetary'
            WHEN Percent > 0 THEN 'percentage'
            ELSE 'other'
        END as benefit_calculation_type
    from source
)

select * from renamed