{{ config(
    materialized='view',
    schema='staging'
) }}

with source_data as (
    select * from {{ source('opendental', 'benefit') }}
    -- Removed date filter to include all benefits
    -- This ensures we have access to all benefits that might be referenced by other models
    -- Benefit status is tracked via patient_plan_id (0 for template benefits)
),

renamed_columns as (
    select
        -- Primary Key
        {{ transform_id_columns([
            {'source': '"BenefitNum"', 'target': 'benefit_id'}
        ]) }},
        
        -- Foreign Keys
        {{ transform_id_columns([
            {'source': '"PlanNum"', 'target': 'insurance_plan_id'},
            {'source': '"PatPlanNum"', 'target': 'patient_plan_id'},
            {'source': '"CovCatNum"', 'target': 'coverage_category_id'},
            {'source': '"CodeNum"', 'target': 'procedure_code_id'},
            {'source': '"CodeGroupNum"', 'target': 'code_group_id'}
        ]) }},
        
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
        {{ standardize_metadata_columns(
            created_at_column='"SecDateTEntry"',
            updated_at_column='"SecDateTEdit"',
            created_by_column=none
        ) }}

    from source_data
)

select * from renamed_columns
