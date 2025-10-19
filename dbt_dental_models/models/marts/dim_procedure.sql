{{
    config(
        materialized='table',
        schema='marts',
        unique_key='procedure_code_id',
        on_schema_change='fail',
        indexes=[
            {'columns': ['procedure_code_id'], 'unique': true},
            {'columns': ['_updated_at']},
            {'columns': ['procedure_code']},
            {'columns': ['procedure_category']},
            {'columns': ['is_hygiene']},
            {'columns': ['is_prosthetic']},
            {'columns': ['is_radiology']}
        ]
    )
}}

/*
Dimension model for dental procedures and treatment codes.
Part of System A: Fee Processing and Procedure Management

This model:
1. Provides comprehensive procedure code information for all clinical and billing analytics
2. Serves as the business-ready dimension for procedure analysis and reporting
3. Integrates all procedure attributes, categorizations, and fee information

Business Logic Features:
- Clinical categorization based on CDT codes (centralized in intermediate)
- Complexity analysis for treatment planning and resource allocation
- Revenue tier classification for financial analysis
- Fee integration with statistical analysis for pricing optimization
- Treatment planning categorization for clinical workflow

Key Metrics:
- procedure_category: Clinical specialty classification (Preventive, Restorative, etc.)
- complexity_level: Procedure complexity (Simple, Moderate, Complex)
- revenue_tier: Financial impact classification (Low, Medium, High)
- clinical_urgency: Treatment priority level (Low, Medium, High)
- insurance_complexity: Billing complexity assessment (Low, Medium, High)

Data Quality Notes:
- Business logic centralized in int_procedure_catalog for consistency
- Some procedures may not have standard fees (flagged with has_standard_fee)
- Fee statistics calculated from all available fee records

Performance Considerations:
- Indexed on procedure_code_id for fast lookups
- Indexed on procedure_code for code-based queries
- Indexed on procedure_category for specialty filtering
- Indexed on clinical flags for procedure type filtering
- Table materialization for optimal query performance

Dependencies:
- int_procedure_catalog: Comprehensive procedure catalog with all business logic
*/

-- 1. Source data retrieval from intermediate layer
with source_procedure_catalog as (
    select * from {{ ref('int_procedure_catalog') }}
),

-- 2. Final mart selection (business logic already in intermediate)
final as (
    select
        -- Primary identification
        procedure_code_id,
        
        -- Core attributes
        procedure_code,
        procedure_description as description,
        abbreviated_description,
        procedure_category_id,
        
        -- Treatment area information
        treatment_area,
        treatment_area_description as treatment_area_desc,
        
        -- Business categorizations (from intermediate)
        procedure_category,
        complexity_level,
        revenue_tier,
        clinical_urgency,
        insurance_complexity,
        treatment_planning_category,
        
        -- Clinical flags
        is_hygiene,
        is_prosthetic,
        is_radiology,
        is_multi_visit,
        
        -- Clinical metrics
        base_units,
        default_provider_id,
        default_revenue_code,
        
        -- Fee information (from intermediate)
        standard_fee_id,
        fee_schedule_id,
        standard_fee,
        fee_schedule_description,
        fee_schedule_type_id,
        fee_type_description as fee_schedule_type_desc,
        
        -- Fee statistics (from intermediate)
        available_fee_options,
        min_available_fee,
        max_available_fee,
        avg_fee_amount,
        
        -- Fee validation flags (from intermediate)
        has_standard_fee,
        has_multiple_fees,
        
        -- Documentation
        default_claim_note,
        default_treatment_plan_note,
        layman_term,
        medical_code,
        diagnostic_codes,
        
        -- Standardized mart metadata
        {{ standardize_mart_metadata(
            primary_source_alias='pc',
            source_metadata_fields=['_loaded_at', '_created_at', '_updated_at', '_transformed_at']
        ) }}
        
    from source_procedure_catalog pc
)

select * from final
