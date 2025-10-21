{{ config(
    materialized='table',
    schema='intermediate',
    unique_key='procedure_code_id',
    on_schema_change='fail',
    indexes=[
        {'columns': ['procedure_code_id'], 'unique': true},
        {'columns': ['procedure_code']},
        {'columns': ['procedure_category']},
        {'columns': ['is_hygiene']},
        {'columns': ['is_prosthetic']},
        {'columns': ['_updated_at']}
    ],
    tags=['foundation', 'weekly']) }}

/*
    Intermediate model for procedure code catalog
    Part of System A: Fee Processing and Procedure Management
    
    This model:
    1. Provides comprehensive procedure code catalog with all available procedures
    2. Integrates fee information with statistical analysis for pricing
    3. Applies clinical categorization and complexity analysis
    4. Enriches with definition lookups for treatment areas and fee types
    5. Serves as foundation for all procedure-related downstream analytics
    
    Business Logic Features:
    - Clinical Categorization: Based on CDT codes (D1=Preventive, D2=Restorative, etc.)
    - Complexity Analysis: Simple, Moderate, Complex based on base_units and multi-visit
    - Revenue Tier Classification: High, Medium, Low based on procedure category
    - Clinical Urgency: Treatment priority levels for workflow management
    - Insurance Complexity: Billing complexity assessment for claims processing
    - Treatment Planning: Long-term, Medium-term, Short-term categorization
    
    Data Sources:
    - stg_opendental__procedurecode: Procedure code definitions and attributes
    - stg_opendental__fee: Fee amounts and pricing data
    - stg_opendental__feesched: Fee schedule metadata and types
    - stg_opendental__definition: Treatment area and fee type descriptions
    
    Data Quality Notes:
    - Some procedures may not have standard fees (NULL handling included)
    - Definition lookups use category_id 5 (treatment areas) and 6 (fee types)
    - Fee statistics calculated from all available fee records
    - Most recent fee selected per procedure code and clinic combination
    
    Performance Considerations:
    - Table materialization (procedure catalog changes infrequently)
    - Indexed on procedure_code_id, procedure_code, and procedure_category
    - Weekly refresh cycle appropriate for procedure catalog updates
    - Fee statistics pre-calculated for mart efficiency
*/

-- 1. Source data retrieval
with source_procedure as (
    select * from {{ ref('stg_opendental__procedurecode') }}
),

-- 2. Fee schedule lookup
fee_schedule_lookup as (
    select
        fee_schedule_id,
        fee_schedule_description,
        fee_schedule_type_id,
        is_hidden
    from {{ ref('stg_opendental__feesched') }}
),

-- 3. Definition lookups
definition_lookup as (
    select
        definition_id,
        category_id,
        item_name,
        item_value,
        item_order,
        item_color
    from {{ ref('stg_opendental__definition') }}
),

treatment_area_definitions as (
    select
        -- Cast to smallint to match procedurecode.treatment_area type
        case 
            when item_value ~ '^[0-9]+$' then item_value::smallint
            else null
        end as treatment_area_id,
        item_name as treatment_area_description
    from definition_lookup
    where category_id = 5  -- Treatment areas
),

fee_type_definitions as (
    select
        -- Cast to bigint to match feesched.fee_schedule_type_id type
        case 
            when item_value ~ '^[0-9]+$' then item_value::bigint
            else null
        end as fee_type_id,
        item_name as fee_type_description
    from definition_lookup
    where category_id = 6  -- Fee schedule types
),

-- 4. Fee information with ranking (most recent per procedure and clinic)
procedure_fees as (
    select
        procedure_code_id,
        fee_id,
        fee_schedule_id,
        clinic_id,
        provider_id,
        fee_amount,
        _created_at,
        row_number() over (
            partition by procedure_code_id, clinic_id
            order by _created_at desc
        ) as fee_rank
    from {{ ref('stg_opendental__fee') }}
),

-- 5. Fee statistics per procedure code
fee_statistics as (
    select
        procedure_code_id,
        count(distinct fee_id) as available_fee_options,
        min(fee_amount) as min_available_fee,
        max(fee_amount) as max_available_fee,
        avg(fee_amount) as avg_fee_amount,
        stddev(fee_amount) as fee_std_dev
    from {{ ref('stg_opendental__fee') }}
    group by procedure_code_id
),

-- 6. Business logic transformation - Clinical categorization
procedure_categorized as (
    select
        sp.*,
        
        -- Clinical categorization based on CDT codes
        case
            when sp.procedure_code like 'D1%' then 'Preventive'
            when sp.procedure_code like 'D2%' then 'Restorative'
            when sp.procedure_code like 'D3%' then 'Endodontics'
            when sp.procedure_code like 'D4%' then 'Periodontics'
            when sp.procedure_code like 'D5%' then 'Prosthodontics'
            when sp.procedure_code like 'D6%' then 'Implants'
            when sp.procedure_code like 'D7%' then 'Oral Surgery'
            when sp.procedure_code like 'D8%' then 'Orthodontics'
            when sp.procedure_code like 'D9%' then 'Other'
            when sp.is_hygiene = true then 'Preventive'
            when sp.is_prosthetic = true then 'Prosthodontics'
            when sp.is_radiology = true then 'Diagnostic'
            else 'Other'
        end as procedure_category,
        
        -- Complexity level based on clinical factors
        case
            when sp.is_multi_visit = true then 'Complex'
            when sp.base_units >= 3 then 'Complex'
            when sp.base_units >= 2 then 'Moderate'
            else 'Simple'
        end as complexity_level,
        
        -- Revenue tier based on procedure category
        case
            when sp.procedure_code like 'D5%' then 'High'  -- Prosthodontics
            when sp.procedure_code like 'D6%' then 'High'  -- Implants
            when sp.procedure_code like 'D7%' then 'High'  -- Oral Surgery
            when sp.procedure_code like 'D8%' then 'High'  -- Orthodontics
            when sp.procedure_code like 'D3%' then 'Medium' -- Endodontics
            when sp.procedure_code like 'D4%' then 'Medium' -- Periodontics
            when sp.procedure_code like 'D2%' then 'Medium' -- Restorative
            else 'Low'
        end as revenue_tier,
        
        -- Clinical urgency for treatment prioritization
        case
            when sp.procedure_code like 'D7%' then 'High'  -- Oral Surgery
            when sp.procedure_code like 'D3%' then 'High'  -- Endodontics
            when sp.procedure_code like 'D4%' then 'Medium' -- Periodontics
            when sp.procedure_code like 'D2%' then 'Medium' -- Restorative
            else 'Low'
        end as clinical_urgency,
        
        -- Insurance billing complexity
        case
            when sp.is_multi_visit = true or sp.base_units >= 3 then 'High'
            when sp.medical_code is not null or sp.diagnostic_codes is not null then 'Medium'
            else 'Low'
        end as insurance_complexity,
        
        -- Treatment planning timeframe
        case
            when sp.procedure_code like 'D5%' then 'Long-term'   -- Prosthodontics
            when sp.procedure_code like 'D8%' then 'Long-term'   -- Orthodontics
            when sp.procedure_code like 'D3%' then 'Medium-term' -- Endodontics
            when sp.procedure_code like 'D4%' then 'Medium-term' -- Periodontics
            else 'Short-term'
        end as treatment_planning_category
        
    from source_procedure sp
),

-- 7. Final integration with fee data and definitions
final as (
    select
        -- Primary identification
        pc.procedure_code_id,
        pc.procedure_code,
        
        -- Core attributes
        pc.description as procedure_description,
        pc.abbreviated_description,
        pc.layman_term,
        pc.procedure_category_id,
        
        -- Treatment area information
        pc.treatment_area,
        tad.treatment_area_description,
        
        -- Business categorization (from business logic)
        pc.procedure_category,
        pc.complexity_level,
        pc.revenue_tier,
        pc.clinical_urgency,
        pc.insurance_complexity,
        pc.treatment_planning_category,
        
        -- Clinical flags
        pc.is_hygiene,
        pc.is_prosthetic,
        pc.is_radiology,
        pc.is_multi_visit,
        pc.base_units,
        
        -- Provider and billing defaults
        pc.default_provider_id,
        pc.default_revenue_code,
        pc.default_claim_note,
        pc.default_treatment_plan_note,
        pc.medical_code,
        pc.diagnostic_codes,
        
        -- Standard fee information (most recent per clinic)
        pf.fee_id as standard_fee_id,
        pf.fee_schedule_id,
        pf.fee_amount as standard_fee,
        fsl.fee_schedule_description,
        fsl.fee_schedule_type_id,
        ftd.fee_type_description,
        
        -- Fee statistics
        fs.available_fee_options,
        fs.min_available_fee,
        fs.max_available_fee,
        fs.avg_fee_amount,
        fs.fee_std_dev,
        
        -- Fee validation flags
        case 
            when pf.fee_amount is not null then true
            else false 
        end as has_standard_fee,
        
        case
            when fs.available_fee_options > 1 then true
            else false
        end as has_multiple_fees,
        
        -- Metadata
        {{ standardize_intermediate_metadata(
            primary_source_alias='pc',
            source_metadata_fields=['_created_at', '_updated_at']
        ) }}
        
    from procedure_categorized pc
    left join treatment_area_definitions tad
        on pc.treatment_area = tad.treatment_area_id
    left join procedure_fees pf
        on pc.procedure_code_id = pf.procedure_code_id
        and pf.fee_rank = 1  -- Most recent fee
    left join fee_schedule_lookup fsl
        on pf.fee_schedule_id = fsl.fee_schedule_id
    left join fee_type_definitions ftd
        on fsl.fee_schedule_type_id = ftd.fee_type_id
    left join fee_statistics fs
        on pc.procedure_code_id = fs.procedure_code_id
)

select * from final

