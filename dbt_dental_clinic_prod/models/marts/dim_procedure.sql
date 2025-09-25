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
2. Enhances procedure data with clinical categorization and complexity analysis
3. Integrates fee information and statistical analysis for pricing insights

Business Logic Features:
- Clinical categorization based on CDT codes and procedure flags
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
- Some procedures may not have standard fees (handled with null checks)
- Definition lookups use hardcoded category IDs (category_id 5 for treatment areas, 6 for fee types)
- Fee statistics are calculated from available fee data and may be incomplete for new procedures

Performance Considerations:
- Indexed on procedure_code_id for fast lookups
- Indexed on procedure_code for code-based queries
- Indexed on procedure_category for specialty filtering
- Indexed on clinical flags for procedure type filtering

Dependencies:
- stg_opendental__procedurecode: Primary source for procedure data
- stg_opendental__feesched: Fee schedule information
- stg_opendental__definition: Lookup values for coded fields
- stg_opendental__fee: Fee amounts and statistics
*/

-- 1. Source data retrieval
with source_procedure as (
    select * from {{ ref('stg_opendental__procedurecode') }}
),

-- 2. Lookup/reference data
procedure_lookup as (
    select
        fee_schedule_id,
        fee_schedule_description,
        fee_schedule_type_id,
        is_hidden
    from {{ ref('stg_opendental__feesched') }}
),

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

-- 3. Fee information with ranking
procedure_fee_data as (
    select
        procedure_code_id,
        -- Standard fees with ranking to get the most relevant fee per procedure code
        fee_id,
        fee_schedule_id,
        clinic_id,
        provider_id,
        fee_amount as standard_fee,
        _created_at,
        row_number() over (
            partition by procedure_code_id, clinic_id
            order by _created_at desc
        ) as fee_rank
    from {{ ref('stg_opendental__fee') }}
),

-- 4. Fee statistics (separate CTE to avoid DISTINCT in window functions)
procedure_fee_stats as (
    select
        procedure_code_id,
        count(distinct fee_id) as available_fee_options,
        min(fee_amount) as min_available_fee,
        max(fee_amount) as max_available_fee,
        avg(fee_amount) as avg_fee_amount
    from {{ ref('stg_opendental__fee') }}
    group by procedure_code_id
),

-- 5. Business logic enhancement
procedure_enhanced as (
    select
        -- Primary identification
        sp.procedure_code_id,

        -- Core attributes
        sp.procedure_code,
        sp.description,
        sp.abbreviated_description,
        sp.procedure_category_id,
        sp.treatment_area,
        sp.is_hygiene,
        sp.is_prosthetic,
        sp.is_radiology,
        sp.is_multi_visit,
        sp.base_units,
        sp.default_provider_id,
        sp.default_revenue_code,
        sp.default_claim_note,
        sp.default_treatment_plan_note,
        sp.layman_term,
        sp.medical_code,
        sp.diagnostic_codes,

        -- Calculated fields - Clinical categorization
        case
            when sp.procedure_code like 'D1%' then 'Preventive'
            when sp.procedure_code like 'D2%' then 'Restorative'
            when sp.procedure_code like 'D3%' then 'Endodontics'
            when sp.procedure_code like 'D4%' then 'Periodontics'
            when sp.procedure_code like 'D5%' then 'Prosthodontics'
            when sp.procedure_code like 'D6%' then 'Oral Surgery'
            when sp.procedure_code like 'D7%' then 'Orthodontics'
            when sp.procedure_code like 'D8%' then 'Other'
            when sp.is_hygiene = true then 'Preventive'
            when sp.is_prosthetic = true then 'Prosthodontics'
            when sp.is_radiology = true then 'Diagnostic'
            else 'Other'
        end as procedure_category,
        
        -- Calculated fields - Complexity analysis
        case
            when sp.is_multi_visit = true then 'Complex'
            when sp.base_units >= 3 then 'Complex'
            when sp.base_units >= 2 then 'Moderate'
            else 'Simple'
        end as complexity_level,
        
        -- Calculated fields - Financial classification
        case
            when case
                when sp.procedure_code like 'D1%' then 'Preventive'
                when sp.procedure_code like 'D2%' then 'Restorative'
                when sp.procedure_code like 'D3%' then 'Endodontics'
                when sp.procedure_code like 'D4%' then 'Periodontics'
                when sp.procedure_code like 'D5%' then 'Prosthodontics'
                when sp.procedure_code like 'D6%' then 'Oral Surgery'
                when sp.procedure_code like 'D7%' then 'Orthodontics'
                when sp.procedure_code like 'D8%' then 'Other'
                when sp.is_hygiene = true then 'Preventive'
                when sp.is_prosthetic = true then 'Prosthodontics'
                when sp.is_radiology = true then 'Diagnostic'
                else 'Other'
            end in ('Prosthodontics', 'Oral Surgery', 'Orthodontics') then 'High'
            when case
                when sp.procedure_code like 'D1%' then 'Preventive'
                when sp.procedure_code like 'D2%' then 'Restorative'
                when sp.procedure_code like 'D3%' then 'Endodontics'
                when sp.procedure_code like 'D4%' then 'Periodontics'
                when sp.procedure_code like 'D5%' then 'Prosthodontics'
                when sp.procedure_code like 'D6%' then 'Oral Surgery'
                when sp.procedure_code like 'D7%' then 'Orthodontics'
                when sp.procedure_code like 'D8%' then 'Other'
                when sp.is_hygiene = true then 'Preventive'
                when sp.is_prosthetic = true then 'Prosthodontics'
                when sp.is_radiology = true then 'Diagnostic'
                else 'Other'
            end in ('Endodontics', 'Periodontics') then 'Medium'
            else 'Low'
        end as revenue_tier,
        
        -- Calculated fields - Clinical workflow
        case
            when case
                when sp.procedure_code like 'D1%' then 'Preventive'
                when sp.procedure_code like 'D2%' then 'Restorative'
                when sp.procedure_code like 'D3%' then 'Endodontics'
                when sp.procedure_code like 'D4%' then 'Periodontics'
                when sp.procedure_code like 'D5%' then 'Prosthodontics'
                when sp.procedure_code like 'D6%' then 'Oral Surgery'
                when sp.procedure_code like 'D7%' then 'Orthodontics'
                when sp.procedure_code like 'D8%' then 'Other'
                when sp.is_hygiene = true then 'Preventive'
                when sp.is_prosthetic = true then 'Prosthodontics'
                when sp.is_radiology = true then 'Diagnostic'
                else 'Other'
            end in ('Oral Surgery', 'Endodontics') then 'High'
            when case
                when sp.procedure_code like 'D1%' then 'Preventive'
                when sp.procedure_code like 'D2%' then 'Restorative'
                when sp.procedure_code like 'D3%' then 'Endodontics'
                when sp.procedure_code like 'D4%' then 'Periodontics'
                when sp.procedure_code like 'D5%' then 'Prosthodontics'
                when sp.procedure_code like 'D6%' then 'Oral Surgery'
                when sp.procedure_code like 'D7%' then 'Orthodontics'
                when sp.procedure_code like 'D8%' then 'Other'
                when sp.is_hygiene = true then 'Preventive'
                when sp.is_prosthetic = true then 'Prosthodontics'
                when sp.is_radiology = true then 'Diagnostic'
                else 'Other'
            end in ('Periodontics', 'Restorative') then 'Medium'
            else 'Low'
        end as clinical_urgency,
        
        -- Calculated fields - Billing complexity
        case
            when sp.is_multi_visit = true or sp.base_units >= 3 then 'High'
            when sp.medical_code is not null or sp.diagnostic_codes is not null then 'Medium'
            else 'Low'
        end as insurance_complexity,
        
        -- Calculated fields - Treatment planning
        case
            when case
                when sp.procedure_code like 'D1%' then 'Preventive'
                when sp.procedure_code like 'D2%' then 'Restorative'
                when sp.procedure_code like 'D3%' then 'Endodontics'
                when sp.procedure_code like 'D4%' then 'Periodontics'
                when sp.procedure_code like 'D5%' then 'Prosthodontics'
                when sp.procedure_code like 'D6%' then 'Oral Surgery'
                when sp.procedure_code like 'D7%' then 'Orthodontics'
                when sp.procedure_code like 'D8%' then 'Other'
                when sp.is_hygiene = true then 'Preventive'
                when sp.is_prosthetic = true then 'Prosthodontics'
                when sp.is_radiology = true then 'Diagnostic'
                else 'Other'
            end in ('Prosthodontics', 'Orthodontics') then 'Long-term'
            when case
                when sp.procedure_code like 'D1%' then 'Preventive'
                when sp.procedure_code like 'D2%' then 'Restorative'
                when sp.procedure_code like 'D3%' then 'Endodontics'
                when sp.procedure_code like 'D4%' then 'Periodontics'
                when sp.procedure_code like 'D5%' then 'Prosthodontics'
                when sp.procedure_code like 'D6%' then 'Oral Surgery'
                when sp.procedure_code like 'D7%' then 'Orthodontics'
                when sp.procedure_code like 'D8%' then 'Other'
                when sp.is_hygiene = true then 'Preventive'
                when sp.is_prosthetic = true then 'Prosthodontics'
                when sp.is_radiology = true then 'Diagnostic'
                else 'Other'
            end in ('Endodontics', 'Periodontics') then 'Medium-term'
            else 'Short-term'
        end as treatment_planning_category,

        -- Metadata
        sp._created_at,
        sp._updated_at,
        current_timestamp as _loaded_at
    from source_procedure sp
),

-- 6. Final integration with fee data and lookups
final as (
    select
        -- Primary identification
        pe.procedure_code_id,
        
        -- Core attributes
        pe.procedure_code,
        pe.description,
        pe.abbreviated_description,
        pe.procedure_category_id,
        pe.treatment_area,
        coalesce(def_treatment.item_name, pe.treatment_area::text) as treatment_area_desc,
        
        -- Enhanced categorizations
        pe.procedure_category,
        pe.complexity_level,
        pe.revenue_tier,
        pe.clinical_urgency,
        pe.insurance_complexity,
        pe.treatment_planning_category,
        
        -- Clinical flags
        pe.is_hygiene,
        pe.is_prosthetic,
        pe.is_radiology,
        pe.is_multi_visit,
        
        -- Clinical metrics
        pe.base_units,
        pe.default_provider_id,
        pe.default_revenue_code,
        
        -- Fee information
        pfd.fee_id as standard_fee_id,
        pfd.fee_schedule_id,
        pfd.standard_fee,
        pl.fee_schedule_description,
        pl.fee_schedule_type_id,
        coalesce(def_fee_type.item_name, pl.fee_schedule_type_id::text) as fee_schedule_type_desc,
        
        -- Fee statistics
        pfs.available_fee_options,
        pfs.min_available_fee,
        pfs.max_available_fee,
        pfs.avg_fee_amount,
        
        -- Fee validation flags
        case 
            when pfd.standard_fee is null then false
            else true 
        end as has_standard_fee,
        
        -- Documentation
        pe.default_claim_note,
        pe.default_treatment_plan_note,
        pe.layman_term,
        pe.medical_code,
        pe.diagnostic_codes,
        
        -- Metadata
        pe._created_at,
        pe._updated_at,
        pe._loaded_at
    from procedure_enhanced pe
    -- Join with fee information (only get the most recent fee per procedure)
    left join procedure_fee_data pfd
        on pe.procedure_code_id = pfd.procedure_code_id
        and pfd.fee_rank = 1
    -- Join with fee statistics
    left join procedure_fee_stats pfs
        on pe.procedure_code_id = pfs.procedure_code_id
    -- Join with fee schedule lookup
    left join procedure_lookup pl
        on pfd.fee_schedule_id = pl.fee_schedule_id
    -- Join with definitions for various coded values
    left join definition_lookup def_treatment
        on def_treatment.category_id = 5  -- Treatment areas category
        and def_treatment.item_value = pe.treatment_area::text
    left join definition_lookup def_fee_type
        on def_fee_type.category_id = 6  -- Fee schedule types category
        and def_fee_type.item_value = pl.fee_schedule_type_id::text
)

select * from final
