{{ config(
    materialized='table',
    schema='intermediate',
    unique_key='insurance_plan_id',
    on_schema_change='fail',
    indexes=[
        {'columns': ['insurance_plan_id'], 'unique': true},
        {'columns': ['patient_id']},
        {'columns': ['carrier_id']},
        {'columns': ['_updated_at']}
    ]
) }}

/*
    Intermediate model for insurance coverage
    Part of System B: Insurance & Claims Processing
    
    This model:
    1. Integrates patient insurance plans with carrier, employer, and benefit information
    2. Provides comprehensive insurance coverage data for claims processing
    3. Handles incomplete records and historical plan data
    
    Business Logic Features:
    - Insurance Plan Integration: Links patient plans with carrier and employer details
    - Benefit Aggregation: Consolidates template benefits into JSON structure
    - Verification Status: Tracks insurance verification and active status
    - Incomplete Record Handling: Preserves records with missing carrier or subscriber info
    
    Data Quality Notes:
    - Incomplete records are preserved with carrier_id = -1 and empty carrier_name
    - Subscriber_id = -1 when carrier info is missing or subscriber doesn't exist
    - is_incomplete_record = true in both cases
    - Historical plans are included regardless of age for audit trail
    
    Performance Considerations:
    - Table materialization for complex joins across multiple large tables
    - Indexed on key lookup fields (patient_id, carrier_id)
    - JSON aggregation for benefit details to reduce row count
*/

-- 1. Source data retrieval
with source_insurance_plan as (
    select * from {{ ref('stg_opendental__insplan') }}
),

source_subscriber as (
    select * from {{ ref('stg_opendental__inssub') }}
),

source_patient_plan as (
    select * from {{ ref('stg_opendental__patplan') }}
),

-- 2. Lookup/reference data
carrier_lookup as (
    select
        carrier_id,
        carrier_name
    from {{ ref('stg_opendental__carrier') }}
),

employer_lookup as (
    select
        employer_id,
        employer_name,
        city,
        state
    from {{ ref('stg_opendental__employer') }}
),

verification_lookup as (
    select
        foreign_key_id as insurance_plan_id,
        last_verified_date,
        date_created,
        date_updated
    from {{ ref('stg_opendental__insverify') }}
    where verify_type = 1  -- Only get insurance subscriber verifications
),

-- 3. Calculation/aggregation CTEs
benefit_aggregation as (
    select
        insurance_plan_id,
        json_agg(
            json_build_object(
                'benefit_id', benefit_id,
                'coverage_category_id', coverage_category_id,
                'procedure_code_id', procedure_code_id,
                'code_group_id', code_group_id,
                'benefit_type', benefit_type,
                'coverage_percent', coverage_percent,
                'monetary_amount', monetary_amount,
                'time_period', time_period,
                'quantity_qualifier', quantity_qualifier,
                'quantity', quantity,
                'coverage_level', coverage_level,
                'treatment_area', treatment_area
            )
        ) as benefit_details
    from {{ ref('stg_opendental__benefit') }}
    where patient_plan_id = 0  -- Only get template benefits
    group by insurance_plan_id
),

-- 4. Business logic CTEs
insurance_plan_enhanced as (
    select
        -- Primary Key
        insurance_plan_id,
        
        -- Foreign Keys
        carrier_id,
        employer_id,
        
        -- Plan Details
        group_name,
        group_number,
        plan_type,
        
        -- Flags
        is_medical,
        is_hidden,
        show_base_units,
        code_subst_none,
        hide_from_verify_list,
        has_ppo_subst_writeoffs,
        is_blue_book_enabled,
        
        -- Metadata fields (preserved from staging model)
        _loaded_at,
        _created_at,
        _updated_at,
        _created_by

    from source_insurance_plan
),

subscriber_enhanced as (
    select
        inssub_id as subscriber_id,
        subscriber_external_id,
        -- Metadata fields for standardization
        _loaded_at,
        _created_at,
        _updated_at,
        _created_by
    from source_subscriber
),

patient_plan_enhanced as (
    select
        patient_id,
        patplan_id,
        insurance_subscriber_id,
        ordinal,
        is_pending,
        relationship as patient_relationship,
        date_created as patient_plan_created_at,
        date_updated as patient_plan_updated_at
    from source_patient_plan
),

-- 5. Integration CTE (joins everything together)
insurance_coverage_integrated as (
    select
        -- Primary Key
        pp.patplan_id as insurance_plan_id,

        -- Foreign Keys
        pp.patient_id,
        pp.ordinal,
        case 
            when ip.carrier_id is null then -1
            else ip.carrier_id
        end as carrier_id,
        case
            when ip.carrier_id is null then -1
            when s.subscriber_id is null then -1
            else pp.insurance_subscriber_id
        end as subscriber_id,

        -- Plan Details
        case 
            when ip.carrier_id is null then ''
            else ip.plan_type
        end as plan_type,
        case 
            when ip.carrier_id is null then ''
            else ip.group_number
        end as group_number,
        case 
            when ip.group_name is null then ''
            else ip.group_name
        end as group_name,
        case 
            when ip.carrier_id is null then ''
            else c.carrier_name
        end as carrier_name,

        -- Verification Status
        v.last_verified_date as verification_date,

        -- Benefit Details
        b.benefit_details,

        -- Employer Information
        case
            when ip.employer_id is null then null
            else ip.employer_id
        end as employer_id,
        case
            when ip.employer_id is null then ''
            else e.employer_name
        end as employer_name,
        case
            when ip.employer_id is null then ''
            else e.city
        end as employer_city,
        case
            when ip.employer_id is null then ''
            else e.state
        end as employer_state,
        
        -- Status Flags
        case
            when pp.is_pending = true then false
            when v.last_verified_date is not null then true
            else false
        end as is_active,
        case
            when ip.carrier_id is null then true
            when s.subscriber_id is null then true
            else false
        end as is_incomplete_record,

        -- Dates
        case
            when pp.patient_plan_created_at is not null and pp.patient_plan_created_at::text != '' then pp.patient_plan_created_at
            when pp.patient_plan_updated_at is not null and pp.patient_plan_updated_at::text != '' then pp.patient_plan_updated_at
            else '2020-01-01'::timestamp
        end as effective_date,
        case
            when pp.is_pending = true then pp.patient_plan_updated_at
            else null
        end as termination_date,

        -- Primary source metadata (insurance plan - primary source)
        {{ standardize_intermediate_metadata(primary_source_alias='ip') }},
        
        -- Secondary source metadata (subscriber - may be NULL)
        s._loaded_at as subscriber_loaded_at,
        s._created_at as subscriber_created_at,
        s._updated_at as subscriber_updated_at,
        s._created_by as subscriber_created_by

    from patient_plan_enhanced pp
    left join insurance_plan_enhanced ip
        on pp.patplan_id = ip.insurance_plan_id
    left join carrier_lookup c
        on ip.carrier_id = c.carrier_id
    left join employer_lookup e
        on ip.employer_id = e.employer_id
    left join subscriber_enhanced s
        on pp.insurance_subscriber_id = s.subscriber_id
    left join verification_lookup v
        on pp.patplan_id = v.insurance_plan_id
    left join benefit_aggregation b
        on pp.patplan_id = b.insurance_plan_id
)

-- 6. Final selection
select * from insurance_coverage_integrated 
