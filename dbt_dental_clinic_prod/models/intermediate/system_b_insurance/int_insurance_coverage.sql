{{ config(        materialized='table',
        
        unique_key='insurance_plan_id',
        on_schema_change='fail',
        indexes=[
            {'columns': ['insurance_plan_id'], 'unique': true},
            {'columns': ['patient_id']},
            {'columns': ['carrier_id']},
            {'columns': ['_updated_at']}
        ]) }}

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



with Source as (
    select
        *,
        -- Include metadata fields for standardization
        _loaded_at,
        _created_at,
        _updated_at,
        _created_by
    from {{ ref('stg_opendental__insplan') }}
),

InsurancePlan as (
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

    from Source
),

Carrier as (
    select
        carrier_id,
        carrier_name
    from {{ ref('stg_opendental__carrier') }}
),

Employer as (
    select
        employer_id,
        employer_name,
        city,
        state
    from {{ ref('stg_opendental__employer') }}
),

Subscriber as (
    select
        inssub_id as subscriber_id,
        subscriber_external_id,
        -- Include metadata fields for standardization
        _loaded_at,
        _created_at,
        _updated_at,
        _created_by
    from {{ ref('stg_opendental__inssub') }}
),

PatientPlan as (
    select
        patient_id,
        patplan_id,
        insurance_subscriber_id,
        ordinal,
        is_pending,
        relationship as patient_relationship,
        created_at as patient_plan_created_at,
        updated_at as patient_plan_updated_at
    from {{ ref('stg_opendental__patplan') }}
),

Verification as (
    select
        foreign_key_id as insurance_plan_id,
        last_verified_date,
        entry_timestamp,
        last_modified_at
    from {{ ref('stg_opendental__insverify') }}
    where verify_type = 1  -- Only get insurance subscriber verifications
),

Benefits as (
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

Final as (
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
            when pp.is_pending = 1 then false
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
            when pp.is_pending = 1 then pp.patient_plan_updated_at
            else null
        end as termination_date,

        -- Metadata fields from joined tables (for macro usage)
        ip._loaded_at,
        ip._created_at,
        ip._updated_at,
        ip._created_by,
        s._loaded_at as subscriber_loaded_at,
        s._created_at as subscriber_created_at,
        s._updated_at as subscriber_updated_at,
        s._created_by as subscriber_created_by,

        -- Standardized metadata using macro
        {{ standardize_intermediate_metadata() }}

    from PatientPlan pp
    left join InsurancePlan ip
        on pp.patplan_id = ip.insurance_plan_id
    left join Carrier c
        on ip.carrier_id = c.carrier_id
    left join Employer e
        on ip.employer_id = e.employer_id
    left join Subscriber s
        on pp.insurance_subscriber_id = s.subscriber_id
    left join Verification v
        on pp.patplan_id = v.insurance_plan_id
    left join Benefits b
        on pp.patplan_id = b.insurance_plan_id
)

select * from Final 
