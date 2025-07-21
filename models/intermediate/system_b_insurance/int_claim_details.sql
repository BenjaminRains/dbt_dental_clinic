{{ config(        materialized='table',
        
        unique_key='claim_detail_id',
        on_schema_change='fail',
        indexes=[
            {'columns': ['claim_detail_id'], 'unique': true},
            {'columns': ['claim_id']},
            {'columns': ['patient_id']},
            {'columns': ['procedure_id']},
            {'columns': ['_updated_at']}
        ]) }}

/*
    Intermediate model for claim_details
    Part of System B: Insurance & Claims Processing
    
    This model:
    1. Combines claim data with detailed procedure and insurance information
    2. Provides comprehensive view of claim procedures with financial details
    3. Integrates insurance coverage verification and benefit information
    
    Business Logic Features:
    - Claim Procedure Integration: Links claims to specific procedures with billing details
    - Insurance Coverage Mapping: Associates claims with active insurance plans and benefits
    - Financial Tracking: Tracks billed, allowed, paid amounts and patient responsibility
    - Procedure Classification: Includes procedure codes, categories, and treatment area details
    
    Data Quality Notes:
    - Some claims may have multiple procedures requiring composite key handling
    - Insurance coverage data depends on verification status and effective dates
    - Financial amounts may be null for pending or rejected claims
    
    Performance Considerations:
    - Uses table materialization due to complex joins across multiple large tables
    - Indexed on key lookup fields (claim_id, patient_id, procedure_id)
    - Consider incremental materialization if volume becomes problematic
*/

with source_claim as (
    select
        claim_id,
        patient_id,
        plan_id,
        claim_status,
        claim_type,
        service_date as claim_date,
        secure_edit_timestamp as last_tracking_date
    from {{ ref('stg_opendental__claim') }}
),

source_claim_proc as (
    select
        claim_id,
        procedure_id,
        claim_procedure_id,
        status as claim_procedure_status,
        fee_billed as billed_amount,
        allowed_override as allowed_amount,
        insurance_payment_amount as paid_amount,
        write_off,
        copay_amount as patient_responsibility
    from {{ ref('stg_opendental__claimproc') }}
),

procedure_lookup as (
    select distinct
        pl.procedure_id,
        pl.procedure_code_id
    from {{ ref('stg_opendental__procedurelog') }} pl
    inner join {{ ref('stg_opendental__claimproc') }} cp
        on pl.procedure_id = cp.procedure_id
),

procedure_definitions as (
    select
        procedure_code_id,
        procedure_code,
        code_prefix,
        description as code_description,
        abbreviated_description,
        procedure_time,
        procedure_category_id,
        treatment_area,
        is_prosthetic_flag,
        is_hygiene_flag,
        base_units,
        is_radiology_flag,
        no_bill_insurance_flag,
        default_claim_note,
        medical_code,
        diagnostic_codes
    from {{ ref('stg_opendental__procedurecode') }}
),

insurance_coverage as (
    select
        insurance_plan_id,
        patient_id,
        carrier_id,
        subscriber_id,
        plan_type,
        group_number,
        group_name,
        verification_date,
        benefit_details,
        is_active,
        effective_date,
        termination_date
    from {{ ref('int_insurance_coverage') }}
),

claim_details_integrated as (
    select
        -- Primary Key
        {{ dbt_utils.generate_surrogate_key(['c.claim_id', 'cp.procedure_id', 'cp.claim_procedure_id']) }} as claim_detail_id,
        c.claim_id,
        cp.claim_procedure_id,

        -- Foreign Keys
        c.patient_id,
        ic.insurance_plan_id,
        ic.carrier_id,
        ic.subscriber_id,
        cp.procedure_id,

        -- Claim Status and Type
        c.claim_status,
        c.claim_type,
        c.claim_date,
        cp.claim_procedure_status,

        -- Procedure Details
        pc.procedure_code,
        pc.code_prefix,
        pc.code_description as procedure_description,
        pc.abbreviated_description,
        pc.procedure_time,
        pc.procedure_category_id,
        pc.treatment_area,
        pc.is_prosthetic_flag,
        pc.is_hygiene_flag,
        pc.base_units,
        pc.is_radiology_flag,
        pc.no_bill_insurance_flag,
        pc.default_claim_note,
        pc.medical_code,
        pc.diagnostic_codes,

        -- Financial Information
        cp.billed_amount,
        cp.allowed_amount,
        cp.paid_amount,
        cp.write_off as write_off_amount,
        cp.patient_responsibility,

        -- Insurance Plan Details
        ic.plan_type,
        ic.group_number,
        ic.group_name,
        ic.verification_date,
        ic.benefit_details,
        ic.is_active as verification_status,
        ic.effective_date,
        ic.termination_date,

        -- Metadata
        c.claim_date as _created_at,
        coalesce(c.last_tracking_date, c.claim_date) as _updated_at,
        current_timestamp as _transformed_at

    from source_claim c
    left join source_claim_proc cp
        on c.claim_id = cp.claim_id
    left join procedure_lookup pl
        on cp.procedure_id = pl.procedure_id
    left join procedure_definitions pc
        on pl.procedure_code_id = pc.procedure_code_id
    left join insurance_coverage ic
        on c.patient_id = ic.patient_id
        and c.plan_id = ic.insurance_plan_id
)

select * from claim_details_integrated 
