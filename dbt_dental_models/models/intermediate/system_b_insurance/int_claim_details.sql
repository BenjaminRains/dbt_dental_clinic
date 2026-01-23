{{ config(        
        materialized='table',
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
    4. Includes claim form template details for form management and analytics
    
    Business Logic Features:
    - Claim Procedure Integration: Links claims to specific procedures with billing details
    - Insurance Coverage Mapping: Associates claims with active insurance plans and benefits
    - Financial Tracking: Tracks billed, allowed, paid amounts and patient responsibility
    - Procedure Classification: Includes procedure codes, categories, and treatment area details
    - Form Template Integration: Associates claims with their form templates and layout settings
    
    Data Quality Notes:
    - Some claims may have multiple procedures requiring composite key handling
    - Insurance coverage data depends on verification status and effective dates
    - Financial amounts may be null for pending or rejected claims
    - Claim form templates may be null for claims without specified forms
    - Model starts with claimprocs to include all valid procedures, even when claims have missing dates
    - Uses procedure_date as effective date when claim_date is NULL (for claims with DateService = '0001-01-01')
    
    Performance Considerations:
    - Uses table materialization due to complex joins across multiple large tables
    - Indexed on key lookup fields (claim_id, patient_id, procedure_id)
    - Consider incremental materialization if volume becomes problematic
*/

with source_claim_proc as (
    select
        claim_id,
        procedure_id,
        claim_procedure_id,
        patient_id,
        plan_id,
        status as claim_procedure_status,
        fee_billed as billed_amount,
        allowed_override as allowed_amount,
        insurance_payment_amount as paid_amount,
        write_off,
        copay_amount as patient_responsibility,
        procedure_date
    from {{ ref('stg_opendental__claimproc') }}
),

source_claim as (
    select
        claim_id,
        patient_id,
        plan_id,
        claim_form_id,
        claim_status,
        claim_type,
        service_date as claim_date,
        sec_date_t_edit as last_tracking_date
    from {{ ref('stg_opendental__claim') }}
),

procedure_lookup as (
    select distinct
        pl.procedure_id,
        pl.procedure_code_id,
        pl.provider_id
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
        is_prosthetic,
        is_hygiene,
        base_units,
        is_radiology,
        no_bill_insurance,
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

claim_form_templates as (
    select
        claim_form_id,
        description as claim_form_description,
        font_name as form_font_name,
        font_size as form_font_size,
        unique_id as form_unique_id,
        is_hidden as form_is_hidden,
        print_images as form_print_images,
        offset_x as form_offset_x,
        offset_y as form_offset_y,
        width as form_width,
        height as form_height
    from {{ ref('stg_opendental__claimform') }}
),

claim_details_integrated as (
    select
        -- Primary Key
        {{ dbt_utils.generate_surrogate_key(['cp.claim_id', 'cp.procedure_id', 'cp.claim_procedure_id']) }} as claim_detail_id,
        cp.claim_id,
        cp.claim_procedure_id,

        -- Foreign Keys
        -- Use patient_id and plan_id from claimproc (primary source), fallback to claim if available
        coalesce(cp.patient_id, c.patient_id) as patient_id,
        c.claim_form_id,
        ic.insurance_plan_id,
        ic.carrier_id,
        ic.subscriber_id,
        cp.procedure_id,
        pl.provider_id,

        -- Claim Status and Type
        c.claim_status,
        c.claim_type,
        -- Use claim_date from claim if available, otherwise use procedure_date from claimproc
        coalesce(c.claim_date, cp.procedure_date) as claim_date,
        cp.claim_procedure_status,

        -- Procedure Details
        pc.procedure_code,
        pc.code_prefix,
        pc.code_description as procedure_description,
        pc.abbreviated_description,
        pc.procedure_time,
        pc.procedure_category_id,
        pc.treatment_area,
        pc.is_prosthetic,
        pc.is_hygiene,
        pc.base_units,
        pc.is_radiology,
        pc.no_bill_insurance,
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

        -- Claim Form Template Details
        cft.claim_form_description,
        cft.form_font_name,
        cft.form_font_size,
        cft.form_unique_id,
        cft.form_is_hidden,
        cft.form_print_images,
        cft.form_offset_x,
        cft.form_offset_y,
        cft.form_width,
        cft.form_height,

        -- Generate metadata for this intermediate model
        current_timestamp as _loaded_at,
        -- Use effective claim_date (which may be procedure_date) for _created_at
        coalesce(c.claim_date, cp.procedure_date) as _created_at,
        -- Use claim's last_tracking_date if available, otherwise use effective claim_date
        coalesce(c.last_tracking_date, c.claim_date, cp.procedure_date) as _updated_at,
        0 as _created_by

    from source_claim_proc cp
    left join source_claim c
        on cp.claim_id = c.claim_id
    left join procedure_lookup pl
        on cp.procedure_id = pl.procedure_id
    left join procedure_definitions pc
        on pl.procedure_code_id = pc.procedure_code_id
    left join insurance_coverage ic
        on coalesce(cp.patient_id, c.patient_id) = ic.patient_id
        and coalesce(cp.plan_id, c.plan_id) = ic.insurance_plan_id
    left join claim_form_templates cft
        on c.claim_form_id = cft.claim_form_id
),

final as (
    select
        -- Core business fields
        claim_detail_id,
        claim_id,
        claim_procedure_id,
        patient_id,
        insurance_plan_id,
        carrier_id,
        subscriber_id,
        procedure_id,
        provider_id,
        claim_status,
        claim_type,
        claim_date,
        claim_procedure_status,
        procedure_code,
        code_prefix,
        procedure_description,
        abbreviated_description,
        procedure_time,
        procedure_category_id,
        treatment_area,
        is_prosthetic,
        is_hygiene,
        base_units,
        is_radiology,
        no_bill_insurance,
        default_claim_note,
        medical_code,
        diagnostic_codes,
        billed_amount,
        allowed_amount,
        paid_amount,
        write_off_amount,
        patient_responsibility,
        plan_type,
        group_number,
        group_name,
        verification_date,
        benefit_details,
        verification_status,
        effective_date,
        termination_date,
        
        -- Claim Form Template Details
        claim_form_id,
        claim_form_description,
        form_font_name,
        form_font_size,
        form_unique_id,
        form_is_hidden,
        form_print_images,
        form_offset_x,
        form_offset_y,
        form_width,
        form_height,
        
        -- Standardized metadata (preserved from staging models)
        _loaded_at,
        _created_at,
        _updated_at,
        _created_by,
        current_timestamp as _transformed_at
    from claim_details_integrated
)

select * from final 
