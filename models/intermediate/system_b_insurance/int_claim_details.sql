{{
    config(
        materialized='table',
        schema='intermediate',
        unique_key='claim_id || "-" || procedure_id'
    )
}}

with Claim as (
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

ClaimProc as (
    select distinct
        claim_id,
        procedure_id,
        fee_billed as billed_amount,
        allowed_override as allowed_amount,
        insurance_payment_amount as paid_amount,
        write_off,
        copay_amount as patient_responsibility
    from {{ ref('stg_opendental__claimproc') }}
),

ProcedureLog as (
    select distinct
        pl.procedure_id,
        pl.procedure_code_id
    from {{ ref('stg_opendental__procedurelog') }} pl
    inner join {{ ref('stg_opendental__claimproc') }} cp
        on pl.procedure_id = cp.procedure_id
),

ProcedureCode as (
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

InsuranceCoverage as (
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

Final as (
    select
        -- Primary Key
        c.claim_id,

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

        -- Meta Fields
        c.claim_date as created_at,
        c.last_tracking_date as updated_at

    from Claim c
    left join ClaimProc cp
        on c.claim_id = cp.claim_id
    left join ProcedureLog pl
        on cp.procedure_id = pl.procedure_id
    left join ProcedureCode pc
        on pl.procedure_code_id = pc.procedure_code_id
    left join InsuranceCoverage ic
        on c.patient_id = ic.patient_id
        and c.plan_id = ic.insurance_plan_id
)

select * from Final 