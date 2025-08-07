{{ config(        materialized='table',
        
        unique_key=['claim_id', 'procedure_id', 'claim_procedure_id']) }}

/*
Fact table for claim transactions and procedures.
This model combines data from multiple sources to create a comprehensive
claim fact table that supports both current and historical analysis.

Key features:
- Individual claim transaction tracking
- Procedure-level payment details
- Claim status monitoring
- EOB documentation tracking
- Payment timing analysis
- Revenue cycle metrics
*/

with ClaimDetails as (
    select * from {{ ref('int_claim_details') }}
),

ClaimPayments as (
    select * from {{ ref('int_claim_payments') }}
),

ClaimSnapshot as (
    select * from {{ ref('int_claim_snapshot') }}
),

ClaimTracking as (
    select * from {{ ref('int_claim_tracking') }}
),

EobAttachments as (
    select * from {{ ref('int_insurance_eob_attachments') }}
),

Final as (
    select
        -- Primary Key
        cd.claim_id,
        cd.procedure_id,
        cd.claim_procedure_id,

        -- Foreign Keys
        cd.patient_id,
        cd.insurance_plan_id,
        cd.carrier_id,
        cd.subscriber_id,
        cd.provider_id,

        -- Date Fields
        cd.claim_date,
        cp.check_date,
        cs.snapshot_date,
        ct.tracking_date,

        -- Claim Status and Type
        cd.claim_status,
        cd.claim_type,
        cd.claim_procedure_status,
        cs.snapshot_status,
        ct.tracking_status,

        -- Procedure Details
        cd.procedure_code,
        cd.code_prefix,
        cd.procedure_description,
        cd.abbreviated_description,
        cd.procedure_time,
        cd.procedure_category_id,
        cd.treatment_area,
        cd.is_prosthetic_flag,
        cd.is_hygiene_flag,
        cd.base_units,
        cd.is_radiology_flag,
        cd.no_bill_insurance_flag,
        cd.default_claim_note,
        cd.medical_code,
        cd.diagnostic_codes,

        -- Financial Information
        cd.billed_amount,
        cd.allowed_amount,
        cd.paid_amount,
        cd.write_off_amount,
        cd.patient_responsibility,
        cp.check_amount,
        cp.payment_type_id,
        cp.is_partial,

        -- EOB Documentation
        eob.eob_attachment_count,
        eob.eob_attachment_ids,
        eob.eob_attachment_file_names,

        -- Insurance Plan Details
        cd.plan_type,
        cd.group_number,
        cd.group_name,
        cd.verification_date,
        cd.benefit_details,
        cd.verification_status,
        cd.effective_date,
        cd.termination_date,

        -- Meta Fields
        cd.created_at as _created_at,
        cd.updated_at as _updated_at,
        current_timestamp as _loaded_at

    from ClaimDetails cd
    left join ClaimPayments cp
        on cd.claim_id = cp.claim_id
        and cd.procedure_id = cp.procedure_id
        and cd.claim_procedure_id = cp.claim_procedure_id
    left join ClaimSnapshot cs
        on cd.claim_id = cs.claim_id
        and cd.procedure_id = cs.procedure_id
        and cd.claim_procedure_id = cs.claim_procedure_id
    left join ClaimTracking ct
        on cd.claim_id = ct.claim_id
        and cd.procedure_id = ct.procedure_id
        and cd.claim_procedure_id = ct.claim_procedure_id
    left join EobAttachments eob
        on cp.claim_payment_id = eob.claim_payment_id
)

select * from Final
