{{ config(
    materialized='incremental',
    unique_key='claim_procedure_id'
) }}

with source as (
    select * from {{ source('opendental', 'claimproc') }}
    where "ProcDate" >= '2023-01-01'
    {% if is_incremental() %}
        and "SecDateTEdit" > (select max(last_modified_at) from {{ this }})
    {% endif %}
),

renamed as (
    select
        -- Primary key
        "ClaimProcNum" as claim_procedure_id,
        
        -- Foreign keys
        "ProcNum" as procedure_id,
        "ClaimNum" as claim_id,
        "PatNum" as patient_id,
        "ProvNum" as provider_id,
        "PlanNum" as plan_id,
        "ClaimPaymentNum" as claim_payment_id,
        "ClinicNum" as clinic_id,
        "InsSubNum" as insurance_subscriber_id,
        "PayPlanNum" as payment_plan_id,
        "ClaimPaymentTracking" as claim_payment_tracking_id,
        "SecUserNumEntry" as secure_user_entry_id,
        
        -- Date fields
        "DateCP" as claim_procedure_date,
        "ProcDate" as procedure_date,
        "DateEntry" as entry_date,
        "SecDateEntry" as secure_entry_date,
        "SecDateTEdit" as last_modified_at,
        "DateSuppReceived" as supplemental_received_date,
        "DateInsFinalized" as insurance_finalized_date,
        
        -- Monetary amounts
        "FeeBilled" as fee_billed,
        "InsPayEst" as insurance_payment_estimate,
        "InsPayAmt" as insurance_payment_amount,
        "DedApplied" as deductible_applied,
        "WriteOff" as write_off,
        "AllowedOverride" as allowed_override,
        "CopayAmt" as copay_amount,
        "PaidOtherIns" as paid_other_insurance,
        "BaseEst" as base_estimate,
        "CopayOverride" as copay_override,
        "DedEst" as deductible_estimate,
        "DedEstOverride" as deductible_estimate_override,
        "InsEstTotal" as insurance_estimate_total,
        "InsEstTotalOverride" as insurance_estimate_total_override,
        "PaidOtherInsOverride" as paid_other_insurance_override,
        "WriteOffEst" as write_off_estimate,
        "WriteOffEstOverride" as write_off_estimate_override,
        
        -- Integer/status fields
        "Status" as status,
        "Percentage" as percentage,
        "PercentOverride" as percentage_override,
        "NoBillIns" as no_bill_insurance,
        "LineNumber" as line_number,
        "PaymentRow" as payment_row,
        ("IsTransfer" = 1)::boolean as is_transfer,
        ("IsOverpay" = 1)::boolean as is_overpay,
        
        -- Character fields
        "Remarks" as remarks,
        "CodeSent" as code_sent,
        "EstimateNote" as estimate_note,
        "ClaimAdjReasonCodes" as claim_adjustment_reason_codes,
        "SecurityHash" as security_hash
        
    from source
)

select * from renamed
