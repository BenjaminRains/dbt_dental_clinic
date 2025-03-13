with source as (
    select * from {{ source('opendental', 'claimproc') }}
),

renamed as (
    select
        -- Primary keys and relationships
        ClaimProcNum as claim_procedure_id,
        ProcNum as procedure_id,
        ClaimNum as claim_id,
        PatNum as patient_id,
        ProvNum as provider_id,
        ClaimPaymentNum as claim_payment_id,
        PlanNum as insurance_plan_id,
        InsSubNum as insurance_subscriber_id,
        ClinicNum as clinic_id,
        PayPlanNum as payment_plan_id,
        ClaimPaymentTracking as claim_payment_tracking_id,

        -- Financial information - billed amounts
        FeeBilled as fee_billed,
        CodeSent as procedure_code_sent,
        LineNumber as claim_line_number,
        NoBillIns as do_not_bill_insurance,

        -- Financial information - estimates
        InsPayEst as insurance_payment_estimate,
        DedEst as deductible_estimate,
        BaseEst as base_estimate,
        InsEstTotal as insurance_estimate_total,
        WriteOffEst as writeoff_estimate,
        CopayAmt as copay_amount,
        PaidOtherIns as paid_by_other_insurance_estimate,
        EstimateNote as estimate_note,

        -- Financial information - actual payments
        InsPayAmt as insurance_payment_amount,
        DedApplied as deductible_applied,
        WriteOff as writeoff_amount,
        IsOverpay as is_overpayment,

        -- Override values
        AllowedOverride as allowed_amount_override,
        PercentOverride as percentage_override,
        CopayOverride as copay_override,
        DedEstOverride as deductible_estimate_override,
        InsEstTotalOverride as insurance_estimate_total_override,
        PaidOtherInsOverride as paid_by_other_insurance_override,
        WriteOffEstOverride as writeoff_estimate_override,

        -- Procedure and claim status
        Status as status,
        Percentage as coverage_percentage,
        Remarks as remarks,
        ClaimAdjReasonCodes as claim_adjustment_reason_codes,
        IsTransfer as is_transfer,
        PaymentRow as payment_row,

        -- Dates and timeline
        ProcDate as procedure_date,
        DateCP as payment_received_date,
        DateEntry as entry_date,
        DateSuppReceived as supplemental_info_received_date,
        DateInsFinalized as insurance_finalized_date,

        -- Calculated fields
        CASE
            WHEN Status = 0 THEN 'NotReceived'
            WHEN Status = 1 THEN 'Received'
            WHEN Status = 2 THEN 'NotSent'
            WHEN Status = 3 THEN 'Supplemental'
            WHEN Status = 4 THEN 'CapClaim'
            WHEN Status = 5 THEN 'Preauth'
            WHEN Status = 6 THEN 'Estimate'
            WHEN Status = 7 THEN 'CapComplete'
            WHEN Status = 8 THEN 'CapEstimate'
            WHEN Status = 9 THEN 'Other'
            ELSE 'Unknown'
        END as status_description,

        -- Metadata
        SecUserNumEntry as created_by_user_id,
        SecDateEntry as created_at,
        SecDateTEdit as updated_at
    from source
)

select * from renamed