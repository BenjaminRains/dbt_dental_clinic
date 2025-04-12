{{ config(
    materialized='incremental',
    unique_key='payment_allocation_id',
    schema='intermediate'
) }}

/*
    Intermediate model for payment allocations
    Connects payments with their splits across procedures and insurance claims
    Part of System C: Payment Allocation & Reconciliation
    
    This model combines:
    1. Patient payments and their splits
    2. Insurance claim payments and their allocations
    3. Payment plan allocations
*/

WITH patient_payments AS (
    SELECT
        payment_id,
        patient_id,
        payment_date,
        payment_amount,
        payment_type_id,
        check_number,
        bank_branch,
        payment_source,
        is_split_flag,
        is_recurring_cc_flag,
        payment_status,
        process_status,
        merchant_fee,
        payment_notes,
        clinic_id,
        created_by_user_id,
        entry_date,
        updated_at,
        deposit_id,
        external_id,
        is_cc_completed_flag,
        recurring_charge_date,
        receipt_text
    FROM {{ ref('stg_opendental__payment') }}
),

payment_splits AS (
    SELECT
        paysplit_id,
        payment_id,
        patient_id,
        clinic_id,
        provider_id,
        procedure_id,
        adjustment_id,
        payplan_id,
        payplan_charge_id,
        forward_split_id,
        split_amount,
        payment_date,
        procedure_date,
        is_discount_flag,
        discount_type,
        unearned_type,
        payplan_debit_type,
        entry_date,
        updated_at
    FROM {{ ref('stg_opendental__paysplit') }}
),

insurance_payments AS (
    SELECT
        claim_payment_id,
        check_date,
        date_issued,
        check_amount,
        check_number,
        bank_branch,
        carrier_name,
        is_partial,
        payment_type_id,
        payment_group_id,
        clinic_id,
        created_by_user_id,
        created_date,
        last_modified_at,
        deposit_id,
        note
    FROM {{ ref('stg_opendental__claimpayment') }}
),

claim_procedures AS (
    SELECT
        claim_procedure_id,
        procedure_id,
        claim_id,
        patient_id,
        provider_id,
        plan_id,
        claim_payment_id,
        clinic_id,
        insurance_subscriber_id,
        payment_plan_id,
        claim_procedure_date,
        procedure_date,
        insurance_finalized_date,
        deductible_applied,
        write_off,
        allowed_override,
        copay_amount,
        paid_other_insurance,
        base_estimate,
        insurance_estimate_total,
        insurance_payment_amount,
        status,
        percentage,
        is_transfer,
        is_overpay,
        remarks,
        code_sent,
        estimate_note,
        last_modified_at
    FROM {{ ref('stg_opendental__claimproc') }}
    WHERE status IN (1, 3)  -- Only include Received and Supplemental claims
    AND claim_payment_id != 0  -- Exclude records with no payment
),

-- Combine all payment allocations
payment_allocations AS (
    -- Patient payment splits
    SELECT
        ps.paysplit_id AS payment_allocation_id,
        'PATIENT' AS payment_source_type,
        pp.payment_id,
        pp.patient_id,
        pp.clinic_id,
        ps.provider_id,
        ps.procedure_id,
        ps.adjustment_id,
        ps.payplan_id,
        ps.payplan_charge_id,
        ps.split_amount,
        COALESCE(ps.payment_date, pp.payment_date) AS payment_date,
        ps.procedure_date,
        pp.payment_type_id,
        pp.payment_source,
        pp.payment_status,
        pp.process_status,
        ps.is_discount_flag,
        ps.discount_type,
        ps.unearned_type,
        ps.payplan_debit_type,
        pp.merchant_fee,
        pp.payment_notes,
        pp.check_number,
        pp.bank_branch,
        pp.created_by_user_id,
        COALESCE(ps.entry_date, pp.entry_date) AS entry_date,
        COALESCE(ps.updated_at, pp.updated_at) AS updated_at,
        pp.deposit_id,
        pp.external_id,
        pp.is_cc_completed_flag,
        pp.recurring_charge_date,
        pp.receipt_text,
        NULL AS carrier_name,
        NULL AS is_partial,
        NULL AS payment_group_id,
        NULL AS insurance_subscriber_id,
        NULL AS claim_id,
        NULL AS plan_id,
        NULL AS deductible_applied,
        NULL AS write_off,
        NULL AS allowed_override,
        NULL AS copay_amount,
        NULL AS paid_other_insurance,
        NULL AS base_estimate,
        NULL AS insurance_estimate_total,
        NULL AS status,
        NULL AS percentage,
        NULL AS is_transfer,
        NULL AS is_overpay,
        NULL AS remarks,
        NULL AS code_sent,
        NULL AS estimate_note
    FROM payment_splits ps
    LEFT JOIN patient_payments pp 
        ON ps.payment_id = pp.payment_id

    UNION ALL

    -- Insurance claim payments
    SELECT
        cp.claim_procedure_id AS payment_allocation_id,
        'INSURANCE' AS payment_source_type,
        ip.claim_payment_id AS payment_id,
        cp.patient_id,
        cp.clinic_id,
        cp.provider_id,
        cp.procedure_id,
        NULL AS adjustment_id,
        cp.payment_plan_id AS payplan_id,
        NULL AS payplan_charge_id,
        cp.insurance_payment_amount AS split_amount,
        COALESCE(cp.insurance_finalized_date, ip.check_date) AS payment_date,
        cp.procedure_date,
        ip.payment_type_id,
        NULL AS payment_source,
        NULL AS payment_status,
        NULL AS process_status,
        FALSE AS is_discount_flag,
        NULL AS discount_type,
        NULL AS unearned_type,
        NULL AS payplan_debit_type,
        NULL AS merchant_fee,
        ip.note AS payment_notes,
        ip.check_number,
        ip.bank_branch,
        ip.created_by_user_id,
        COALESCE(cp.claim_procedure_date, ip.created_date) AS entry_date,
        COALESCE(cp.last_modified_at, ip.last_modified_at) AS updated_at,
        ip.deposit_id,
        NULL AS external_id,
        NULL AS is_cc_completed_flag,
        NULL AS recurring_charge_date,
        NULL AS receipt_text,
        ip.carrier_name,
        ip.is_partial,
        ip.payment_group_id,
        cp.insurance_subscriber_id,
        cp.claim_id,
        cp.plan_id,
        cp.deductible_applied,
        cp.write_off,
        cp.allowed_override,
        cp.copay_amount,
        cp.paid_other_insurance,
        cp.base_estimate,
        cp.insurance_estimate_total,
        cp.status,
        cp.percentage,
        cp.is_transfer,
        cp.is_overpay,
        cp.remarks,
        cp.code_sent,
        cp.estimate_note
    FROM claim_procedures cp
    LEFT JOIN insurance_payments ip 
        ON cp.claim_payment_id = ip.claim_payment_id
)

SELECT
    pa.*,
    -- Payment type descriptions
    CASE
        WHEN pa.payment_source_type = 'PATIENT' THEN
            CASE pa.payment_type_id
                WHEN 0 THEN 'Administrative'
                WHEN 69 THEN 'High Value Payment'
                WHEN 70 THEN 'Regular Payment'
                WHEN 71 THEN 'Standard Payment'
                WHEN 72 THEN 'Refund'
                WHEN 391 THEN 'High Value Payment'
                WHEN 412 THEN 'New Payment Type'
                WHEN 417 THEN 'Special Case'
                WHEN 574 THEN 'Very High Value'
                WHEN 634 THEN 'New Payment Type'
                ELSE 'Unknown'
            END
        WHEN pa.payment_source_type = 'INSURANCE' THEN
            CASE pa.payment_type_id
                WHEN 1 THEN 'Insurance Check'
                WHEN 2 THEN 'Electronic Payment'
                WHEN 3 THEN 'Insurance Credit'
                ELSE 'Unknown Insurance Payment'
            END
    END AS payment_type_description,
    
    -- Payment status descriptions
    CASE
        WHEN pa.payment_source_type = 'PATIENT' THEN
            CASE pa.payment_status
                WHEN 0 THEN 'Pending'
                WHEN 1 THEN 'Completed'
                WHEN 2 THEN 'Failed'
                WHEN 3 THEN 'Voided'
                ELSE 'Unknown'
            END
        ELSE 'N/A'
    END AS payment_status_description,
    
    -- Process status descriptions
    CASE
        WHEN pa.payment_source_type = 'PATIENT' THEN
            CASE pa.process_status
                WHEN 0 THEN 'Not Processed'
                WHEN 1 THEN 'Processing'
                WHEN 2 THEN 'Processed'
                WHEN 3 THEN 'Error'
                ELSE 'Unknown'
            END
        ELSE 'N/A'
    END AS process_status_description,
    
    -- Split type descriptions
    CASE
        WHEN pa.unearned_type = 288 THEN 'Unearned Revenue'
        WHEN pa.unearned_type = 439 THEN 'Treatment Plan Prepayment'
        WHEN pa.is_discount_flag THEN 'Discount'
        WHEN pa.payplan_id IS NOT NULL THEN 'Payment Plan'
        ELSE 'Normal Payment'
    END AS split_type_description,
    
    -- AR calculation flags
    CASE
        WHEN pa.payment_date <= CURRENT_DATE THEN TRUE
        ELSE FALSE
    END AS include_in_ar,
    
    -- Tracking fields
    CURRENT_TIMESTAMP AS model_created_at,
    CURRENT_TIMESTAMP AS model_updated_at
FROM payment_allocations pa 