{{ config(
    materialized='incremental',
    unique_key='payment_allocation_id',
    schema='intermediate'
) }}

/*
    Intermediate model for payment allocations
    Connects payments with their allocations across procedures and insurance claims
    Part of System C: Payment Allocation & Reconciliation
    
    This model combines:
    1. Patient payments and their allocations
    2. Insurance claim payments and their allocations
    3. Payment plan allocations
*/

WITH PaymentDefinitions AS (
    SELECT 
        definition_id,
        item_name,
        item_value,
        category_id
    FROM {{ ref('stg_opendental__definition') }}
    WHERE category_id IN (1, 2)  -- Payment type categories
),

PatientPayments AS (
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

InsurancePayments AS (
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

ClaimProcedures AS (
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
PaymentAllocations AS (
    -- Patient payments
    SELECT
        pp.payment_id AS payment_allocation_id,
        'PATIENT' AS payment_source_type,
        pp.payment_id,
        pp.patient_id,
        pp.clinic_id,
        NULL AS provider_id,
        NULL AS procedure_id,
        NULL AS adjustment_id,
        NULL AS payplan_id,
        NULL AS payplan_charge_id,
        pp.payment_amount AS split_amount,
        pp.payment_date,
        NULL AS procedure_date,
        pp.payment_type_id,
        pp.payment_source,
        pp.payment_status,
        pp.process_status,
        FALSE AS is_discount_flag,
        NULL AS discount_type,
        NULL AS unearned_type,
        NULL AS payplan_debit_type,
        pp.merchant_fee,
        pp.payment_notes,
        pp.check_number,
        pp.bank_branch,
        pp.created_by_user_id,
        pp.entry_date,
        pp.updated_at,
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
    FROM PatientPayments pp

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
    FROM ClaimProcedures cp
    LEFT JOIN InsurancePayments ip 
        ON cp.claim_payment_id = ip.claim_payment_id
)

SELECT
    pa.*,
    -- Payment type descriptions from definition table
    pd.item_name AS payment_type_description,
    
    -- Payment status descriptions from definition table
    psd.item_name AS payment_status_description,
    
    -- Process status descriptions from definition table
    prsd.item_name AS process_status_description,
    
    -- AR calculation flags
    CASE
        WHEN pa.payment_date <= CURRENT_DATE THEN TRUE
        ELSE FALSE
    END AS include_in_ar,
    
    -- Tracking fields
    CURRENT_TIMESTAMP AS model_created_at,
    CURRENT_TIMESTAMP AS model_updated_at
FROM PaymentAllocations pa
LEFT JOIN PaymentDefinitions pd
    ON pa.payment_type_id = pd.definition_id
    AND pd.category_id = 1  -- Payment type category
LEFT JOIN PaymentDefinitions psd
    ON pa.payment_status = psd.definition_id
    AND psd.category_id = 2  -- Payment status category
LEFT JOIN PaymentDefinitions prsd
    ON pa.process_status = prsd.definition_id
    AND prsd.category_id = 2  -- Process status category 