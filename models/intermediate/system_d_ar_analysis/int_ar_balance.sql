{{ config(
    materialized='incremental',
    unique_key='ar_balance_id',
    schema='intermediate'
) }}

/*
    Intermediate model for AR balance analysis
    Part of System D: AR Analysis
    
    This model:
    1. Creates procedure-level AR balances
    2. Uses shared calculations for aging and payments
    3. Tracks insurance vs patient responsibility
    4. Supports AR reporting and analysis
    
    Note: Handles duplicate claims by preserving unique procedures while
    eliminating duplicate claim records. This is necessary due to source
    system data quality issues where some claims appear multiple times.
*/

WITH ProcedureInfo AS MATERIALIZED (
    SELECT DISTINCT ON (pc.procedure_id, cp.claim_id)
        pc.procedure_id,
        pc.patient_id,
        pc.provider_id,
        pc.procedure_date,
        pc.procedure_code,
        pc.procedure_fee,
        pc.procedure_status,
        pc.procedure_description,
        ci.claim_id,
        ci.claim_status,
        ci.received_date AS claim_date,
        ci.received_date,
        cr.carrier_name,
        ci.insurance_payment_estimate AS insurance_estimate
    FROM {{ ref('int_procedure_complete') }} pc
    LEFT JOIN {{ ref('stg_opendental__claimproc') }} cp
        ON pc.procedure_id = cp.procedure_id
    LEFT JOIN {{ ref('stg_opendental__claim') }} ci
        ON cp.claim_id = ci.claim_id
    LEFT JOIN {{ ref('stg_opendental__carrier') }} cr
        ON ci.plan_id = cr.carrier_id
    WHERE pc.procedure_status IN (2, 8)  -- 2 = Completed, 8 = In Progress
    ORDER BY pc.procedure_id, cp.claim_id, ci.received_date DESC
),

BalanceCalculations AS (
    SELECT
        sc.patient_id,
        sc.procedure_id,
        sc.transaction_date,
        sc.amount,
        sc.aging_bucket,
        sc.days_outstanding,
        SUM(sc.amount) OVER (
            PARTITION BY sc.patient_id, sc.procedure_id 
            ORDER BY sc.transaction_date
        ) AS running_balance,
        SUM(CASE WHEN sc.is_insurance_payment THEN sc.payment_amount ELSE 0 END) 
            OVER (PARTITION BY sc.patient_id, sc.procedure_id) AS insurance_payment_amount,
        SUM(CASE WHEN sc.is_patient_payment THEN sc.payment_amount ELSE 0 END) 
            OVER (PARTITION BY sc.patient_id, sc.procedure_id) AS patient_payment_amount,
        SUM(COALESCE(sc.adjustment_amount, 0)) OVER (
            PARTITION BY sc.patient_id, sc.procedure_id
        ) AS total_adjustment_amount,
        MAX(CASE WHEN sc.is_insurance_payment THEN sc.payment_date END) 
            OVER (PARTITION BY sc.patient_id, sc.procedure_id) AS last_insurance_payment_date,
        MAX(CASE WHEN sc.is_patient_payment THEN sc.payment_date END) 
            OVER (PARTITION BY sc.patient_id, sc.procedure_id) AS last_patient_payment_date
    FROM {{ ref('int_ar_shared_calculations') }} sc
),

ARBalances AS (
    SELECT
        pi.procedure_id,
        pi.patient_id,
        pi.provider_id,
        pi.procedure_date,
        pi.procedure_code,
        pi.procedure_description,
        pi.procedure_fee,
        
        -- Payment information
        COALESCE(bc.insurance_payment_amount, 0) AS insurance_payment_amount,
        COALESCE(bc.patient_payment_amount, 0) AS patient_payment_amount,
        COALESCE(bc.insurance_payment_amount, 0) + COALESCE(bc.patient_payment_amount, 0) AS total_payment_amount,
        
        -- Adjustment information
        COALESCE(bc.total_adjustment_amount, 0) AS total_adjustment_amount,
        
        -- Balance calculations
        pi.procedure_fee - 
            COALESCE(bc.insurance_payment_amount, 0) - 
            COALESCE(bc.patient_payment_amount, 0) - 
            COALESCE(bc.total_adjustment_amount, 0) AS current_balance,
        
        -- Insurance information
        COALESCE(pi.insurance_estimate, 0) AS insurance_estimate,
        CASE 
            WHEN pi.claim_id IS NOT NULL AND pi.claim_status IN ('P', 'R', 'S') THEN
                LEAST(COALESCE(pi.insurance_estimate, 0), pi.procedure_fee)
            ELSE 0
        END AS insurance_pending_amount,
        
        -- Patient responsibility calculation
        -- Note: Negative values are valid (see model documentation)
        -- Implementation handles standard -$1.00 copay from source
        pi.procedure_fee - 
            COALESCE(pi.insurance_estimate, 0) - 
            COALESCE(bc.patient_payment_amount, 0) - 
            COALESCE(bc.total_adjustment_amount, 0) AS patient_responsibility,
        
        -- Aging information
        bc.days_outstanding,
        bc.aging_bucket,
        
        -- Claim information
        pi.claim_id,
        pi.claim_status,
        CASE
            WHEN pi.claim_status = 'P' THEN 'PENDING'
            WHEN pi.claim_status = 'R' THEN 'RECEIVED'
            WHEN pi.claim_status = 'S' THEN 'SUPPLEMENTAL'
            WHEN pi.claim_status = 'D' THEN 'DENIED'
            WHEN pi.claim_status = 'C' THEN 'COMPLETED'
            ELSE 'UNKNOWN'
        END AS claim_status_description,
        pi.claim_date,
        pi.received_date,
        pi.carrier_name,
        
        -- Payment dates
        bc.last_insurance_payment_date,
        bc.last_patient_payment_date,
        GREATEST(
            COALESCE(bc.last_insurance_payment_date, '2000-01-01'), 
            COALESCE(bc.last_patient_payment_date, '2000-01-01')
        ) AS last_payment_date,
        
        -- Responsibility determination
        CASE
            WHEN pi.claim_id IS NOT NULL AND pi.claim_status IN ('P', 'R', 'S') THEN 'INSURANCE'
            ELSE 'PATIENT'
        END AS responsible_party,
        
        -- Include in AR flag
        CASE
            WHEN pi.procedure_fee - 
                COALESCE(bc.insurance_payment_amount, 0) - 
                COALESCE(bc.patient_payment_amount, 0) - 
                COALESCE(bc.total_adjustment_amount, 0) <= 0 THEN FALSE
            ELSE TRUE
        END AS include_in_ar,
        
        -- Metadata fields
        CURRENT_TIMESTAMP AS model_created_at,
        CURRENT_TIMESTAMP AS model_updated_at
        
    FROM ProcedureInfo pi
    LEFT JOIN BalanceCalculations bc
        ON pi.patient_id = bc.patient_id
        AND pi.procedure_id = bc.procedure_id
)

-- Final selection
SELECT
    ROW_NUMBER() OVER (ORDER BY patient_id, procedure_id) AS ar_balance_id,
    procedure_id,
    patient_id,
    provider_id,
    procedure_date,
    procedure_code,
    procedure_description,
    procedure_fee,
    insurance_payment_amount,
    patient_payment_amount,
    total_payment_amount,
    total_adjustment_amount,
    current_balance,
    insurance_estimate,
    insurance_pending_amount,
    patient_responsibility,
    days_outstanding,
    aging_bucket,
    claim_id,
    claim_status,
    claim_status_description,
    claim_date,
    received_date,
    carrier_name,
    last_insurance_payment_date,
    last_patient_payment_date,
    last_payment_date,
    responsible_party,
    include_in_ar,
    model_created_at,
    model_updated_at
FROM ARBalances
WHERE include_in_ar = TRUE
OR current_balance <> 0