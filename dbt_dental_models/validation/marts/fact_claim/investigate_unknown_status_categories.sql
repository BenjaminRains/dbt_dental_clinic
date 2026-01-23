-- Investigation: Unknown Status Categories in fact_claim
-- Purpose: Understand what conditions lead to "Unknown" values in payment_status_category and billing_status_category
-- Date: 2026-01-23
-- 
-- Test Failures:
-- - payment_status_category = 'Unknown': 62,289 records
-- - billing_status_category = 'Unknown': 50,680 records

-- ============================================================================
-- QUERY 1: Payment Status Category - Unknown Analysis
-- ============================================================================
-- Investigate what claim_status values and conditions result in 'Unknown'

-- Query 1.1: Count of Unknown payment status records
SELECT 
    'Payment Status Category - Unknown' as investigation_type,
    COUNT(*) as total_unknown_records,
    COUNT(DISTINCT claim_id) as distinct_claims,
    COUNT(DISTINCT patient_id) as distinct_patients
FROM marts.fact_claim
WHERE payment_status_category = 'Unknown';

-- Query 1.2: Breakdown by claim_status
SELECT 
    'Payment Status - Breakdown by claim_status' as analysis,
    claim_status,
    COUNT(*) as record_count,
    COUNT(DISTINCT claim_id) as distinct_claims,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as pct_of_unknowns,
    -- Additional context
    COUNT(CASE WHEN paid_amount > 0 THEN 1 END) as records_with_payment,
    COUNT(CASE WHEN claim_id = 0 THEN 1 END) as pre_auth_claims,
    COUNT(CASE WHEN claim_id != 0 THEN 1 END) as regular_claims
FROM marts.fact_claim
WHERE payment_status_category = 'Unknown'
GROUP BY claim_status
ORDER BY record_count DESC;

-- Query 1.3: Sample records for each claim_status
SELECT 
    'Payment Status - Sample Records' as analysis,
    claim_id,
    procedure_id,
    claim_status,
    paid_amount,
    billed_amount,
    payment_status_category,
    claim_date,
    claim_type
FROM marts.fact_claim
WHERE payment_status_category = 'Unknown'
ORDER BY claim_status, claim_date DESC
LIMIT 50;

-- ============================================================================
-- QUERY 2: Billing Status Category - Unknown Analysis
-- ============================================================================
-- Investigate what conditions result in 'Unknown' billing status

-- Query 2.1: Count of Unknown billing status records
SELECT 
    'Billing Status Category - Unknown' as investigation_type,
    COUNT(*) as total_unknown_records,
    COUNT(DISTINCT claim_id) as distinct_claims,
    COUNT(DISTINCT patient_id) as distinct_patients
FROM marts.fact_claim
WHERE billing_status_category = 'Unknown';

-- Query 2.2: Breakdown by billed_amount and no_bill_insurance
SELECT 
    'Billing Status - Breakdown by conditions' as analysis,
    CASE 
        WHEN billed_amount = 0 THEN 'billed_amount = 0'
        WHEN billed_amount > 0 THEN 'billed_amount > 0'
        ELSE 'billed_amount IS NULL'
    END as billed_amount_category,
    CASE 
        WHEN no_bill_insurance = true THEN 'no_bill_insurance = true'
        WHEN no_bill_insurance = false THEN 'no_bill_insurance = false'
        WHEN no_bill_insurance IS NULL THEN 'no_bill_insurance IS NULL'
        ELSE 'no_bill_insurance = other'
    END as no_bill_insurance_status,
    COUNT(*) as record_count,
    COUNT(DISTINCT claim_id) as distinct_claims,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as pct_of_unknowns
FROM marts.fact_claim
WHERE billing_status_category = 'Unknown'
GROUP BY 
    CASE 
        WHEN billed_amount = 0 THEN 'billed_amount = 0'
        WHEN billed_amount > 0 THEN 'billed_amount > 0'
        ELSE 'billed_amount IS NULL'
    END,
    CASE 
        WHEN no_bill_insurance = true THEN 'no_bill_insurance = true'
        WHEN no_bill_insurance = false THEN 'no_bill_insurance = false'
        WHEN no_bill_insurance IS NULL THEN 'no_bill_insurance IS NULL'
        ELSE 'no_bill_insurance = other'
    END
ORDER BY record_count DESC;

-- Query 2.3: Sample records for billing status unknown
SELECT 
    'Billing Status - Sample Records' as analysis,
    claim_id,
    procedure_id,
    billed_amount,
    no_bill_insurance,
    billing_status_category,
    claim_date,
    claim_status,
    procedure_code
FROM marts.fact_claim
WHERE billing_status_category = 'Unknown'
ORDER BY billed_amount, claim_date DESC
LIMIT 50;

-- ============================================================================
-- QUERY 3: Combined Analysis - Records with BOTH Unknown Statuses
-- ============================================================================

-- Query 3.1: Count of records with both statuses unknown
SELECT 
    'Both Statuses Unknown' as analysis,
    COUNT(*) as record_count,
    COUNT(DISTINCT claim_id) as distinct_claims,
    COUNT(DISTINCT patient_id) as distinct_patients
FROM marts.fact_claim
WHERE payment_status_category = 'Unknown'
    AND billing_status_category = 'Unknown';

-- Query 3.2: Breakdown of records with both unknown
SELECT 
    'Both Unknown - Breakdown' as analysis,
    claim_status,
    CASE 
        WHEN billed_amount = 0 THEN 'billed_amount = 0'
        WHEN billed_amount > 0 THEN 'billed_amount > 0'
        ELSE 'billed_amount IS NULL'
    END as billed_amount_category,
    CASE 
        WHEN no_bill_insurance = true THEN 'no_bill_insurance = true'
        WHEN no_bill_insurance = false THEN 'no_bill_insurance = false'
        WHEN no_bill_insurance IS NULL THEN 'no_bill_insurance IS NULL'
    END as no_bill_insurance_status,
    COUNT(*) as record_count
FROM marts.fact_claim
WHERE payment_status_category = 'Unknown'
    AND billing_status_category = 'Unknown'
GROUP BY claim_status, 
    CASE 
        WHEN billed_amount = 0 THEN 'billed_amount = 0'
        WHEN billed_amount > 0 THEN 'billed_amount > 0'
        ELSE 'billed_amount IS NULL'
    END,
    CASE 
        WHEN no_bill_insurance = true THEN 'no_bill_insurance = true'
        WHEN no_bill_insurance = false THEN 'no_bill_insurance = false'
        WHEN no_bill_insurance IS NULL THEN 'no_bill_insurance IS NULL'
    END
ORDER BY record_count DESC;

-- ============================================================================
-- QUERY 4: Payment Status - Claim Status Distribution (All Records)
-- ============================================================================
-- Compare claim_status distribution for Unknown vs Known payment statuses

-- Query 4: Payment Status - Claim Status Comparison
SELECT 
    'Payment Status - Claim Status Comparison' as analysis,
    claim_status,
    COUNT(CASE WHEN payment_status_category = 'Unknown' THEN 1 END) as unknown_count,
    COUNT(CASE WHEN payment_status_category != 'Unknown' THEN 1 END) as known_count,
    COUNT(*) as total_count,
    ROUND(100.0 * COUNT(CASE WHEN payment_status_category = 'Unknown' THEN 1 END) / COUNT(*), 2) as pct_unknown
FROM marts.fact_claim
GROUP BY claim_status
ORDER BY total_count DESC;

-- ============================================================================
-- QUERY 5: Billing Status - Condition Analysis (All Records)
-- ============================================================================
-- Compare conditions for Unknown vs Known billing statuses

-- Query 5: Billing Status - Condition Comparison
SELECT 
    'Billing Status - Condition Comparison' as analysis,
    CASE 
        WHEN billed_amount = 0 THEN 'billed_amount = 0'
        WHEN billed_amount > 0 THEN 'billed_amount > 0'
        ELSE 'billed_amount IS NULL'
    END as billed_amount_category,
    CASE 
        WHEN no_bill_insurance = true THEN 'no_bill_insurance = true'
        WHEN no_bill_insurance = false THEN 'no_bill_insurance = false'
        WHEN no_bill_insurance IS NULL THEN 'no_bill_insurance IS NULL'
        ELSE 'no_bill_insurance = other'
    END as no_bill_insurance_status,
    COUNT(CASE WHEN billing_status_category = 'Unknown' THEN 1 END) as unknown_count,
    COUNT(CASE WHEN billing_status_category != 'Unknown' THEN 1 END) as known_count,
    COUNT(*) as total_count,
    ROUND(100.0 * COUNT(CASE WHEN billing_status_category = 'Unknown' THEN 1 END) / COUNT(*), 2) as pct_unknown
FROM marts.fact_claim
GROUP BY 
    CASE 
        WHEN billed_amount = 0 THEN 'billed_amount = 0'
        WHEN billed_amount > 0 THEN 'billed_amount > 0'
        ELSE 'billed_amount IS NULL'
    END,
    CASE 
        WHEN no_bill_insurance = true THEN 'no_bill_insurance = true'
        WHEN no_bill_insurance = false THEN 'no_bill_insurance = false'
        WHEN no_bill_insurance IS NULL THEN 'no_bill_insurance IS NULL'
        ELSE 'no_bill_insurance = other'
    END
ORDER BY total_count DESC;

-- ============================================================================
-- QUERY 6: Payment Status - Pre-auth Claims Analysis
-- ============================================================================
-- Check if pre-auth claims (claim_id = 0) are contributing to Unknown

-- Query 6: Payment Status - Pre-auth Analysis
SELECT 
    'Payment Status - Pre-auth Analysis' as analysis,
    CASE 
        WHEN claim_id = 0 THEN 'Pre-auth (claim_id = 0)'
        ELSE 'Regular Claim (claim_id != 0)'
    END as claim_type,
    payment_status_category,
    COUNT(*) as record_count,
    COUNT(DISTINCT claim_id) as distinct_claims
FROM marts.fact_claim
WHERE payment_status_category = 'Unknown'
GROUP BY 
    CASE 
        WHEN claim_id = 0 THEN 'Pre-auth (claim_id = 0)'
        ELSE 'Regular Claim (claim_id != 0)'
    END,
    payment_status_category
ORDER BY record_count DESC;

-- ============================================================================
-- QUERY 7: Summary Statistics
-- ============================================================================

-- Query 7: Summary Statistics
SELECT 
    'Summary Statistics' as analysis,
    COUNT(*) as total_records,
    COUNT(CASE WHEN payment_status_category = 'Unknown' THEN 1 END) as payment_unknown_count,
    ROUND(100.0 * COUNT(CASE WHEN payment_status_category = 'Unknown' THEN 1 END) / COUNT(*), 2) as payment_unknown_pct,
    COUNT(CASE WHEN billing_status_category = 'Unknown' THEN 1 END) as billing_unknown_count,
    ROUND(100.0 * COUNT(CASE WHEN billing_status_category = 'Unknown' THEN 1 END) / COUNT(*), 2) as billing_unknown_pct,
    COUNT(CASE WHEN payment_status_category = 'Unknown' AND billing_status_category = 'Unknown' THEN 1 END) as both_unknown_count,
    ROUND(100.0 * COUNT(CASE WHEN payment_status_category = 'Unknown' AND billing_status_category = 'Unknown' THEN 1 END) / COUNT(*), 2) as both_unknown_pct
FROM marts.fact_claim;
