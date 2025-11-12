-- Verify Collection Rate calculation
-- Should be between 80-120% for a healthy practice

-- CRITICAL ISSUE: 
-- 1. mart_ar_summary only includes patients with total_balance > 0, excluding paid-off patients
-- 2. fact_claim only includes INSURANCE claims, missing direct-pay procedures
-- 
-- SOLUTION: For collection rate in a mostly direct-pay practice, we MUST use:
-- - fact_procedure: ALL procedures (both insurance and direct-pay) = Production/Billed
-- - fact_payment: ALL payments (both insurance and patient) = Collections
-- 
-- Collection Rate = Total Payments / Total Procedures (Production)

-- 1. Compare different data sources to understand the gap
WITH mart_with_balance AS (
    SELECT 
        SUM(billed_last_year) as total_billed,
        SUM(total_payments_last_year) as total_payments,
        COUNT(*) as patient_count
    FROM raw_marts.mart_ar_summary
    WHERE total_balance > 0
),
insurance_claims_only AS (
    SELECT 
        SUM(billed_amount) as total_billed,
        COUNT(DISTINCT patient_id) as patient_count
    FROM raw_marts.fact_claim
    WHERE claim_date >= CURRENT_DATE - INTERVAL '365 days'
),
all_procedures AS (
    SELECT 
        SUM(actual_fee) as total_billed,
        COUNT(DISTINCT patient_id) as patient_count
    FROM raw_marts.fact_procedure
    WHERE date_id IN (
        SELECT date_id 
        FROM raw_marts.dim_date 
        WHERE date_day >= CURRENT_DATE - INTERVAL '365 days'
    )
)
SELECT 
    'mart_ar_summary (balance > 0)' as source,
    total_billed,
    total_payments,
    patient_count,
    ROUND((total_payments / NULLIF(total_billed, 0) * 100)::numeric, 1) as collection_rate_pct
FROM mart_with_balance

UNION ALL

SELECT 
    'Insurance claims only (fact_claim)' as source,
    total_billed,
    NULL::numeric as total_payments,
    patient_count,
    NULL::numeric as collection_rate_pct
FROM insurance_claims_only

UNION ALL

SELECT 
    'ALL procedures (fact_procedure) - CORRECT' as source,
    total_billed,
    NULL::numeric as total_payments,
    patient_count,
    NULL::numeric as collection_rate_pct
FROM all_procedures;

-- 2. Calculate collection rate using ALL procedures and ALL payments (CORRECT approach)
-- This captures ALL production (both insurance and direct-pay) and ALL collections
WITH production_totals AS (
    SELECT 
        SUM(fp.actual_fee) as total_production,
        COUNT(DISTINCT fp.patient_id) as patient_count
    FROM raw_marts.fact_procedure fp
    INNER JOIN raw_marts.dim_date dd ON fp.date_id = dd.date_id
    WHERE dd.date_day >= CURRENT_DATE - INTERVAL '365 days'
),
payment_totals AS (
    SELECT 
        SUM(payment_amount) as total_collections,
        COUNT(DISTINCT patient_id) as patient_count
    FROM raw_marts.fact_payment
    WHERE payment_date >= CURRENT_DATE - INTERVAL '365 days'
        AND payment_direction = 'Income'  -- Exclude refunds
),
collection_rate AS (
    SELECT 
        pt.total_production,
        pay.total_collections,
        pt.patient_count as patients_with_procedures,
        pay.patient_count as patients_with_payments,
        ROUND((pay.total_collections / NULLIF(pt.total_production, 0) * 100)::numeric, 1) as collection_rate_pct
    FROM production_totals pt
    CROSS JOIN payment_totals pay
)
SELECT 
    'Collection Rate (All Procedures vs All Payments)' as metric,
    total_production as total_billed,
    total_collections as total_payments,
    collection_rate_pct,
    patients_with_procedures,
    patients_with_payments,
    CASE 
        WHEN collection_rate_pct > 200 THEN '❌ TOO HIGH - Data issue?'
        WHEN collection_rate_pct BETWEEN 80 AND 120 THEN '✅ NORMAL'
        WHEN collection_rate_pct BETWEEN 50 AND 80 THEN '⚠️ LOW'
        ELSE '❌ CHECK VALUES'
    END as status
FROM collection_rate;

-- 3. Breakdown by source for validation
SELECT 
    'Insurance claims (fact_claim)' as source,
    SUM(billed_amount) as total_billed,
    COUNT(DISTINCT patient_id) as patient_count,
    NULL::numeric as total_payments
FROM raw_marts.fact_claim
WHERE claim_date >= CURRENT_DATE - INTERVAL '365 days'

UNION ALL

SELECT 
    'ALL procedures (fact_procedure)' as source,
    SUM(actual_fee) as total_billed,
    COUNT(DISTINCT patient_id) as patient_count,
    NULL::numeric as total_payments
FROM raw_marts.fact_procedure fp
INNER JOIN raw_marts.dim_date dd ON fp.date_id = dd.date_id
WHERE dd.date_day >= CURRENT_DATE - INTERVAL '365 days'

UNION ALL

SELECT 
    'ALL payments (fact_payment)' as source,
    NULL::numeric as total_billed,
    COUNT(DISTINCT patient_id) as patient_count,
    SUM(payment_amount) as total_payments
FROM raw_marts.fact_payment
WHERE payment_date >= CURRENT_DATE - INTERVAL '365 days'
    AND payment_direction = 'Income';

-- 4. Show the gap: Insurance claims vs All procedures
-- This demonstrates why fact_claim is insufficient for a direct-pay practice
SELECT 
    'Insurance claims only (fact_claim)' as category,
    COUNT(DISTINCT patient_id) as patient_count,
    SUM(billed_amount) as total_billed
FROM raw_marts.fact_claim
WHERE claim_date >= CURRENT_DATE - INTERVAL '365 days'

UNION ALL

SELECT 
    'ALL procedures (fact_procedure)' as category,
    COUNT(DISTINCT patient_id) as patient_count,
    SUM(actual_fee) as total_billed
FROM raw_marts.fact_procedure fp
INNER JOIN raw_marts.dim_date dd ON fp.date_id = dd.date_id
WHERE dd.date_day >= CURRENT_DATE - INTERVAL '365 days'

UNION ALL

SELECT 
    'Direct-pay procedures (missing from fact_claim)' as category,
    COUNT(DISTINCT fp.patient_id) as patient_count,
    SUM(fp.actual_fee) as total_billed
FROM raw_marts.fact_procedure fp
INNER JOIN raw_marts.dim_date dd ON fp.date_id = dd.date_id
LEFT JOIN raw_marts.fact_claim fc 
    ON fp.patient_id = fc.patient_id 
    AND dd.date_day = fc.claim_date
WHERE dd.date_day >= CURRENT_DATE - INTERVAL '365 days'
    AND fc.claim_id IS NULL;  -- Procedures without insurance claims

