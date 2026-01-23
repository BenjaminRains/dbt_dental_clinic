-- ============================================================================
-- Fact Claim Validation Queries
-- ============================================================================
-- Purpose: Comprehensive validation queries for marts.fact_claim
-- Database: opendental_analytics (PostgreSQL)
-- Usage: Run these queries to validate fact_claim data quality and accuracy
-- ============================================================================

-- ============================================================================
-- PHASE 1: DATA COMPLETENESS VALIDATION
-- ============================================================================

-- 1.1 Row Count Reconciliation
-- Updated to match new model logic: includes all claimprocs with valid ProcDate,
-- even if their associated claims are filtered out in staging
WITH source_count AS (
    -- Count all claimprocs in staging (matching model logic)
    SELECT COUNT(*) as source_rows
    FROM staging.stg_opendental__claimproc cp
    WHERE cp.procedure_date >= '2023-01-01'
),
fact_count AS (
    SELECT COUNT(*) as fact_rows
    FROM marts.fact_claim
    WHERE claim_date >= '2023-01-01'
),
-- Diagnostic: Check for claimprocs without matching claims in staging
claimprocs_without_claims AS (
    SELECT COUNT(*) as count_without_claims
    FROM int.int_claim_details icd
    WHERE icd.claim_date >= '2023-01-01'
        AND NOT EXISTS (
            SELECT 1 
            FROM staging.stg_opendental__claim sc
            WHERE sc.claim_id = icd.claim_id
        )
),
-- Diagnostic: Check for duplicates
duplicate_check AS (
    SELECT 
        claim_id,
        procedure_id,
        claim_procedure_id,
        COUNT(*) as dup_count
    FROM int.int_claim_details
    WHERE claim_date >= '2023-01-01'
    GROUP BY claim_id, procedure_id, claim_procedure_id
    HAVING COUNT(*) > 1
),
-- Legacy comparison: original validation against raw data with INNER JOIN
legacy_source_count AS (
    SELECT COUNT(*) as legacy_rows
    FROM raw.claimproc cp
    INNER JOIN raw.claim c ON cp."ClaimNum" = c."ClaimNum"
    WHERE cp."ProcDate" >= '2023-01-01'
)
SELECT 
    s.source_rows,
    f.fact_rows,
    (s.source_rows - f.fact_rows) as difference,
    cwc.count_without_claims as claimprocs_without_matching_claims,
    (SELECT COUNT(*) FROM duplicate_check) as duplicate_composite_keys,
    ls.legacy_rows as legacy_source_count_with_inner_join,
    CASE 
        WHEN s.source_rows = f.fact_rows THEN 'PASS'
        WHEN ABS(s.source_rows - f.fact_rows) / NULLIF(s.source_rows, 0) < 0.01 THEN 'WARN - <1% variance'
        ELSE 'FAIL - Significant variance'
    END as validation_status
FROM source_count s
CROSS JOIN fact_count f
CROSS JOIN claimprocs_without_claims cwc
CROSS JOIN legacy_source_count ls;

-- 1.1.1 Diagnostic: Missing Rows Root Cause Analysis
-- Run these queries to identify why rows are missing from fact_claim

-- Diagnostic 1: Check for NULL key values in source
SELECT 
    'NULL ClaimNum' as issue_type,
    COUNT(*) as record_count
FROM raw.claimproc cp
INNER JOIN raw.claim c ON cp."ClaimNum" = c."ClaimNum"
WHERE cp."ProcDate" >= '2023-01-01'
    AND (cp."ClaimNum" IS NULL OR cp."ProcNum" IS NULL OR cp."ClaimProcNum" IS NULL)
UNION ALL
SELECT 
    'Missing in int_claim_details' as issue_type,
    COUNT(*) as record_count
FROM raw.claimproc cp
INNER JOIN raw.claim c ON cp."ClaimNum" = c."ClaimNum"
WHERE cp."ProcDate" >= '2023-01-01'
    AND NOT EXISTS (
        SELECT 1 
        FROM int.int_claim_details icd
        WHERE icd.claim_id = cp."ClaimNum"
            AND icd.procedure_id = cp."ProcNum"
            AND icd.claim_procedure_id = cp."ClaimProcNum"
    )
UNION ALL
SELECT 
    'In int_claim_details but not fact_claim' as issue_type,
    COUNT(*) as record_count
FROM int.int_claim_details icd
WHERE icd.claim_date >= '2023-01-01'
    AND NOT EXISTS (
        SELECT 1 
        FROM marts.fact_claim fc
        WHERE fc.claim_id = icd.claim_id
            AND fc.procedure_id = icd.procedure_id
            AND fc.claim_procedure_id = icd.claim_procedure_id
    );

-- Diagnostic 2: Check for duplicate records that get deduplicated
WITH int_with_rn AS (
    SELECT 
        claim_id,
        procedure_id,
        claim_procedure_id,
        claim_date,
        ROW_NUMBER() OVER (
            PARTITION BY claim_id, procedure_id, claim_procedure_id
            ORDER BY claim_date DESC
        ) as rn,
        COUNT(*) OVER (
            PARTITION BY claim_id, procedure_id, claim_procedure_id
        ) as duplicate_count
    FROM int.int_claim_details
    WHERE claim_date >= '2023-01-01'
)
SELECT 
    'Duplicates filtered by rn=1' as issue_type,
    COUNT(*) as record_count,
    COUNT(DISTINCT claim_id || '-' || procedure_id || '-' || claim_procedure_id) as unique_keys
FROM int_with_rn
WHERE rn > 1;

-- Diagnostic 3: Three-way comparison to identify where rows are lost
WITH source_count AS (
    SELECT COUNT(*) as cnt, 'Source (raw)' as layer
    FROM raw.claimproc cp
    INNER JOIN raw.claim c ON cp."ClaimNum" = c."ClaimNum"
    WHERE cp."ProcDate" >= '2023-01-01'
),
int_count AS (
    SELECT COUNT(*) as cnt, 'Intermediate (int_claim_details)' as layer
    FROM int.int_claim_details
    WHERE claim_date >= '2023-01-01'
),
fact_count AS (
    SELECT COUNT(*) as cnt, 'Fact (fact_claim)' as layer
    FROM marts.fact_claim
    WHERE claim_date >= '2023-01-01'
),
-- Check for duplicates in int_claim_details
duplicate_check AS (
    SELECT 
        claim_id,
        procedure_id,
        claim_procedure_id,
        COUNT(*) as dup_count
    FROM int.int_claim_details
    WHERE claim_date >= '2023-01-01'
    GROUP BY claim_id, procedure_id, claim_procedure_id
    HAVING COUNT(*) > 1
),
-- Check for claimprocs without matching claims
claimprocs_without_claims AS (
    SELECT COUNT(*) as cnt, 'Claimprocs without matching claims' as layer
    FROM int.int_claim_details icd
    WHERE icd.claim_date >= '2023-01-01'
        AND NOT EXISTS (
            SELECT 1 
            FROM staging.stg_opendental__claim sc
            WHERE sc.claim_id = icd.claim_id
        )
)
SELECT * FROM source_count
UNION ALL
SELECT * FROM int_count
UNION ALL
SELECT * FROM fact_count
UNION ALL
SELECT (SELECT COUNT(*) FROM duplicate_check) as cnt, 'Duplicates in int_claim_details' as layer
UNION ALL
SELECT * FROM claimprocs_without_claims
UNION ALL
SELECT COALESCE((SELECT SUM(dup_count - 1) FROM duplicate_check), 0) as cnt, 'Extra rows from duplicates' as layer
ORDER BY layer;

-- Diagnostic 4: Why rows are missing in int_claim_details
-- Check if missing rows have claims that don't exist in staging
SELECT 
    'Claimproc without matching claim in staging' as issue_type,
    COUNT(*) as record_count
FROM raw.claimproc cp
INNER JOIN raw.claim c ON cp."ClaimNum" = c."ClaimNum"
WHERE cp."ProcDate" >= '2023-01-01'
    AND NOT EXISTS (
        SELECT 1 
        FROM int.int_claim_details icd
        WHERE icd.claim_id = cp."ClaimNum"
            AND icd.procedure_id = cp."ProcNum"
            AND icd.claim_procedure_id = cp."ClaimProcNum"
    )
    AND NOT EXISTS (
        SELECT 1 
        FROM raw.claim c2
        WHERE c2."ClaimNum" = cp."ClaimNum"
    )
UNION ALL
SELECT 
    'Claimproc with NULL procedure_id' as issue_type,
    COUNT(*) as record_count
FROM raw.claimproc cp
INNER JOIN raw.claim c ON cp."ClaimNum" = c."ClaimNum"
WHERE cp."ProcDate" >= '2023-01-01'
    AND NOT EXISTS (
        SELECT 1 
        FROM int.int_claim_details icd
        WHERE icd.claim_id = cp."ClaimNum"
            AND icd.procedure_id = cp."ProcNum"
            AND icd.claim_procedure_id = cp."ClaimProcNum"
    )
    AND cp."ProcNum" IS NULL;

-- Diagnostic 4b: Check if claims exist in staging but claimprocs don't match
SELECT 
    'Claim exists in staging but claimproc not in int_claim_details' as issue_type,
    COUNT(*) as record_count
FROM raw.claimproc cp
INNER JOIN raw.claim c ON cp."ClaimNum" = c."ClaimNum"
WHERE cp."ProcDate" >= '2023-01-01'
    AND NOT EXISTS (
        SELECT 1 
        FROM int.int_claim_details icd
        WHERE icd.claim_id = cp."ClaimNum"
            AND icd.procedure_id = cp."ProcNum"
            AND icd.claim_procedure_id = cp."ClaimProcNum"
    )
    AND EXISTS (
        SELECT 1 
        FROM staging.stg_opendental__claim sc
        WHERE sc.claim_id = cp."ClaimNum"
    )
    AND EXISTS (
        SELECT 1 
        FROM staging.stg_opendental__claimproc scp
        WHERE scp.claim_id = cp."ClaimNum"
            AND scp.procedure_id = cp."ProcNum"
            AND scp.claim_procedure_id = cp."ClaimProcNum"
    );

-- Diagnostic 4c: Sample the actual missing records to inspect
SELECT 
    cp."ClaimNum" as claim_id,
    cp."ProcNum" as procedure_id,
    cp."ClaimProcNum" as claim_procedure_id,
    cp."ProcDate" as proc_date,
    c."DateService" as claim_date,
    c."ClaimStatus" as claim_status,
    CASE 
        WHEN EXISTS (SELECT 1 FROM staging.stg_opendental__claim sc WHERE sc.claim_id = cp."ClaimNum") 
        THEN 'Yes' ELSE 'No' 
    END as claim_in_staging,
    CASE 
        WHEN EXISTS (SELECT 1 FROM staging.stg_opendental__claimproc scp 
                     WHERE scp.claim_id = cp."ClaimNum" 
                     AND scp.procedure_id = cp."ProcNum" 
                     AND scp.claim_procedure_id = cp."ClaimProcNum") 
        THEN 'Yes' ELSE 'No' 
    END as claimproc_in_staging
FROM raw.claimproc cp
INNER JOIN raw.claim c ON cp."ClaimNum" = c."ClaimNum"
WHERE cp."ProcDate" >= '2023-01-01'
    AND NOT EXISTS (
        SELECT 1 
        FROM int.int_claim_details icd
        WHERE icd.claim_id = cp."ClaimNum"
            AND icd.procedure_id = cp."ProcNum"
            AND icd.claim_procedure_id = cp."ClaimProcNum"
    )
LIMIT 20;

-- Diagnostic 4d: Root Cause - Invalid claim dates
-- Check if all missing rows have invalid claim dates
SELECT 
    'Missing rows with invalid claim date (0001-01-01)' as issue_type,
    COUNT(*) as record_count,
    COUNT(DISTINCT cp."ClaimNum") as unique_claims
FROM raw.claimproc cp
INNER JOIN raw.claim c ON cp."ClaimNum" = c."ClaimNum"
WHERE cp."ProcDate" >= '2023-01-01'
    AND NOT EXISTS (
        SELECT 1 
        FROM int.int_claim_details icd
        WHERE icd.claim_id = cp."ClaimNum"
            AND icd.procedure_id = cp."ProcNum"
            AND icd.claim_procedure_id = cp."ClaimProcNum"
    )
    AND c."DateService" = '0001-01-01'
UNION ALL
SELECT 
    'Missing rows with valid claim dates' as issue_type,
    COUNT(*) as record_count,
    COUNT(DISTINCT cp."ClaimNum") as unique_claims
FROM raw.claimproc cp
INNER JOIN raw.claim c ON cp."ClaimNum" = c."ClaimNum"
WHERE cp."ProcDate" >= '2023-01-01'
    AND NOT EXISTS (
        SELECT 1 
        FROM int.int_claim_details icd
        WHERE icd.claim_id = cp."ClaimNum"
            AND icd.procedure_id = cp."ProcNum"
            AND icd.claim_procedure_id = cp."ClaimProcNum"
    )
    AND c."DateService" != '0001-01-01';

-- Diagnostic 5: Why rows in int_claim_details are filtered out in fact_claim
-- Check for NULL key values that would be filtered
SELECT 
    'NULL claim_id in int_claim_details' as issue_type,
    COUNT(*) as record_count
FROM int.int_claim_details icd
WHERE icd.claim_date >= '2023-01-01'
    AND NOT EXISTS (
        SELECT 1 
        FROM marts.fact_claim fc
        WHERE fc.claim_id = icd.claim_id
            AND fc.procedure_id = icd.procedure_id
            AND fc.claim_procedure_id = icd.claim_procedure_id
    )
    AND icd.claim_id IS NULL
UNION ALL
SELECT 
    'NULL procedure_id in int_claim_details' as issue_type,
    COUNT(*) as record_count
FROM int.int_claim_details icd
WHERE icd.claim_date >= '2023-01-01'
    AND NOT EXISTS (
        SELECT 1 
        FROM marts.fact_claim fc
        WHERE fc.claim_id = icd.claim_id
            AND fc.procedure_id = icd.procedure_id
            AND fc.claim_procedure_id = icd.claim_procedure_id
    )
    AND icd.procedure_id IS NULL
UNION ALL
SELECT 
    'NULL claim_procedure_id in int_claim_details' as issue_type,
    COUNT(*) as record_count
FROM int.int_claim_details icd
WHERE icd.claim_date >= '2023-01-01'
    AND NOT EXISTS (
        SELECT 1 
        FROM marts.fact_claim fc
        WHERE fc.claim_id = icd.claim_id
            AND fc.procedure_id = icd.procedure_id
            AND fc.claim_procedure_id = icd.claim_procedure_id
    )
    AND icd.claim_procedure_id IS NULL
UNION ALL
SELECT 
    'All keys present but still missing (likely duplicates)' as issue_type,
    COUNT(*) as record_count
FROM int.int_claim_details icd
WHERE icd.claim_date >= '2023-01-01'
    AND NOT EXISTS (
        SELECT 1 
        FROM marts.fact_claim fc
        WHERE fc.claim_id = icd.claim_id
            AND fc.procedure_id = icd.procedure_id
            AND fc.claim_procedure_id = icd.claim_procedure_id
    )
    AND icd.claim_id IS NOT NULL
    AND icd.procedure_id IS NOT NULL
    AND icd.claim_procedure_id IS NOT NULL;

-- 1.2 Composite Key Uniqueness Validation
SELECT 
    claim_id,
    procedure_id,
    claim_procedure_id,
    COUNT(*) as duplicate_count
FROM marts.fact_claim
GROUP BY claim_id, procedure_id, claim_procedure_id
HAVING COUNT(*) > 1;

-- 1.3 Missing Key Records Validation
SELECT 
    cp."ClaimNum" as claim_id,
    cp."ProcNum" as procedure_id,
    cp."ClaimProcNum" as claim_procedure_id,
    c."DateService" as claim_date
FROM raw.claimproc cp
INNER JOIN raw.claim c ON cp."ClaimNum" = c."ClaimNum"
WHERE cp."ProcDate" >= '2023-01-01'
    AND NOT EXISTS (
        SELECT 1 
        FROM marts.fact_claim fc
        WHERE fc.claim_id = cp."ClaimNum"
            AND fc.procedure_id = cp."ProcNum"
            AND fc.claim_procedure_id = cp."ClaimProcNum"
    )
LIMIT 100;

-- ============================================================================
-- PHASE 2: DATA ACCURACY VALIDATION
-- ============================================================================

-- 2.1 Financial Amount Reconciliation
WITH source_amounts AS (
    SELECT 
        cp."ClaimNum" as claim_id,
        cp."ProcNum" as procedure_id,
        cp."ClaimProcNum" as claim_procedure_id,
        cp."FeeBilled" as source_billed_amount,
        COALESCE(cp."AllowedOverride", 0) as source_allowed_amount,
        COALESCE(cp."InsPayAmt", 0) as source_paid_amount,
        COALESCE(cp."WriteOff", 0) as source_write_off_amount,
        COALESCE(cp."CopayAmt", 0) as source_patient_responsibility
    FROM raw.claimproc cp
    WHERE cp."ProcDate" >= '2023-01-01'
),
fact_amounts AS (
    SELECT 
        claim_id,
        procedure_id,
        claim_procedure_id,
        billed_amount,
        allowed_amount,
        paid_amount,
        write_off_amount,
        patient_responsibility
    FROM marts.fact_claim
    WHERE claim_date >= '2023-01-01'
)
SELECT 
    s.claim_id,
    s.procedure_id,
    s.claim_procedure_id,
    s.source_billed_amount,
    f.billed_amount,
    ABS(s.source_billed_amount - f.billed_amount) as billed_diff,
    s.source_paid_amount,
    f.paid_amount,
    ABS(s.source_paid_amount - f.paid_amount) as paid_diff,
    CASE 
        WHEN ABS(s.source_billed_amount - f.billed_amount) > 0.01 THEN 'FAIL'
        WHEN ABS(s.source_paid_amount - f.paid_amount) > 0.01 THEN 'FAIL'
        ELSE 'PASS'
    END as validation_status
FROM source_amounts s
INNER JOIN fact_amounts f 
    ON s.claim_id = f.claim_id
    AND s.procedure_id = f.procedure_id
    AND s.claim_procedure_id = f.claim_procedure_id
WHERE ABS(s.source_billed_amount - f.billed_amount) > 0.01
    OR ABS(s.source_paid_amount - f.paid_amount) > 0.01
LIMIT 100;

-- 2.2 Claim Status Validation
WITH source_status AS (
    SELECT 
        cp."ClaimNum" as claim_id,
        cp."ProcNum" as procedure_id,
        cp."ClaimProcNum" as claim_procedure_id,
        c."ClaimStatus" as source_claim_status,
        c."ClaimType" as source_claim_type,
        cp."Status"::smallint as source_procedure_status
    FROM raw.claimproc cp
    INNER JOIN raw.claim c ON cp."ClaimNum" = c."ClaimNum"
    WHERE cp."ProcDate" >= '2023-01-01'
),
fact_status AS (
    SELECT 
        claim_id,
        procedure_id,
        claim_procedure_id,
        claim_status,
        claim_type,
        claim_procedure_status
    FROM marts.fact_claim
    WHERE claim_date >= '2023-01-01'
)
SELECT 
    s.claim_id,
    s.procedure_id,
    s.claim_procedure_id,
    s.source_claim_status,
    f.claim_status,
    s.source_claim_type,
    f.claim_type,
    s.source_procedure_status,
    f.claim_procedure_status,
    CASE 
        WHEN s.source_claim_status != f.claim_status THEN 'FAIL - Status mismatch'
        WHEN s.source_claim_type != f.claim_type THEN 'FAIL - Type mismatch'
        WHEN s.source_procedure_status != f.claim_procedure_status THEN 'FAIL - Procedure status mismatch'
        ELSE 'PASS'
    END as validation_status
FROM source_status s
INNER JOIN fact_status f 
    ON s.claim_id = f.claim_id
    AND s.procedure_id = f.procedure_id
    AND s.claim_procedure_id = f.claim_procedure_id
WHERE s.source_claim_status != f.claim_status
    OR s.source_claim_type != f.claim_type
    OR s.source_procedure_status != f.claim_procedure_status
LIMIT 100;

-- 2.3 Date Field Validation
-- Note: claim_id = 0 represents pre-authorization/draft claims which may have future dates
SELECT 
    claim_id,
    claim_date,
    check_date,
    snapshot_date,
    tracking_date,
    CASE 
        WHEN claim_date < '2020-01-01' THEN 'WARN - Very old claim date'
        WHEN claim_date > CURRENT_DATE + INTERVAL '1 day' AND claim_id = 0 THEN 'INFO - Future date for pre-auth/draft claim'
        WHEN claim_date > CURRENT_DATE + INTERVAL '1 day' AND claim_id != 0 THEN 'FAIL - Future claim date'
        WHEN check_date IS NOT NULL AND check_date < claim_date THEN 'WARN - Payment before claim'
        WHEN check_date IS NOT NULL AND check_date > CURRENT_DATE THEN 'FAIL - Future payment date'
        ELSE 'PASS'
    END as date_validation_status
FROM marts.fact_claim
WHERE claim_date < '2020-01-01'
    OR (claim_date > CURRENT_DATE + INTERVAL '1 day' AND claim_id != 0)  -- Only flag future dates for submitted claims
    OR (check_date IS NOT NULL AND check_date < claim_date)
    OR (check_date IS NOT NULL AND check_date > CURRENT_DATE)
LIMIT 100;

-- 2.3.1 Diagnostic: Investigate claim_id = 0 records
SELECT 
    'Total records with claim_id = 0' as metric,
    COUNT(*) as count
FROM marts.fact_claim
WHERE claim_id = 0
UNION ALL
SELECT 
    'Future-dated records with claim_id = 0' as metric,
    COUNT(*) as count
FROM marts.fact_claim
WHERE claim_id = 0
    AND claim_date > CURRENT_DATE + INTERVAL '1 day'
UNION ALL
SELECT 
    'Future-dated records with claim_id != 0' as metric,
    COUNT(*) as count
FROM marts.fact_claim
WHERE claim_id != 0
    AND claim_date > CURRENT_DATE + INTERVAL '1 day'
UNION ALL
SELECT 
    'Date range for claim_id = 0' as metric,
    COUNT(DISTINCT claim_date) as count
FROM marts.fact_claim
WHERE claim_id = 0;

-- ============================================================================
-- PHASE 3: BUSINESS LOGIC VALIDATION
-- ============================================================================

-- 3.1 Payment Status Category Validation
SELECT 
    claim_id,
    paid_amount,
    claim_status,
    payment_status_category,
    CASE 
        WHEN paid_amount > 0 AND payment_status_category != 'Paid' THEN 'FAIL'
        WHEN claim_status = 'Denied' AND payment_status_category != 'Denied' THEN 'FAIL'
        WHEN claim_status = 'Submitted' AND payment_status_category != 'Pending' THEN 'FAIL'
        WHEN claim_status = 'Rejected' AND payment_status_category != 'Rejected' THEN 'FAIL'
        WHEN paid_amount = 0 AND claim_status NOT IN ('Denied', 'Submitted', 'Rejected') 
            AND payment_status_category = 'Unknown' THEN 'PASS'
        ELSE 'PASS'
    END as validation_status
FROM marts.fact_claim
WHERE (paid_amount > 0 AND payment_status_category != 'Paid')
    OR (claim_status = 'Denied' AND payment_status_category != 'Denied')
    OR (claim_status = 'Submitted' AND payment_status_category != 'Pending')
    OR (claim_status = 'Rejected' AND payment_status_category != 'Rejected')
LIMIT 100;

-- 3.2 Payment Completion Status Validation
SELECT 
    claim_id,
    write_off_amount,
    patient_responsibility,
    paid_amount,
    billed_amount,
    payment_completion_status,
    CASE 
        WHEN write_off_amount > 0 AND payment_completion_status != 'Write-off' THEN 'FAIL'
        WHEN patient_responsibility > 0 AND write_off_amount = 0 
            AND payment_completion_status != 'Patient Balance' THEN 'FAIL'
        WHEN paid_amount = billed_amount AND write_off_amount = 0 
            AND patient_responsibility = 0 
            AND payment_completion_status != 'Fully Paid' THEN 'FAIL'
        WHEN paid_amount > 0 AND paid_amount < billed_amount 
            AND write_off_amount = 0 
            AND payment_completion_status != 'Partial Payment' THEN 'FAIL'
        ELSE 'PASS'
    END as validation_status
FROM marts.fact_claim
WHERE (write_off_amount > 0 AND payment_completion_status != 'Write-off')
    OR (patient_responsibility > 0 AND write_off_amount = 0 
        AND payment_completion_status != 'Patient Balance')
    OR (paid_amount = billed_amount AND write_off_amount = 0 
        AND patient_responsibility = 0 
        AND payment_completion_status != 'Fully Paid')
    OR (paid_amount > 0 AND paid_amount < billed_amount 
        AND write_off_amount = 0 
        AND payment_completion_status != 'Partial Payment')
LIMIT 100;

-- 3.3 Payment Days Calculation Validation
SELECT 
    claim_id,
    claim_date,
    check_date,
    payment_days_from_claim,
    (check_date - claim_date) as manual_calculation,
    ABS(payment_days_from_claim - (check_date - claim_date)) as difference,
    CASE 
        WHEN check_date IS NULL AND payment_days_from_claim IS NULL THEN 'PASS'
        WHEN check_date IS NOT NULL AND payment_days_from_claim IS NULL THEN 'FAIL'
        WHEN ABS(payment_days_from_claim - (check_date - claim_date)) > 0 THEN 'FAIL'
        ELSE 'PASS'
    END as validation_status
FROM marts.fact_claim
WHERE check_date IS NOT NULL
    AND ABS(payment_days_from_claim - (check_date - claim_date)) > 0
LIMIT 100;

-- ============================================================================
-- PHASE 4: FINANCIAL RECONCILIATION
-- ============================================================================

-- 4.1 Financial Balance Validation
-- Note: Excluding:
--   - claim_id = 0 (pre-authorization/draft claims) as they may have payments but no billed amount yet
--   - patient_responsibility = -1.0 (placeholder for undetermined patient responsibility)
-- The balance equation doesn't apply to these records
SELECT 
    claim_id,
    billed_amount,
    paid_amount,
    write_off_amount,
    patient_responsibility,
    (paid_amount + write_off_amount + patient_responsibility) as calculated_total,
    (billed_amount - (paid_amount + write_off_amount + patient_responsibility)) as balance_difference,
    CASE 
        WHEN ABS(billed_amount - (paid_amount + write_off_amount + patient_responsibility)) > 0.01 
            THEN 'FAIL - Financial amounts do not balance'
        ELSE 'PASS'
    END as validation_status
FROM marts.fact_claim
WHERE claim_id != 0  -- Exclude pre-authorization/draft claims
    AND patient_responsibility != -1.0  -- Exclude placeholder values (undetermined patient responsibility)
    AND ABS(billed_amount - (paid_amount + write_off_amount + patient_responsibility)) > 0.01
LIMIT 100;

-- 4.2 Payment Amount Validation
-- Note: Excluding:
--   - claim_id = 0 (pre-authorization/draft claims) as they may have payments but no billed/allowed amounts yet
--   - allowed_amount = -1.0 (placeholder for undetermined allowed amount)
-- The validation rules don't apply to these records
SELECT 
    claim_id,
    billed_amount,
    allowed_amount,
    paid_amount,
    CASE 
        WHEN paid_amount > allowed_amount THEN 'FAIL - Paid exceeds allowed'
        WHEN allowed_amount > billed_amount THEN 'WARN - Allowed exceeds billed'
        WHEN paid_amount < 0 THEN 'FAIL - Negative payment'
        WHEN allowed_amount < 0 THEN 'FAIL - Negative allowed'
        WHEN billed_amount < 0 THEN 'FAIL - Negative billed'
        ELSE 'PASS'
    END as validation_status
FROM marts.fact_claim
WHERE claim_id != 0  -- Exclude pre-authorization/draft claims
    AND allowed_amount != -1.0  -- Exclude placeholder values (undetermined allowed amount)
    AND (paid_amount > allowed_amount
        OR allowed_amount > billed_amount
        OR paid_amount < 0
        OR allowed_amount < 0  -- Negative allowed amounts (excluding -1.0 placeholder already filtered)
        OR billed_amount < 0)
LIMIT 100;

-- 4.2.1 Diagnostic: Investigate "Paid exceeds allowed" failures
-- Analyze the pattern of cases where paid_amount > allowed_amount
SELECT 
    claim_id,
    procedure_id,
    claim_procedure_id,
    billed_amount,
    allowed_amount,
    paid_amount,
    (paid_amount - allowed_amount) as overpayment_amount,
    claim_status,
    claim_date,
    check_date,
    CASE 
        WHEN billed_amount = 0.0 THEN 'No billed amount'
        WHEN allowed_amount < billed_amount THEN 'Allowed < Billed'
        WHEN allowed_amount = billed_amount THEN 'Allowed = Billed'
        ELSE 'Other'
    END as billing_pattern
FROM marts.fact_claim
WHERE claim_id != 0
    AND allowed_amount != -1.0
    AND paid_amount > allowed_amount
ORDER BY (paid_amount - allowed_amount) DESC
LIMIT 20;

-- 4.2.2 Investigation Queries: Business Rule Clarification for paid_amount > allowed_amount
-- These queries help determine if overpayments are legitimate or data quality issues

-- a. Source System Review
-- Query raw schema tables to verify payment amounts from source OpenDental data
-- Raw schema contains OpenDental data with only basic MySQL to PostgreSQL transformations
SELECT 
    cp."ClaimNum" as claim_id,
    cp."ProcNum" as procedure_id,
    cp."ClaimProcNum" as claim_procedure_id,
    cp."FeeBilled" as billed_amount,
    cp."InsPayEst" as estimated_allowed,
    cp."InsPayAmt" as paid_amount,
    c."ClaimStatus" as claim_status,
    c."DateService" as claim_date
FROM raw.claimproc cp
LEFT JOIN raw.claim c ON cp."ClaimNum" = c."ClaimNum"
WHERE cp."ClaimNum" IN (19550, 18070, 22564, 29429, 18774, 24119, 16330, 21977, 25494, 18175)
ORDER BY cp."ClaimNum", cp."ProcNum";

-- d. Historical Pattern Analysis
-- Analyze patterns in overpayment cases
SELECT 
    claim_status,
    COUNT(*) as overpayment_count,
    AVG(paid_amount - allowed_amount) as avg_overpayment,
    MAX(paid_amount - allowed_amount) as max_overpayment,
    COUNT(DISTINCT insurance_plan_id) as affected_plans,
    COUNT(DISTINCT procedure_id) as affected_procedures
FROM marts.fact_claim
WHERE claim_id != 0
    AND allowed_amount != -1.0
    AND paid_amount > allowed_amount
GROUP BY claim_status
ORDER BY overpayment_count DESC;

-- e. Payment Allocation Review
-- Check if payments are being allocated incorrectly across procedures
SELECT 
    claim_id,
    COUNT(*) as procedure_count,
    SUM(billed_amount) as total_billed,
    SUM(allowed_amount) as total_allowed,
    SUM(paid_amount) as total_paid,
    SUM(paid_amount) - SUM(allowed_amount) as total_overpayment
FROM marts.fact_claim
WHERE claim_id IN (19550, 18070, 22564, 29429, 18774, 24119, 16330, 21977, 25494, 18175)
GROUP BY claim_id
HAVING SUM(paid_amount) > SUM(allowed_amount)
ORDER BY total_overpayment DESC;

-- f. Insurance Company Analysis
-- Check if specific insurance companies have higher overpayment rates
SELECT 
    di.carrier_name,
    COUNT(*) as overpayment_count,
    AVG(fc.paid_amount - fc.allowed_amount) as avg_overpayment,
    SUM(fc.paid_amount - fc.allowed_amount) as total_overpayment
FROM marts.fact_claim fc
LEFT JOIN marts.dim_insurance di ON fc.insurance_plan_id = di.insurance_plan_id
WHERE fc.claim_id != 0
    AND fc.allowed_amount != -1.0
    AND fc.paid_amount > fc.allowed_amount
GROUP BY di.carrier_name
ORDER BY overpayment_count DESC;

-- g. Procedure Code Analysis
-- Check if specific procedure codes have higher overpayment rates
-- Note: fact_claim already contains procedure_code and procedure_description, no join needed
SELECT 
    fc.procedure_code,
    fc.procedure_description,
    COUNT(*) as overpayment_count,
    AVG(fc.paid_amount - fc.allowed_amount) as avg_overpayment
FROM marts.fact_claim fc
WHERE fc.claim_id != 0
    AND fc.allowed_amount != -1.0
    AND fc.paid_amount > fc.allowed_amount
GROUP BY fc.procedure_code, fc.procedure_description
ORDER BY overpayment_count DESC
LIMIT 20;

-- h. Timing Analysis
-- Check if overpayments are related to timing of payment vs claim submission
SELECT 
    DATE_TRUNC('month', claim_date) as claim_month,
    DATE_TRUNC('month', check_date) as payment_month,
    (check_date - claim_date) as days_between_claim_and_payment,
    COUNT(*) as overpayment_count,
    AVG(paid_amount - allowed_amount) as avg_overpayment
FROM marts.fact_claim
WHERE claim_id != 0
    AND allowed_amount != -1.0
    AND paid_amount > allowed_amount
    AND check_date IS NOT NULL
GROUP BY DATE_TRUNC('month', claim_date), DATE_TRUNC('month', check_date), (check_date - claim_date)
ORDER BY overpayment_count DESC;

-- 4.3 Collection Rate Validation
SELECT 
    claim_id,
    billed_amount,
    paid_amount,
    CASE 
        WHEN billed_amount > 0 THEN (paid_amount / billed_amount * 100) 
        ELSE NULL 
    END as collection_rate,
    CASE 
        WHEN billed_amount > 0 AND (paid_amount / billed_amount * 100) > 100 THEN 'WARN - Collection rate > 100%'
        WHEN billed_amount > 0 AND (paid_amount / billed_amount * 100) < 0 THEN 'FAIL - Negative collection rate'
        ELSE 'PASS'
    END as validation_status
FROM marts.fact_claim
WHERE billed_amount > 0
    AND ((paid_amount / billed_amount * 100) > 100 
        OR (paid_amount / billed_amount * 100) < 0)
LIMIT 100;

-- ============================================================================
-- PHASE 5: REFERENTIAL INTEGRITY VALIDATION
-- ============================================================================

-- 5.1 Foreign Key Validation
WITH invalid_patients AS (
    SELECT DISTINCT patient_id
    FROM marts.fact_claim fc
    WHERE NOT EXISTS (
        SELECT 1 FROM marts.dim_patient dp 
        WHERE dp.patient_id = fc.patient_id
    )
),
invalid_providers AS (
    SELECT DISTINCT provider_id
    FROM marts.fact_claim fc
    WHERE provider_id IS NOT NULL
        AND NOT EXISTS (
            SELECT 1 FROM marts.dim_provider dp 
            WHERE dp.provider_id = fc.provider_id
        )
),
invalid_insurance AS (
    SELECT DISTINCT insurance_plan_id
    FROM marts.fact_claim fc
    WHERE insurance_plan_id IS NOT NULL
        AND NOT EXISTS (
            SELECT 1 FROM marts.dim_insurance di 
            WHERE di.insurance_plan_id = fc.insurance_plan_id
        )
)
SELECT 
    'Invalid Patients' as validation_type,
    COUNT(*) as invalid_count
FROM invalid_patients
UNION ALL
SELECT 
    'Invalid Providers' as validation_type,
    COUNT(*) as invalid_count
FROM invalid_providers
UNION ALL
SELECT 
    'Invalid Insurance Plans' as validation_type,
    COUNT(*) as invalid_count
FROM invalid_insurance;

-- 5.2 Source Data Consistency
SELECT 
    fc.claim_id,
    fc.procedure_id,
    fc.claim_procedure_id,
    CASE 
        WHEN NOT EXISTS (
            SELECT 1 FROM raw.claimproc cp 
            WHERE cp."ClaimNum" = fc.claim_id
                AND cp."ProcNum" = fc.procedure_id
                AND cp."ClaimProcNum" = fc.claim_procedure_id
        ) THEN 'FAIL - Cannot trace to source'
        ELSE 'PASS'
    END as validation_status
FROM marts.fact_claim fc
WHERE NOT EXISTS (
    SELECT 1 FROM raw.claimproc cp 
    WHERE cp."ClaimNum" = fc.claim_id
        AND cp."ProcNum" = fc.procedure_id
        AND cp."ClaimProcNum" = fc.claim_procedure_id
)
LIMIT 100;

-- ============================================================================
-- PHASE 6: DATA QUALITY VALIDATION
-- ============================================================================

-- 6.1 Null Value Validation
SELECT 
    'claim_id' as field_name,
    COUNT(*) as null_count
FROM marts.fact_claim
WHERE claim_id IS NULL
UNION ALL
SELECT 
    'procedure_id' as field_name,
    COUNT(*) as null_count
FROM marts.fact_claim
WHERE procedure_id IS NULL
UNION ALL
SELECT 
    'claim_procedure_id' as field_name,
    COUNT(*) as null_count
FROM marts.fact_claim
WHERE claim_procedure_id IS NULL
UNION ALL
SELECT 
    'patient_id' as field_name,
    COUNT(*) as null_count
FROM marts.fact_claim
WHERE patient_id IS NULL
UNION ALL
SELECT 
    'claim_date' as field_name,
    COUNT(*) as null_count
FROM marts.fact_claim
WHERE claim_date IS NULL
UNION ALL
SELECT 
    'billed_amount' as field_name,
    COUNT(*) as null_count
FROM marts.fact_claim
WHERE billed_amount IS NULL;

-- 6.2 Data Range Validation
SELECT 
    'Billed Amount Range' as validation_type,
    MIN(billed_amount) as min_value,
    MAX(billed_amount) as max_value,
    AVG(billed_amount) as avg_value,
    CASE 
        WHEN MIN(billed_amount) < 0 THEN 'FAIL - Negative billed amounts'
        WHEN MAX(billed_amount) > 50000 THEN 'WARN - Very high billed amounts'
        ELSE 'PASS'
    END as validation_status
FROM marts.fact_claim
UNION ALL
SELECT 
    'Payment Days Range' as validation_type,
    MIN(payment_days_from_claim) as min_value,
    MAX(payment_days_from_claim) as max_value,
    AVG(payment_days_from_claim) as avg_value,
    CASE 
        WHEN MIN(payment_days_from_claim) < 0 THEN 'WARN - Negative payment days'
        WHEN MAX(payment_days_from_claim) > 365 THEN 'WARN - Very long payment cycles'
        ELSE 'PASS'
    END as validation_status
FROM marts.fact_claim
WHERE payment_days_from_claim IS NOT NULL;

-- 6.3 EOB Documentation Validation
SELECT 
    eob_documentation_status,
    COUNT(*) as claim_count,
    SUM(CASE WHEN eob_attachment_count > 0 THEN 1 ELSE 0 END) as has_attachments,
    SUM(CASE WHEN check_amount > 0 THEN 1 ELSE 0 END) as has_payments
FROM marts.fact_claim
GROUP BY eob_documentation_status;

-- ============================================================================
-- PHASE 7: INTEGRATION VALIDATION
-- ============================================================================

-- 7.1 Intermediate Model Validation
WITH int_claim_details_count AS (
    SELECT COUNT(*) as int_count
    FROM int.int_claim_details
    WHERE claim_date >= '2023-01-01'
),
fact_claim_count AS (
    SELECT COUNT(*) as fact_count
    FROM marts.fact_claim
    WHERE claim_date >= '2023-01-01'
)
SELECT 
    i.int_count as intermediate_count,
    f.fact_count as fact_count,
    (i.int_count - f.fact_count) as difference,
    CASE 
        WHEN i.int_count = f.fact_count THEN 'PASS'
        WHEN ABS(i.int_count - f.fact_count) / NULLIF(i.int_count, 0) < 0.01 THEN 'WARN - <1% variance'
        ELSE 'FAIL - Significant variance'
    END as validation_status
FROM int_claim_details_count i
CROSS JOIN fact_claim_count f;

-- 7.2 Join Completeness Validation
SELECT 
    'Claims with Payments' as join_type,
    COUNT(*) as fact_count,
    COUNT(CASE WHEN check_date IS NOT NULL THEN 1 END) as has_payment_data,
    ROUND(COUNT(CASE WHEN check_date IS NOT NULL THEN 1 END)::numeric / COUNT(*) * 100, 2) as payment_coverage_pct
FROM marts.fact_claim
WHERE claim_date >= '2023-01-01'
UNION ALL
SELECT 
    'Claims with Snapshots' as join_type,
    COUNT(*) as fact_count,
    COUNT(CASE WHEN snapshot_date IS NOT NULL THEN 1 END) as has_snapshot_data,
    ROUND(COUNT(CASE WHEN snapshot_date IS NOT NULL THEN 1 END)::numeric / COUNT(*) * 100, 2) as snapshot_coverage_pct
FROM marts.fact_claim
WHERE claim_date >= '2023-01-01'
UNION ALL
SELECT 
    'Claims with Tracking' as join_type,
    COUNT(*) as fact_count,
    COUNT(CASE WHEN tracking_date IS NOT NULL THEN 1 END) as has_tracking_data,
    ROUND(COUNT(CASE WHEN tracking_date IS NOT NULL THEN 1 END)::numeric / COUNT(*) * 100, 2) as tracking_coverage_pct
FROM marts.fact_claim
WHERE claim_date >= '2023-01-01';

-- ============================================================================
-- PHASE 10: RECONCILIATION SUMMARY REPORT
-- ============================================================================

-- Comprehensive Validation Summary
-- Note: Using separate queries to avoid UNION ALL type coercion issues
-- Each query returns its own result set for clarity

-- 10.1 Row Count Summary
SELECT 
    'Row Count' as validation_category,
    COUNT(*) as total_records,
    COUNT(DISTINCT claim_id) as unique_claims,
    COUNT(DISTINCT procedure_id) as unique_procedures,
    COUNT(DISTINCT patient_id) as unique_patients,
    COUNT(DISTINCT insurance_plan_id) as unique_insurance_plans
FROM marts.fact_claim
WHERE claim_date >= '2023-01-01';

-- 10.2 Financial Summary
SELECT 
    'Financial Summary' as validation_category,
    COUNT(*) as total_records,
    SUM(billed_amount) as total_billed,
    SUM(paid_amount) as total_paid,
    SUM(write_off_amount) as total_write_off,
    SUM(patient_responsibility) as total_patient_responsibility,
    COUNT(CASE WHEN write_off_amount > 0 THEN 1 END) as records_with_write_off
FROM marts.fact_claim
WHERE claim_date >= '2023-01-01';

-- 10.3 Status Distribution
SELECT 
    'Status Distribution' as validation_category,
    COUNT(*) as total_records,
    SUM(CASE WHEN payment_status_category = 'Paid' THEN 1 ELSE 0 END) as paid_count,
    SUM(CASE WHEN payment_status_category = 'Pending' THEN 1 ELSE 0 END) as pending_count,
    SUM(CASE WHEN payment_status_category = 'Denied' THEN 1 ELSE 0 END) as denied_count,
    SUM(CASE WHEN payment_status_category = 'Rejected' THEN 1 ELSE 0 END) as rejected_count,
    SUM(CASE WHEN payment_status_category = 'Unknown' THEN 1 ELSE 0 END) as unknown_count
FROM marts.fact_claim
WHERE claim_date >= '2023-01-01';

-- 10.4 Data Quality Summary
SELECT 
    'Data Quality' as validation_category,
    COUNT(*) as total_records,
    SUM(CASE WHEN claim_id IS NULL THEN 1 ELSE 0 END) as null_claim_ids,
    SUM(CASE WHEN patient_id IS NULL THEN 1 ELSE 0 END) as null_patient_ids,
    SUM(CASE WHEN billed_amount IS NULL THEN 1 ELSE 0 END) as null_billed_amounts,
    SUM(CASE WHEN claim_date IS NULL THEN 1 ELSE 0 END) as null_claim_dates
FROM marts.fact_claim
WHERE claim_date >= '2023-01-01';

-- 10.2.1 Diagnostic: Investigate Negative Patient Responsibility
-- Analyze why total patient_responsibility is negative (-$73,817)
SELECT 
    COUNT(*) as total_records,
    COUNT(CASE WHEN patient_responsibility < 0 THEN 1 END) as negative_count,
    COUNT(CASE WHEN patient_responsibility = -1.0 THEN 1 END) as placeholder_count,
    COUNT(CASE WHEN patient_responsibility > 0 THEN 1 END) as positive_count,
    SUM(CASE WHEN patient_responsibility < 0 THEN patient_responsibility ELSE 0 END) as total_negative,
    SUM(CASE WHEN patient_responsibility > 0 THEN patient_responsibility ELSE 0 END) as total_positive,
    AVG(CASE WHEN patient_responsibility < 0 THEN patient_responsibility END) as avg_negative,
    MIN(patient_responsibility) as min_value,
    MAX(patient_responsibility) as max_value
FROM marts.fact_claim
WHERE claim_date >= '2023-01-01';

-- 10.2.2 Diagnostic: Sample Records with Negative Patient Responsibility
-- Sample records to understand the pattern
SELECT 
    claim_id,
    procedure_id,
    claim_procedure_id,
    billed_amount,
    paid_amount,
    write_off_amount,
    patient_responsibility,
    (billed_amount - paid_amount - write_off_amount) as calculated_patient_responsibility,
    (paid_amount - billed_amount) as overpayment_amount
FROM marts.fact_claim
WHERE claim_date >= '2023-01-01'
    AND patient_responsibility < 0
    AND patient_responsibility != -1.0  -- Exclude placeholder
ORDER BY patient_responsibility ASC
LIMIT 20;

-- 10.3.1 Diagnostic: Analyze claim_status distribution for Unknown payment_status_category
-- Identify which claim_status values are causing Unknown payment status
SELECT 
    claim_status,
    COUNT(*) as record_count,
    COUNT(CASE WHEN paid_amount > 0 THEN 1 END) as records_with_payment,
    COUNT(CASE WHEN paid_amount = 0 THEN 1 END) as records_without_payment,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as pct_of_unknown
FROM marts.fact_claim
WHERE payment_status_category = 'Unknown'
GROUP BY claim_status
ORDER BY record_count DESC;

-- 10.3.2 Diagnostic: Check if claim_id = 0 records contribute to Unknown status
SELECT 
    CASE WHEN claim_id = 0 THEN 'Pre-auth/Draft (claim_id=0)' ELSE 'Regular Claims' END as claim_type,
    payment_status_category,
    COUNT(*) as record_count,
    COUNT(DISTINCT claim_id) as unique_claims
FROM marts.fact_claim
WHERE payment_status_category = 'Unknown'
GROUP BY 
    CASE WHEN claim_id = 0 THEN 'Pre-auth/Draft (claim_id=0)' ELSE 'Regular Claims' END,
    payment_status_category
ORDER BY record_count DESC;

-- 10.3.3 Diagnostic: Review claim_status values in source data
-- Check what claim_status values exist that might not be mapped
SELECT 
    claim_status,
    COUNT(*) as record_count,
    COUNT(CASE WHEN paid_amount > 0 THEN 1 END) as records_with_payment,
    COUNT(CASE WHEN paid_amount = 0 THEN 1 END) as records_without_payment,
    MIN(claim_date) as earliest_claim_date,
    MAX(claim_date) as latest_claim_date
FROM marts.fact_claim
WHERE payment_status_category = 'Unknown'
GROUP BY claim_status
ORDER BY record_count DESC;

-- 10.3.4 Diagnostic: Analyze relationship between paid_amount and unknown status
-- Check if unpaid claims are the primary driver of Unknown status
SELECT 
    CASE 
        WHEN paid_amount > 0 THEN 'Has Payment'
        WHEN paid_amount = 0 AND claim_status IS NULL THEN 'No Payment, NULL Status'
        WHEN paid_amount = 0 AND claim_status IS NOT NULL THEN 'No Payment, Has Status'
        ELSE 'Other'
    END as payment_status_group,
    COUNT(*) as record_count,
    COUNT(DISTINCT claim_status) as distinct_claim_statuses,
    STRING_AGG(DISTINCT claim_status, ', ' ORDER BY claim_status) as claim_status_values
FROM marts.fact_claim
WHERE payment_status_category = 'Unknown'
GROUP BY 
    CASE 
        WHEN paid_amount > 0 THEN 'Has Payment'
        WHEN paid_amount = 0 AND claim_status IS NULL THEN 'No Payment, NULL Status'
        WHEN paid_amount = 0 AND claim_status IS NOT NULL THEN 'No Payment, Has Status'
        ELSE 'Other'
    END
ORDER BY record_count DESC;
