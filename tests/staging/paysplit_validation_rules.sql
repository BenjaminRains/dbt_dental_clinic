{{ config(severity = 'warn') }}

{% if execute and model.name != 'stg_opendental__paysplit' %}
    -- Skip this test for any model that isn't stg_opendental__paysplit
    select 1 limit 0
{% else %}
/*
  PAYSPLIT VALIDATION RULES
  ========================
  
  Purpose:
  Validates payment splits (allocations) based on PostgreSQL schema and business logic.
  
  Key Fields (from DDL):
  - SplitNum: Primary key
  - PayNum: Payment reference (indexed)
  - SplitAmt: Amount of split (default 0)
  - DatePay: Payment date (BRIN indexed)
  - ProcNum: Procedure reference (indexed)
  - AdjNum: Adjustment reference (indexed)
  - IsDiscount: Flag for discounts (smallint)
  - PatNum: Patient reference (indexed)
  - UnearnedType: Income recognition type (288 and 439 for unearned)

  Key Validation Rules:
  1. Required Fields:
     - SplitNum must be unique and not null
     - PayNum must exist (links to payment)
     - SplitAmt must not be null
     - DatePay should match payment date
  2. Reference Rules:
     - Must have either ProcNum, AdjNum, or PayPlanChargeNum
     - PatNum should match payment's patient
  3. Amount Rules:
     - SplitAmt should not be 0 unless IsDiscount=1
     - Sum of splits should match payment amount
  4. Split Count Rules:
     - Flag high split counts (>10, >100, >1000)
  5. Unearned Income Rules:
     - Monitor Type 288 and 439 splits
     - Flag large unearned amounts
*/

WITH PaymentData AS (
    SELECT 
        payment_id,
        patient_id,
        payment_amount,
        payment_date
    FROM {{ ref('stg_opendental__payment') }}
    WHERE payment_date >= '2023-01-01'
),

PaysplitTotals AS (
    SELECT 
        payment_id,
        SUM(split_amount) as total_splits,
        COUNT(*) as split_count,
        SUM(CASE WHEN unearned_type = 288 THEN split_amount ELSE 0 END) as unearned_288_amount,
        SUM(CASE WHEN unearned_type = 439 THEN split_amount ELSE 0 END) as unearned_439_amount
    FROM {{ ref('stg_opendental__paysplit') }}
    GROUP BY payment_id
),

ValidationFailures AS (
    -- 1. Required Fields Validation
    SELECT 
        ps.paysplit_id,
        ps.payment_id,
        ps.split_amount,
        ps.payment_date,
        'Missing required field' AS failure_type,
        CASE 
            WHEN ps.payment_id IS NULL THEN 'payment_id'
            WHEN ps.split_amount IS NULL THEN 'split_amount'
            WHEN ps.payment_date IS NULL THEN 'payment_date'
        END AS failed_field
    FROM {{ ref('stg_opendental__paysplit') }} ps
    WHERE ps.payment_id IS NULL 
       OR ps.split_amount IS NULL
       OR ps.payment_date IS NULL

    UNION ALL

    -- 2. Reference Validation
    SELECT 
        ps.paysplit_id,
        ps.payment_id,
        ps.split_amount,
        ps.payment_date,
        'Missing allocation reference' AS failure_type,
        'reference' AS failed_field
    FROM {{ ref('stg_opendental__paysplit') }} ps
    WHERE ps.procedure_id IS NULL 
      AND ps.adjustment_id IS NULL 
      AND ps.payplan_charge_id IS NULL

    UNION ALL

    -- 3. Patient Match Validation (Updated for Guarantor relationship)
    SELECT 
        ps.paysplit_id,
        ps.payment_id,
        ps.split_amount,
        ps.payment_date,
        'Patient mismatch without guarantor relationship' AS failure_type,
        'patient_id' AS failed_field
    FROM {{ ref('stg_opendental__paysplit') }} ps
    JOIN PaymentData p ON ps.payment_id = p.payment_id
    LEFT JOIN {{ ref('stg_opendental__patient') }} pat 
        ON ps.patient_id = pat.patient_id
    WHERE ps.patient_id != p.patient_id
        AND p.patient_id != pat.guarantor_id  -- Only flag if payer is not the guarantor

    UNION ALL

    -- 4. Payment Amount Matching
    SELECT 
        NULL as paysplit_id,
        pt.payment_id,
        pt.total_splits as split_amount,
        p.payment_date,
        CASE 
            WHEN ABS(p.payment_amount - pt.total_splits) > 100 
                THEN 'CRITICAL: Split total differs by >$100'
            WHEN ABS(p.payment_amount - pt.total_splits) > 10 
                THEN 'HIGH: Split total differs by >$10'
            WHEN ABS(p.payment_amount - pt.total_splits) > 1 
                THEN 'MEDIUM: Split total differs by >$1'
            ELSE 'LOW: Split total has minor difference'
        END AS failure_type,
        'split_total' AS failed_field
    FROM PaymentData p
    JOIN PaysplitTotals pt ON p.payment_id = pt.payment_id
    WHERE ABS(p.payment_amount - pt.total_splits) > 0.01  -- Allow for penny rounding

    UNION ALL

    -- 5. Zero Amount Validation
    SELECT 
        ps.paysplit_id,
        ps.payment_id,
        ps.split_amount,
        ps.payment_date,
        'Zero amount split' AS failure_type,
        'split_amount' AS failed_field
    FROM {{ ref('stg_opendental__paysplit') }} ps
    WHERE ps.split_amount = 0 
      AND ps.is_discount_flag = FALSE

    UNION ALL

    -- 6. High Split Count Validation
    SELECT 
        NULL as paysplit_id,
        payment_id,
        split_count as split_amount,
        NULL as payment_date,
        CASE 
            WHEN split_count > 1000 THEN 'CRITICAL: Very high split count (>1000)'
            WHEN split_count > 100 THEN 'HIGH: High split count (>100)'
            WHEN split_count > 10 THEN 'MEDIUM: Above average splits (>10)'
        END AS failure_type,
        'split_count' AS failed_field
    FROM PaysplitTotals
    WHERE split_count > 3

    UNION ALL

    -- 7. Unearned Income Type Validation
    SELECT 
        ps.paysplit_id,
        ps.payment_id,
        ps.split_amount,
        ps.payment_date,
        CASE 
            WHEN ps.unearned_type = 288 THEN 'Type 288 Unearned Income'
            WHEN ps.unearned_type = 439 THEN 'Type 439 Unearned Income'
        END AS failure_type,
        'unearned_type' AS failed_field
    FROM {{ ref('stg_opendental__paysplit') }} ps
    WHERE ps.unearned_type IN (288, 439)

    UNION ALL

    -- 8. Large Unearned Amount Validation
    SELECT 
        NULL as paysplit_id,
        payment_id,
        CASE 
            WHEN unearned_288_amount > 0 THEN unearned_288_amount
            ELSE unearned_439_amount
        END as split_amount,
        NULL as payment_date,
        CASE 
            WHEN unearned_288_amount > 1000 THEN 'CRITICAL: Large Type 288 unearned amount'
            WHEN unearned_439_amount > 1000 THEN 'CRITICAL: Large Type 439 unearned amount'
            WHEN (unearned_288_amount + unearned_439_amount) > 500 THEN 'HIGH: Significant unearned amount'
        END AS failure_type,
        'unearned_amount' AS failed_field
    FROM PaysplitTotals
    WHERE (unearned_288_amount > 500 OR unearned_439_amount > 500)

    UNION ALL

    -- 9. Provider Assignment Validation for Unearned Types
    SELECT 
        ps.paysplit_id,
        ps.payment_id,
        ps.split_amount,
        ps.payment_date,
        CASE 
            WHEN ps.unearned_type IN (288, 439) THEN 'CRITICAL: Unearned split missing provider'
            ELSE 'MEDIUM: Split missing provider'
        END AS failure_type,
        'provider_assignment' AS failed_field
    FROM {{ ref('stg_opendental__paysplit') }} ps
    WHERE ps.provider_id IS NULL

    UNION ALL

    -- 10. Negative Amount Pattern Validation
    SELECT 
        ps.paysplit_id,
        ps.payment_id,
        ps.split_amount,
        ps.payment_date,
        CASE
            WHEN ps.unearned_type = 288 AND ps.split_amount > 0 AND ABS(ps.split_amount) > 1000 
                THEN 'HIGH: Unusual positive amount for Type 288'
            WHEN ps.unearned_type = 439 AND ps.split_amount < 0 AND ABS(ps.split_amount) > 1000
                THEN 'HIGH: Unusual negative amount for Type 439'
        END AS failure_type,
        'amount_pattern' AS failed_field
    FROM {{ ref('stg_opendental__paysplit') }} ps
    WHERE (ps.unearned_type = 288 AND ps.split_amount > 0 AND ABS(ps.split_amount) > 1000)
       OR (ps.unearned_type = 439 AND ps.split_amount < 0 AND ABS(ps.split_amount) > 1000)

    UNION ALL

    -- 11. Balance Impact Validation
    SELECT 
        ps.paysplit_id,
        ps.payment_id,
        ps.split_amount,
        ps.payment_date,
        CASE
            WHEN ps.split_amount > 1000 AND ps.provider_id IS NULL 
                THEN 'CRITICAL: High split amount missing provider'
            WHEN ps.split_amount > 1000 AND ps.unearned_type IN (288, 439)
                THEN 'HIGH: High amount unearned split'
        END AS failure_type,
        'balance_risk' AS failed_field
    FROM {{ ref('stg_opendental__paysplit') }} ps
    WHERE ps.split_amount > 1000 
      AND (ps.provider_id IS NULL OR ps.unearned_type IN (288, 439))

    UNION ALL

    -- 12. Statistical Outlier Validation
    SELECT 
        ps.paysplit_id,
        ps.payment_id,
        ps.split_amount,
        ps.payment_date,
        CASE
            WHEN ps.unearned_type = 288 AND ABS(ps.split_amount) > 25000 
                THEN 'CRITICAL: Type 288 amount exceeds normal range'
            WHEN ps.unearned_type = 439 AND ABS(ps.split_amount) > 16000
                THEN 'CRITICAL: Type 439 amount exceeds normal range'
        END AS failure_type,
        'statistical_outlier' AS failed_field
    FROM {{ ref('stg_opendental__paysplit') }} ps
    WHERE (ps.unearned_type = 288 AND ABS(ps.split_amount) > 25000)
       OR (ps.unearned_type = 439 AND ABS(ps.split_amount) > 16000)
)

-- Add this new summary CTE
FailureSummary AS (
    SELECT 
        failure_type,
        COUNT(*) as count
    FROM ValidationFailures
    GROUP BY failure_type
    ORDER BY count DESC
)

-- Output includes a message showing the breakdown
SELECT 
    CASE WHEN row_number() OVER (ORDER BY count DESC) = 1 THEN
        'Summary of paysplit validation issues: ' || 
        string_agg(failure_type || ' (' || count || ')', ', ' ORDER BY count DESC)
        OVER () 
    ELSE NULL END as failure_message,
    paysplit_id,
    payment_id,
    split_amount,
    payment_date,
    failure_type,
    failed_field
FROM ValidationFailures
CROSS JOIN FailureSummary
ORDER BY
    CASE 
        WHEN failure_type LIKE 'CRITICAL%' THEN 1
        WHEN failure_type LIKE 'HIGH%' THEN 2
        WHEN failure_type LIKE 'MEDIUM%' THEN 3
        WHEN failure_type LIKE 'LOW%' THEN 4
        ELSE 5
    END,
    payment_id,
    paysplit_id
{% endif %}
