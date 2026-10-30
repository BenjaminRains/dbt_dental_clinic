-- Comprehensive paysplit analysis for pattern discovery
-- Export results to CSV for notebook analysis

-- 1. Overall Statistics
WITH OverallStats AS (
    SELECT 
        COUNT(*) AS total_splits,
        COUNT(DISTINCT "PayNum") AS unique_payments,
        COUNT(DISTINCT "PatNum") AS unique_patients,
        MIN("DatePay") AS earliest_date,
        MAX("DatePay") AS latest_date,
        ROUND(AVG("SplitAmt")::numeric, 2) AS avg_split_amount,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY "SplitAmt") AS median_split_amount
    FROM public.paysplit
    WHERE "DatePay" >= '2023-01-01'
),

-- 2. Split Type Distribution
SplitTypes AS (
    SELECT 
        CASE
            WHEN "ProcNum" IS NOT NULL THEN 'Procedure'
            WHEN "AdjNum" IS NOT NULL THEN 'Adjustment'
            WHEN "PayPlanChargeNum" IS NOT NULL THEN 'PayPlan'
            ELSE 'Unallocated'
        END AS split_type,
        COUNT(*) AS split_count,
        ROUND(AVG("SplitAmt")::numeric, 2) AS avg_amount,
        MIN("SplitAmt") AS min_amount,
        MAX("SplitAmt") AS max_amount,
        COUNT(CASE WHEN "IsDiscount" = 1 THEN 1 END) AS discount_count,
        COUNT(CASE WHEN "UnearnedType" IN (288, 439) THEN 1 END) AS unearned_count
    FROM public.paysplit
    WHERE "DatePay" >= '2023-01-01'
    GROUP BY split_type
),

-- 3. Split Count Distribution
SplitCounts AS (
    SELECT 
        "PayNum",
        COUNT(*) AS splits_per_payment,
        SUM("SplitAmt") AS total_amount,
        STRING_AGG(
            CASE
                WHEN "ProcNum" IS NOT NULL THEN 'P'
                WHEN "AdjNum" IS NOT NULL THEN 'A'
                WHEN "PayPlanChargeNum" IS NOT NULL THEN 'PP'
                ELSE 'U'
            END, 
            ',' ORDER BY "SplitNum"
        ) AS split_pattern
    FROM public.paysplit
    WHERE "DatePay" >= '2023-01-01'
    GROUP BY "PayNum"
),

-- 4. Unearned Income Patterns
UnearnedPatterns AS (
    SELECT 
        "UnearnedType",
        COUNT(*) AS split_count,
        COUNT(DISTINCT "PayNum") AS payment_count,
        ROUND(AVG("SplitAmt")::numeric, 2) AS avg_amount,
        MIN("SplitAmt") AS min_amount,
        MAX("SplitAmt") AS max_amount,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY "SplitAmt") AS median_amount
    FROM public.paysplit
    WHERE "DatePay" >= '2023-01-01'
        AND "UnearnedType" IS NOT NULL
    GROUP BY "UnearnedType"
),

-- 5. Amount Distribution Buckets
AmountDistribution AS (
    SELECT 
        CASE 
            WHEN "SplitAmt" <= 0 THEN 'Negative/Zero'
            WHEN "SplitAmt" <= 100 THEN '0-100'
            WHEN "SplitAmt" <= 500 THEN '101-500'
            WHEN "SplitAmt" <= 1000 THEN '501-1000'
            WHEN "SplitAmt" <= 5000 THEN '1001-5000'
            ELSE 'Over 5000'
        END AS amount_range,
        COUNT(*) AS split_count,
        ROUND(AVG("SplitAmt")::numeric, 2) AS avg_amount,
        COUNT(DISTINCT "PayNum") AS unique_payments
    FROM public.paysplit
    WHERE "DatePay" >= '2023-01-01'
    GROUP BY 
        CASE 
            WHEN "SplitAmt" <= 0 THEN 'Negative/Zero'
            WHEN "SplitAmt" <= 100 THEN '0-100'
            WHEN "SplitAmt" <= 500 THEN '101-500'
            WHEN "SplitAmt" <= 1000 THEN '501-1000'
            WHEN "SplitAmt" <= 5000 THEN '1001-5000'
            ELSE 'Over 5000'
        END
),

-- 6. Split Patterns Over Time
TimePatterns AS (
    SELECT 
        DATE_TRUNC('month', "DatePay")::date AS month,
        COUNT(*) AS total_splits,
        COUNT(DISTINCT "PayNum") AS unique_payments,
        ROUND(AVG("SplitAmt")::numeric, 2) AS avg_amount,
        COUNT(CASE WHEN "UnearnedType" IN (288, 439) THEN 1 END) AS unearned_count
    FROM public.paysplit
    WHERE "DatePay" >= '2023-01-01'
    GROUP BY DATE_TRUNC('month', "DatePay")::date
    ORDER BY month
)

-- Export each analysis result
SELECT 'Overall Stats' as analysis_type, row_to_json(s)::text as data
FROM OverallStats s
UNION ALL
SELECT 'Split Types', row_to_json(s)::text
FROM SplitTypes s
UNION ALL
SELECT 'Split Counts', row_to_json(s)::text
FROM SplitCounts s
UNION ALL
SELECT 'Unearned Patterns', row_to_json(s)::text
FROM UnearnedPatterns s
UNION ALL
SELECT 'Amount Distribution', row_to_json(s)::text
FROM AmountDistribution s
UNION ALL
SELECT 'Time Patterns', row_to_json(s)::text
FROM TimePatterns s
ORDER BY analysis_type;
