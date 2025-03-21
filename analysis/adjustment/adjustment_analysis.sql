/*
ADJUSTMENT DATA ANALYSIS
=======================
Purpose: Analyze adjustment patterns, distributions, and key characteristics
to inform data modeling decisions and validation rules.

Analysis Sections:
1. Basic Statistics and Distributions
2. Temporal Patterns
3. Financial Analysis
4. Category Analysis
5. Provider/Clinic Patterns
6. Data Quality Checks
*/

-- 1. Basic Statistics and Distributions
-- 1.1 Overall volume and financial impact
SELECT 
    COUNT(*) as total_adjustments,
    COUNT(DISTINCT a."PatNum") as unique_patients,
    SUM(a."AdjAmt") as total_adjustment_amount,
    AVG(a."AdjAmt") as avg_adjustment_amount,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY a."AdjAmt") as median_adjustment
FROM public.adjustment a
WHERE a."ProcDate" >= '2023-01-01';

-- 1.2 Distribution by adjustment type with definitions
SELECT 
    a."AdjType",
    d."ItemName" as adjustment_type_name,
    d."ItemValue" as adjustment_type_description,
    COUNT(*) as adjustment_count,
    SUM(a."AdjAmt") as total_amount,
    AVG(a."AdjAmt") as avg_amount,
    MIN(a."AdjAmt") as min_amount,
    MAX(a."AdjAmt") as max_amount
FROM public.adjustment a
LEFT JOIN public.definition d ON d."DefNum" = a."AdjType" 
    AND d."Category" = 1  -- Category 1 is for adjustment types
    AND d."IsHidden" = 0  -- Only show active definitions
WHERE a."ProcDate" >= '2023-01-01'
GROUP BY 
    a."AdjType",
    d."ItemName",
    d."ItemValue"
ORDER BY SUM(a."AdjAmt") DESC;

-- 2. Temporal Patterns
-- 2.1 Monthly trends
SELECT 
    DATE_TRUNC('month', a."AdjDate") as month,
    COUNT(*) as adjustment_count,
    SUM(a."AdjAmt") as total_amount
FROM public.adjustment a
WHERE a."ProcDate" >= '2023-01-01'
GROUP BY DATE_TRUNC('month', a."AdjDate")
ORDER BY month;


-- 3. Financial Analysis
-- 3.1 Distribution of adjustment amounts
SELECT 
    CASE 
        WHEN a."AdjAmt" >= 1000 THEN 'Very Large (≥$1000)'
        WHEN a."AdjAmt" >= 500 THEN 'Large ($500-999)'
        WHEN a."AdjAmt" >= 100 THEN 'Medium ($100-499)'
        WHEN a."AdjAmt" >= 50 THEN 'Small ($50-99)'
        ELSE 'Very Small (<$50)'
    END as amount_category,
    COUNT(*) as count,
    SUM(a."AdjAmt") as total_amount
FROM public.adjustment a
WHERE a."ProcDate" >= '2023-01-01'
GROUP BY 1
ORDER BY 
    CASE 
        WHEN CASE 
            WHEN a."AdjAmt" >= 1000 THEN 'Very Large (≥$1000)'
            WHEN a."AdjAmt" >= 500 THEN 'Large ($500-999)'
            WHEN a."AdjAmt" >= 100 THEN 'Medium ($100-499)'
            WHEN a."AdjAmt" >= 50 THEN 'Small ($50-99)'
            ELSE 'Very Small (<$50)'
        END = 'Very Small (<$50)' THEN 1
        WHEN CASE 
            WHEN a."AdjAmt" >= 1000 THEN 'Very Large (≥$1000)'
            WHEN a."AdjAmt" >= 500 THEN 'Large ($500-999)'
            WHEN a."AdjAmt" >= 100 THEN 'Medium ($100-499)'
            WHEN a."AdjAmt" >= 50 THEN 'Small ($50-99)'
            ELSE 'Very Small (<$50)'
        END = 'Small ($50-99)' THEN 2
        WHEN CASE 
            WHEN a."AdjAmt" >= 1000 THEN 'Very Large (≥$1000)'
            WHEN a."AdjAmt" >= 500 THEN 'Large ($500-999)'
            WHEN a."AdjAmt" >= 100 THEN 'Medium ($100-499)'
            WHEN a."AdjAmt" >= 50 THEN 'Small ($50-99)'
            ELSE 'Very Small (<$50)'
        END = 'Medium ($100-499)' THEN 3
        WHEN CASE 
            WHEN a."AdjAmt" >= 1000 THEN 'Very Large (≥$1000)'
            WHEN a."AdjAmt" >= 500 THEN 'Large ($500-999)'
            WHEN a."AdjAmt" >= 100 THEN 'Medium ($100-499)'
            WHEN a."AdjAmt" >= 50 THEN 'Small ($50-99)'
            ELSE 'Very Small (<$50)'
        END = 'Large ($500-999)' THEN 4
        WHEN CASE 
            WHEN a."AdjAmt" >= 1000 THEN 'Very Large (≥$1000)'
            WHEN a."AdjAmt" >= 500 THEN 'Large ($500-999)'
            WHEN a."AdjAmt" >= 100 THEN 'Medium ($100-499)'
            WHEN a."AdjAmt" >= 50 THEN 'Small ($50-99)'
            ELSE 'Very Small (<$50)'
        END = 'Very Large (≥$1000)' THEN 5
    END;

-- 3.2 Positive vs Negative adjustments
SELECT 
    CASE WHEN a."AdjAmt" > 0 THEN 'Positive'
         WHEN a."AdjAmt" < 0 THEN 'Negative'
         ELSE 'Zero'
    END as adjustment_direction,
    COUNT(*) as count,
    SUM(a."AdjAmt") as total_amount,
    AVG(a."AdjAmt") as avg_amount
FROM public.adjustment a
WHERE a."ProcDate" >= '2023-01-01'
GROUP BY 1;

-- 4. Category Analysis
-- 4.1 Procedure-linked adjustments
SELECT 
    CASE WHEN a."ProcNum" > 0 THEN 'Procedure-linked'
         ELSE 'Standalone'
    END as adjustment_type,
    COUNT(*) as count,
    AVG(a."AdjAmt") as avg_amount,
    SUM(a."AdjAmt") as total_amount
FROM public.adjustment a
WHERE a."ProcDate" >= '2023-01-01'
GROUP BY 1;

-- 4.2 Retroactive adjustments
SELECT 
    CASE WHEN a."ProcDate" != a."AdjDate" THEN 'Retroactive'
         ELSE 'Same-day'
    END as timing_type,
    COUNT(*) as count,
    AVG(a."AdjAmt") as avg_amount
FROM public.adjustment a
WHERE a."ProcNum" > 0 
AND a."ProcDate" >= '2023-01-01'
GROUP BY 1;

-- 5. Provider/Clinic Patterns
-- 5.1 Provider distribution
SELECT 
    a."ProvNum",
    COUNT(*) as adjustment_count,
    SUM(a."AdjAmt") as total_amount,
    AVG(a."AdjAmt") as avg_amount
FROM public.adjustment a
WHERE a."ProvNum" > 0
AND a."ProcDate" >= '2023-01-01'
GROUP BY a."ProvNum"
ORDER BY COUNT(*) DESC
LIMIT 10;

-- 6. Data Quality Checks
-- 6.1 Null analysis
SELECT 
    COUNT(*) as total_records,
    SUM(CASE WHEN a."AdjDate" IS NULL THEN 1 ELSE 0 END) as null_dates,
    SUM(CASE WHEN a."PatNum" IS NULL THEN 1 ELSE 0 END) as null_patients,
    SUM(CASE WHEN a."AdjAmt" IS NULL THEN 1 ELSE 0 END) as null_amounts,
    SUM(CASE WHEN a."ProvNum" IS NULL THEN 1 ELSE 0 END) as null_providers,
    SUM(CASE WHEN a."ClinicNum" IS NULL THEN 1 ELSE 0 END) as null_clinics
FROM public.adjustment a
WHERE a."ProcDate" >= '2023-01-01';

-- 6.2 Date range analysis
SELECT 
    MIN(a."AdjDate") as earliest_date,
    MAX(a."AdjDate") as latest_date,
    MIN(a."ProcDate") as earliest_proc_date,
    MAX(a."ProcDate") as latest_proc_date
FROM public.adjustment a
WHERE a."ProcDate" >= '2023-01-01';

-- 6.3 Suspicious patterns
SELECT 
    a."AdjNum",
    a."AdjDate",
    a."ProcDate",
    a."AdjAmt",
    a."PatNum",
    a."ProvNum",
    a."ClinicNum",
    a."AdjNote"
FROM public.adjustment a
WHERE a."ProcDate" >= '2023-01-01'
AND (
    a."AdjAmt" = 0
    OR a."AdjDate" > CURRENT_DATE
    OR a."AdjDate" < a."ProcDate"  -- Adjustment date before procedure date
    OR (a."ProcNum" > 0 AND a."ProcDate" IS NULL)  -- Has procedure number but no date
    OR (a."AdjAmt" IS NOT NULL AND a."PatNum" IS NULL)  -- Amount without patient
    OR ABS(a."AdjAmt") > 10000  -- Potentially unusual large amounts
)
ORDER BY ABS(a."AdjAmt") DESC
LIMIT 100;