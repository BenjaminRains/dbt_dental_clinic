-- Investigation Queries for Payment Type Mappings
-- Date: 2026-01-23
-- Purpose: Investigate payment_type to verify all payment_type_id values are properly mapped
-- Related: validation/VALIDATION_TEMPLATE.md

-- ============================================================================
-- Query 1: Payment Type ID Distribution in Source
-- ============================================================================

-- Query 1.1: Summary of payment_type_id values in source (staging)
SELECT 
    'Payment Type ID - Source Distribution' as investigation_type,
    payment_type_id,
    COUNT(*) as payment_count,
    COUNT(DISTINCT payment_id) as distinct_payments,
    COUNT(DISTINCT patient_id) as distinct_patients,
    SUM(payment_amount) as total_amount,
    AVG(payment_amount) as avg_amount,
    MIN(payment_amount) as min_amount,
    MAX(payment_amount) as max_amount,
    COUNT(*) FILTER (WHERE payment_amount < 0) as negative_count,
    COUNT(*) FILTER (WHERE payment_amount > 0) as positive_count,
    COUNT(*) FILTER (WHERE payment_amount = 0) as zero_count
FROM staging.stg_opendental__payment
WHERE payment_date >= '2023-01-01'
GROUP BY payment_type_id
ORDER BY payment_type_id;

-- Query 1.2: Check for NULL payment_type_id values
SELECT 
    'Payment Type ID - NULL Check' as investigation_type,
    COUNT(*) as total_payments,
    COUNT(*) FILTER (WHERE payment_type_id IS NULL) as null_payment_type_id,
    COUNT(*) FILTER (WHERE payment_type_id IS NOT NULL) as non_null_payment_type_id
FROM staging.stg_opendental__payment
WHERE payment_date >= '2023-01-01';

-- Query 1.3: Payment Type ID distribution in intermediate model
SELECT 
    'Payment Type ID - Intermediate Distribution' as investigation_type,
    payment_type_id,
    COUNT(*) as split_count,
    COUNT(DISTINCT payment_id) as distinct_payments,
    COUNT(DISTINCT patient_id) as distinct_patients,
    SUM(split_amount) as total_split_amount,
    AVG(split_amount) as avg_split_amount
FROM intermediate.int_payment_split
GROUP BY payment_type_id
ORDER BY payment_type_id;

-- ============================================================================
-- Query 2: Payment Type Distribution in Mart
-- ============================================================================

-- Query 2.1: Summary of payment_type values
SELECT 
    'Payment Type - Summary' as investigation_type,
    payment_type,
    COUNT(*) as payment_count,
    COUNT(DISTINCT payment_id) as distinct_payments,
    COUNT(DISTINCT patient_id) as distinct_patients,
    SUM(payment_amount) as total_amount,
    AVG(payment_amount) as avg_amount,
    MIN(payment_amount) as min_amount,
    MAX(payment_amount) as max_amount
FROM marts.fact_payment
GROUP BY payment_type
ORDER BY payment_count DESC;

-- Query 2.2: Breakdown by payment_type_id and payment_type
SELECT 
    'Payment Type - Breakdown by ID' as analysis,
    payment_type_id,
    payment_type,
    COUNT(*) as payment_count,
    SUM(payment_amount) as total_amount,
    AVG(payment_amount) as avg_amount
FROM marts.fact_payment
GROUP BY payment_type_id, payment_type
ORDER BY payment_type_id, payment_type;

-- Query 2.3: Sample records for each payment_type
SELECT 
    'Payment Type - Sample Records' as analysis,
    payment_id,
    payment_type_id,
    payment_type,
    payment_amount,
    payment_date,
    patient_id,
    payment_direction,
    payment_size_category
FROM marts.fact_payment
WHERE payment_type = 'Unknown'
ORDER BY payment_date DESC, payment_id
LIMIT 30;

-- ============================================================================
-- Query 3: Unknown Payment Type Analysis
-- ============================================================================

-- Query 3.1: Detailed analysis of Unknown payment_type
SELECT 
    'Payment Type - Unknown Analysis' as investigation_type,
    COUNT(*) as total_unknown_records,
    COUNT(DISTINCT payment_id) as distinct_payments,
    COUNT(DISTINCT patient_id) as distinct_patients,
    SUM(payment_amount) as total_amount,
    AVG(payment_amount) as avg_amount,
    MIN(payment_amount) as min_amount,
    MAX(payment_amount) as max_amount,
    COUNT(*) FILTER (WHERE payment_amount < 0) as negative_count,
    COUNT(*) FILTER (WHERE payment_amount > 0) as positive_count,
    COUNT(*) FILTER (WHERE payment_amount = 0) as zero_count,
    MIN(payment_date) as earliest_payment,
    MAX(payment_date) as latest_payment
FROM marts.fact_payment
WHERE payment_type = 'Unknown';

-- Query 3.2: Breakdown of Unknown by payment_type_id values
SELECT 
    'Payment Type - Unknown by ID' as analysis,
    payment_type_id,
    COUNT(*) as payment_count,
    COUNT(DISTINCT payment_id) as distinct_payments,
    SUM(payment_amount) as total_amount,
    AVG(payment_amount) as avg_amount,
    MIN(payment_amount) as min_amount,
    MAX(payment_amount) as max_amount,
    COUNT(*) FILTER (WHERE payment_amount < 0) as negative_count,
    COUNT(*) FILTER (WHERE payment_amount > 0) as positive_count,
    COUNT(*) FILTER (WHERE payment_amount = 0) as zero_count
FROM marts.fact_payment
WHERE payment_type = 'Unknown'
GROUP BY payment_type_id
ORDER BY payment_count DESC;

-- Query 3.3: Sample Unknown records by payment_type_id
SELECT 
    'Payment Type - Unknown Samples' as analysis,
    payment_id,
    payment_type_id,
    payment_type,
    payment_amount,
    payment_date,
    patient_id,
    payment_direction,
    payment_size_category,
    payment_source
FROM marts.fact_payment
WHERE payment_type = 'Unknown'
ORDER BY payment_type_id, payment_date DESC
LIMIT 50;

-- ============================================================================
-- Query 4: Payment Type ID vs Payment Type Comparison
-- ============================================================================

-- Query 4.1: Cross-tabulation of payment_type_id and payment_type
SELECT 
    'Payment Type ID - Category Cross Tab' as analysis,
    payment_type_id,
    payment_type,
    COUNT(*) as payment_count,
    SUM(payment_amount) as total_amount,
    AVG(payment_amount) as avg_amount
FROM marts.fact_payment
GROUP BY payment_type_id, payment_type
ORDER BY payment_type_id, payment_type;

-- Query 4.2: Check for unexpected mappings (should all map correctly based on CASE statement)
SELECT 
    'Payment Type ID - Unexpected Mappings' as analysis,
    payment_type_id,
    payment_type,
    CASE 
        WHEN payment_type_id = 0 AND payment_type != 'Patient' THEN 'ID 0 not mapped to Patient'
        WHEN payment_type_id = 1 AND payment_type != 'Insurance' THEN 'ID 1 not mapped to Insurance'
        WHEN payment_type_id = 2 AND payment_type != 'Partial' THEN 'ID 2 not mapped to Partial'
        WHEN payment_type_id = 3 AND payment_type != 'PrePayment' THEN 'ID 3 not mapped to PrePayment'
        WHEN payment_type_id = 4 AND payment_type != 'Adjustment' THEN 'ID 4 not mapped to Adjustment'
        WHEN payment_type_id = 5 AND payment_type != 'Refund' THEN 'ID 5 not mapped to Refund'
        WHEN payment_type_id NOT IN (0, 1, 2, 3, 4, 5) AND payment_type != 'Unknown' THEN 'Unmapped ID not Unknown'
        ELSE 'OK'
    END as mapping_issue,
    COUNT(*) as payment_count
FROM marts.fact_payment
WHERE (
    (payment_type_id = 0 AND payment_type != 'Patient')
    OR (payment_type_id = 1 AND payment_type != 'Insurance')
    OR (payment_type_id = 2 AND payment_type != 'Partial')
    OR (payment_type_id = 3 AND payment_type != 'PrePayment')
    OR (payment_type_id = 4 AND payment_type != 'Adjustment')
    OR (payment_type_id = 5 AND payment_type != 'Refund')
    OR (payment_type_id NOT IN (0, 1, 2, 3, 4, 5) AND payment_type != 'Unknown')
)
GROUP BY payment_type_id, payment_type,
    CASE 
        WHEN payment_type_id = 0 AND payment_type != 'Patient' THEN 'ID 0 not mapped to Patient'
        WHEN payment_type_id = 1 AND payment_type != 'Insurance' THEN 'ID 1 not mapped to Insurance'
        WHEN payment_type_id = 2 AND payment_type != 'Partial' THEN 'ID 2 not mapped to Partial'
        WHEN payment_type_id = 3 AND payment_type != 'PrePayment' THEN 'ID 3 not mapped to PrePayment'
        WHEN payment_type_id = 4 AND payment_type != 'Adjustment' THEN 'ID 4 not mapped to Adjustment'
        WHEN payment_type_id = 5 AND payment_type != 'Refund' THEN 'ID 5 not mapped to Refund'
        WHEN payment_type_id NOT IN (0, 1, 2, 3, 4, 5) AND payment_type != 'Unknown' THEN 'Unmapped ID not Unknown'
        ELSE 'OK'
    END
ORDER BY payment_count DESC;

-- ============================================================================
-- Query 5: Payment Type ID Values from Documentation
-- ============================================================================

-- Query 5.1: Check which documented payment_type_id values exist in source
-- Based on PAYMENT_TYPE_ID_DOCUMENTATION.md, these values should exist:
-- [0, 69, 70, 71, 72, 261, 303, 391, 412, 417, 464, 465, 466, 467, 469, 574, 634, 645, 646, 647, 661]
SELECT 
    'Payment Type ID - Documented Values Check' as analysis,
    payment_type_id,
    CASE 
        WHEN payment_type_id IN (0, 69, 70, 71, 72, 261, 303, 391, 412, 417, 464, 465, 466, 467, 469, 574, 634, 645, 646, 647, 661) THEN 'Documented'
        ELSE 'Not Documented'
    END as documentation_status,
    COUNT(*) as payment_count,
    SUM(payment_amount) as total_amount,
    AVG(payment_amount) as avg_amount
FROM staging.stg_opendental__payment
WHERE payment_date >= '2023-01-01'
GROUP BY payment_type_id
ORDER BY payment_type_id;

-- Query 5.2: Compare documented values vs current mappings
SELECT 
    'Payment Type ID - Mapping Coverage' as analysis,
    payment_type_id,
    CASE 
        WHEN payment_type_id IN (0, 1, 2, 3, 4, 5) THEN 'Currently Mapped'
        WHEN payment_type_id IN (69, 70, 71, 72, 261, 303, 391, 412, 417, 464, 465, 466, 467, 469, 574, 634, 645, 646, 647, 661) THEN 'Documented but Unmapped'
        ELSE 'Not Documented'
    END as mapping_status,
    COUNT(*) as payment_count,
    SUM(payment_amount) as total_amount,
    AVG(payment_amount) as avg_amount,
    MIN(payment_date) as earliest_payment,
    MAX(payment_date) as latest_payment
FROM staging.stg_opendental__payment
WHERE payment_date >= '2023-01-01'
GROUP BY payment_type_id
ORDER BY payment_count DESC;

-- ============================================================================
-- Query 6: Payment Direction and Size Category Analysis
-- ============================================================================

-- Query 6.1: Payment Direction distribution
SELECT 
    'Payment Direction - Distribution' as analysis,
    payment_direction,
    COUNT(*) as payment_count,
    SUM(payment_amount) as total_amount,
    AVG(payment_amount) as avg_amount,
    MIN(payment_amount) as min_amount,
    MAX(payment_amount) as max_amount
FROM marts.fact_payment
GROUP BY payment_direction
ORDER BY payment_count DESC;

-- Query 6.2: Payment Size Category distribution
SELECT 
    'Payment Size Category - Distribution' as analysis,
    payment_size_category,
    COUNT(*) as payment_count,
    SUM(payment_amount) as total_amount,
    AVG(payment_amount) as avg_amount,
    MIN(payment_amount) as min_amount,
    MAX(payment_amount) as max_amount
FROM marts.fact_payment
GROUP BY payment_size_category
ORDER BY payment_count DESC;

-- Query 6.3: Check for edge cases in payment_direction and payment_size_category
SELECT 
    'Payment Categories - Edge Cases' as analysis,
    payment_direction,
    payment_size_category,
    COUNT(*) as payment_count,
    AVG(payment_amount) as avg_amount,
    MIN(payment_amount) as min_amount,
    MAX(payment_amount) as max_amount
FROM marts.fact_payment
WHERE (payment_direction = 'Zero' AND payment_size_category != 'Small')
   OR (payment_direction = 'Refund' AND payment_size_category NOT IN ('Small', 'Medium', 'Large', 'Very Large', 'Negative'))
   OR (payment_direction = 'Income' AND payment_size_category = 'Negative')
GROUP BY payment_direction, payment_size_category
ORDER BY payment_count DESC;

-- ============================================================================
-- Query 7: Summary Statistics
-- ============================================================================

-- Query 7.1: Overall summary
SELECT 
    'Payment Type - Summary Statistics' as analysis,
    COUNT(*) as total_payments,
    COUNT(*) FILTER (WHERE payment_type = 'Patient') as patient_count,
    COUNT(*) FILTER (WHERE payment_type = 'Insurance') as insurance_count,
    COUNT(*) FILTER (WHERE payment_type = 'Partial') as partial_count,
    COUNT(*) FILTER (WHERE payment_type = 'PrePayment') as prepayment_count,
    COUNT(*) FILTER (WHERE payment_type = 'Adjustment') as adjustment_count,
    COUNT(*) FILTER (WHERE payment_type = 'Refund') as refund_count,
    COUNT(*) FILTER (WHERE payment_type = 'Unknown') as unknown_count,
    ROUND(100.0 * COUNT(*) FILTER (WHERE payment_type = 'Unknown') / COUNT(*), 2) as unknown_percentage,
    COUNT(DISTINCT payment_type_id) as distinct_payment_type_ids,
    COUNT(DISTINCT payment_type_id) FILTER (WHERE payment_type = 'Unknown') as unmapped_payment_type_ids
FROM marts.fact_payment;

-- Query 7.2: Payment Type ID coverage summary
SELECT 
    'Payment Type ID - Coverage Summary' as analysis,
    COUNT(DISTINCT payment_type_id) as total_distinct_ids,
    COUNT(DISTINCT payment_type_id) FILTER (WHERE payment_type_id IN (0, 1, 2, 3, 4, 5)) as mapped_ids,
    COUNT(DISTINCT payment_type_id) FILTER (WHERE payment_type_id NOT IN (0, 1, 2, 3, 4, 5)) as unmapped_ids,
    STRING_AGG(DISTINCT payment_type_id::text, ', ' ORDER BY payment_type_id::text) FILTER (WHERE payment_type_id NOT IN (0, 1, 2, 3, 4, 5)) as unmapped_id_list
FROM marts.fact_payment;
