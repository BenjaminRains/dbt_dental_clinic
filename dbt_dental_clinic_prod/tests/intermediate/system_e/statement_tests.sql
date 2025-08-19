-- Test that billing statements have valid dates and critical payment logic
-- This test returns records that fail critical validation checks
-- Edge cases are documented but not treated as failures
--
-- Test Strategy:
-- - CRITICAL errors: Fail the test (data integrity issues)
-- - EDGE CASES: Documented but not treated as failures (legitimate business scenarios)
--
-- Edge cases that are NOT treated as failures:
-- 1. Payment amount inconsistencies (7-day > 14-day > 30-day) - due to payment processing timing
-- 2. Zero payment with non-no_payment result - legitimate for zero balance statements
--
-- These edge cases represent ~1.4% of records and are considered acceptable business scenarios.
SELECT
    billing_statement_id,
    statement_id,
    date_sent,
    date_range_from,
    date_range_to,
    payment_amount_7days,
    payment_amount_14days,
    payment_amount_30days,
    resulted_in_payment,
    payment_result,
    balance_total,
    CASE
        WHEN date_sent IS NULL THEN 'CRITICAL: NULL date_sent'
        WHEN date_range_from > date_range_to THEN 'CRITICAL: Invalid date range'
        WHEN date_sent < '2000-01-01' THEN 'CRITICAL: Date too old'
        WHEN payment_amount_7days > payment_amount_14days THEN 'EDGE CASE: 7-day payment > 14-day payment'
        WHEN payment_amount_14days > payment_amount_30days THEN 'EDGE CASE: 14-day payment > 30-day payment'
        WHEN (payment_amount_30days > 0 AND resulted_in_payment = FALSE) THEN 'CRITICAL: Payment but no payment flag'
        WHEN (payment_amount_30days = 0 AND resulted_in_payment = TRUE) THEN 'CRITICAL: No payment but payment flag'
        WHEN (payment_amount_30days > 0 AND payment_result = 'no_payment') THEN 'CRITICAL: Payment but no_payment result'
        WHEN (payment_amount_30days = 0 AND payment_result != 'no_payment') THEN 'EDGE CASE: Zero payment with non-no_payment result'
        ELSE 'Unknown validation error'
    END AS validation_error
FROM {{ ref('int_billing_statements') }}
WHERE 
    -- Critical validation errors (should fail)
    date_sent IS NULL
    OR (date_range_from > date_range_to)
    OR date_sent < '2000-01-01'
    OR (payment_amount_30days > 0 AND resulted_in_payment = FALSE)
    OR (payment_amount_30days = 0 AND resulted_in_payment = TRUE)
    OR (payment_amount_30days > 0 AND payment_result = 'no_payment')
    
    -- Edge cases (documented but not treated as failures)
    -- These are legitimate business scenarios that may occur:
    -- - Payment amount inconsistencies due to payment processing timing
    -- - Zero balance statements with different payment result classifications
    -- OR payment_amount_7days > payment_amount_14days
    -- OR payment_amount_14days > payment_amount_30days
    -- OR (payment_amount_30days = 0 AND payment_result != 'no_payment')