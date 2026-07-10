-- Failing rows: payment result flags inconsistent with 30-day payment amount
SELECT
    billing_statement_id,
    statement_id,
    payment_amount_30days,
    resulted_in_payment,
    payment_result
FROM {{ ref('int_billing_statements') }}
WHERE
    (payment_amount_30days > 0 AND resulted_in_payment = FALSE)
    OR (payment_amount_30days = 0 AND resulted_in_payment = TRUE)
    OR (payment_amount_30days > 0 AND payment_result = 'no_payment')
    OR (payment_amount_30days = 0 AND payment_result != 'no_payment')
