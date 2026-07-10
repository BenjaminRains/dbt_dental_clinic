{{ config(severity='warn') }}

-- Failing rows: negative or non-monotonic 7/14/30-day payment amounts.
-- Warn only: negatives and non-monotonic windows are legitimate when refunds/adjustments
-- post inside the statement payment window.
SELECT
    billing_statement_id,
    statement_id,
    payment_amount_7days,
    payment_amount_14days,
    payment_amount_30days
FROM {{ ref('int_billing_statements') }}
WHERE
    payment_amount_7days < 0
    OR payment_amount_14days < 0
    OR payment_amount_30days < 0
    OR payment_amount_7days > payment_amount_14days
    OR payment_amount_14days > payment_amount_30days
