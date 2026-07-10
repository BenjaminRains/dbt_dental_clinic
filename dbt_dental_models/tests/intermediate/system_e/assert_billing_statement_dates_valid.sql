-- Failing rows: billing statements with invalid / missing dates
SELECT
    billing_statement_id,
    statement_id,
    date_sent,
    date_range_from,
    date_range_to
FROM {{ ref('int_billing_statements') }}
WHERE
    date_sent IS NULL
    OR (date_range_from > date_range_to)
    OR date_sent < '2000-01-01'
