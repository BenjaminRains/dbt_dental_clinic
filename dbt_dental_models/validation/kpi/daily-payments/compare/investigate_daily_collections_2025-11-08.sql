-- KPI investigation: mart_daily_payments vs OD Daily Payments (2025-11-08, Saturday)
-- Run in DBeaver against PostgreSQL opendental_analytics (analytics_user).
--
-- Mart vs staging: PASS $3,791.65 (patient only; no claim payments)
-- OD golden: od_daily_payments_11082025_11082025.csv — PASS $3,791.65 (exact match)

-- =============================================================================
-- Query 1: Mart breakdown
-- =============================================================================
SELECT *
FROM marts.mart_daily_payments
WHERE payment_date = DATE '2025-11-08';

-- =============================================================================
-- Query 2: Staging reconstruction (OD recipe)
-- =============================================================================
WITH patient AS (
    SELECT sum(payment_amount)::numeric(18, 2) AS total
    FROM staging.stg_opendental__payment
    WHERE payment_date = DATE '2025-11-08'
      AND payment_amount <> 0
),
insurance AS (
    SELECT sum(check_amount)::numeric(18, 2) AS total
    FROM staging.stg_opendental__claimpayment
    WHERE check_date = DATE '2025-11-08'
)
SELECT
    p.total AS patient_staging,
    i.total AS insurance_staging,
    p.total + i.total AS reconstructed_net
FROM patient p
CROSS JOIN insurance i;

-- =============================================================================
-- Query 3: PayType breakdown (compare to OD section subtotals)
-- =============================================================================
SELECT
    p.payment_type_id,
    d.item_name,
    count(*) AS payment_count,
    sum(p.payment_amount)::numeric(18, 2) AS header_total
FROM staging.stg_opendental__payment p
LEFT JOIN staging.stg_opendental__definition d
    ON d.definition_id = p.payment_type_id
WHERE p.payment_date = DATE '2025-11-08'
  AND p.payment_amount <> 0
GROUP BY 1, 2
ORDER BY header_total DESC;
