-- KPI validation layer 4: API / frontend path vs mart vs OD golden
-- Run in DBeaver against PostgreSQL opendental_analytics (analytics_user).
--
-- The clinic API (api/services/kpi_service.py) runs the SAME SELECT as api_equivalent below.
-- Practice Manager Home displays net_collections_amount (hero), patient/insurance/count (subtitle).
-- PASS when api columns = mart columns = OD golden net on each spot-check date.
--
-- After SQL PASS: smoke-test GET /kpi/daily-collections?payment_date=YYYY-MM-DD (see scripts/README.md).

WITH golden_dates AS (
    SELECT * FROM (VALUES
        (DATE '2026-06-24', 11197.40::numeric(18, 2)),
        (DATE '2025-10-07', 21747.30::numeric(18, 2)),
        (DATE '2025-11-08',  3791.65::numeric(18, 2))
    ) AS t(payment_date, od_net_collections)
),
-- Mirrors get_daily_collections_kpi() in api/services/kpi_service.py
api_equivalent AS (
    SELECT
        m.payment_date,
        m.net_collections_amount,
        m.patient_payment_amount,
        m.insurance_payment_amount,
        m.payment_count,
        true AS has_data
    FROM marts.mart_daily_payments m
    INNER JOIN golden_dates g ON m.payment_date = g.payment_date
),
mart AS (
    SELECT m.*
    FROM marts.mart_daily_payments m
    INNER JOIN golden_dates g ON m.payment_date = g.payment_date
)
SELECT
    g.payment_date,
    g.od_net_collections,
    a.net_collections_amount AS api_net_collections,
    m.net_collections_amount AS mart_net_collections,
    a.net_collections_amount - g.od_net_collections AS api_vs_od_diff,
    a.net_collections_amount - m.net_collections_amount AS api_vs_mart_diff,
    a.patient_payment_amount AS api_patient,
    a.insurance_payment_amount AS api_insurance,
    a.payment_count AS api_payment_count,
    CASE
        WHEN a.payment_date IS NULL THEN 'FAIL — no mart row (API would return has_data=false, zeros)'
        WHEN abs(a.net_collections_amount - g.od_net_collections) <= 10.00 THEN 'PASS'
        WHEN abs(a.net_collections_amount - g.od_net_collections) / nullif(abs(g.od_net_collections), 0) < 0.005 THEN 'PASS'
        ELSE 'FAIL'
    END AS validation_status,
    'Frontend shows api_net_collections via GET /kpi/daily-collections' AS frontend_field
FROM golden_dates g
LEFT JOIN api_equivalent a ON g.payment_date = a.payment_date
LEFT JOIN mart m ON g.payment_date = m.payment_date
ORDER BY g.payment_date;

-- Latest-date endpoint (Practice Manager "Latest" toggle):
-- Mirrors get_latest_collections_date() — MAX(payment_date) WHERE payment_count > 0
SELECT
    'latest_collections_date' AS check_name,
    max(payment_date) AS api_latest_payment_date,
    max(payment_date) FILTER (WHERE payment_count > 0) AS expected_from_mart
FROM marts.mart_daily_payments;
