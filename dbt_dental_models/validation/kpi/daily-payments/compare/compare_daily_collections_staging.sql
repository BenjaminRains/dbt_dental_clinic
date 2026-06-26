-- KPI validation: mart_daily_payments vs staging reconstruction (patient + claimpayment)
-- Run in DBeaver against PostgreSQL opendental_analytics (analytics_user).
--
-- Internal check before OD golden export. PASS when mart_vs_staging_diff = 0.
-- OD reconciliation still requires golden CSV total in compare_daily_collections.sql.

WITH params AS (
    SELECT d::date AS payment_date
    FROM unnest(ARRAY[
        DATE '2026-06-24',
        DATE '2025-10-07',
        DATE '2025-11-08'
    ]) AS t(d)
),
patient AS (
    SELECT
        p.payment_date::date AS payment_date,
        round(sum(p.payment_amount) filter (where p.payment_amount <> 0)::numeric, 2) AS patient_total
    FROM staging.stg_opendental__payment p
    INNER JOIN params x ON p.payment_date::date = x.payment_date
    GROUP BY 1
),
insurance AS (
    SELECT
        c.check_date::date AS payment_date,
        round(sum(c.check_amount::numeric), 2) AS insurance_total
    FROM staging.stg_opendental__claimpayment c
    INNER JOIN params x ON c.check_date::date = x.payment_date
    GROUP BY 1
),
mart AS (
    SELECT
        m.payment_date,
        m.net_collections_amount,
        m.patient_payment_amount,
        m.insurance_payment_amount,
        m.payment_count
    FROM marts.mart_daily_payments m
    INNER JOIN params x ON m.payment_date = x.payment_date
)
SELECT
    x.payment_date,
    trim(to_char(x.payment_date, 'Day')) AS day_name,
    m.net_collections_amount AS mart_net,
    m.patient_payment_amount AS mart_patient,
    m.insurance_payment_amount AS mart_insurance,
    m.payment_count,
    coalesce(p.patient_total, 0) AS staging_patient,
    coalesce(i.insurance_total, 0) AS staging_insurance,
    coalesce(p.patient_total, 0) + coalesce(i.insurance_total, 0) AS staging_reconstructed,
    m.net_collections_amount
        - (coalesce(p.patient_total, 0) + coalesce(i.insurance_total, 0)) AS mart_vs_staging_diff,
    CASE
        WHEN m.payment_date IS NULL THEN 'FAIL — no mart row'
        WHEN m.net_collections_amount
            - (coalesce(p.patient_total, 0) + coalesce(i.insurance_total, 0)) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS staging_validation_status
FROM params x
LEFT JOIN mart m ON m.payment_date = x.payment_date
LEFT JOIN patient p ON p.payment_date = x.payment_date
LEFT JOIN insurance i ON i.payment_date = x.payment_date
ORDER BY x.payment_date;
