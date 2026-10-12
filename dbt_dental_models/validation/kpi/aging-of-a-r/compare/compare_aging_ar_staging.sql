-- KPI validation: mart_ar_summary vs dim_patient / int_ar_balance reconstruction
-- Run in DBeaver against PostgreSQL opendental_analytics (analytics_user).
--
-- Internal fidelity (no OD export required).
--   - total: SUM(mart.total_balance) should equal SUM(dim_patient.total_balance)
--     for patients with total_balance > 0 (Patient / Inactive), latest snapshot.
--   - buckets: SUM of scaled mart buckets should equal mart total (model test).

WITH params AS (
    SELECT CURRENT_DATE AS snapshot_date
),
mart AS (
    SELECT
        MAX(m.snapshot_date) AS snapshot_date,
        SUM(m.total_balance)::numeric(18, 2) AS mart_total,
        SUM(m.balance_0_30_days)::numeric(18, 2) AS mart_0_30,
        SUM(m.balance_31_60_days)::numeric(18, 2) AS mart_31_60,
        SUM(m.balance_61_90_days)::numeric(18, 2) AS mart_61_90,
        SUM(m.balance_over_90_days)::numeric(18, 2) AS mart_over_90,
        COUNT(DISTINCT m.patient_id) AS mart_patients
    FROM marts.mart_ar_summary m
    CROSS JOIN params p
    WHERE m.snapshot_date = p.snapshot_date
),
dim AS (
    SELECT
        SUM(p.total_balance)::numeric(18, 2) AS dim_total,
        COUNT(*) AS dim_patients
    FROM marts.dim_patient p
    WHERE p.total_balance > 0
      AND COALESCE(p.patient_status, 'Patient') IN ('Patient', 'Inactive')
),
bucket_check AS (
    SELECT
        m.mart_total,
        (m.mart_0_30 + m.mart_31_60 + m.mart_61_90 + m.mart_over_90)::numeric(18, 2) AS mart_buckets_sum,
        m.mart_total
            - (m.mart_0_30 + m.mart_31_60 + m.mart_61_90 + m.mart_over_90) AS buckets_vs_total_diff
    FROM mart m
)
SELECT
    m.snapshot_date,
    d.dim_total,
    m.mart_total,
    m.mart_total - d.dim_total AS mart_vs_dim_diff,
    m.mart_patients,
    d.dim_patients,
    b.mart_buckets_sum,
    b.buckets_vs_total_diff,
    CASE
        WHEN m.snapshot_date IS NULL THEN 'FAIL — no mart rows for snapshot_date'
        WHEN ABS(m.mart_total - d.dim_total) <= 0.01
             AND ABS(b.buckets_vs_total_diff) <= 0.05 THEN 'PASS'
        WHEN ABS(m.mart_total - d.dim_total) <= 0.01 THEN 'WARN — total OK, bucket sum drift'
        ELSE 'FAIL'
    END AS staging_validation_status
FROM mart m
CROSS JOIN dim d
CROSS JOIN bucket_check b;
