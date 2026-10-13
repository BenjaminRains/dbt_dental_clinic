-- KPI validation: practice total AR vs OpenDental Aging of A/R export
-- Run in DBeaver against PostgreSQL opendental_analytics (analytics_user).
--
-- Prerequisites:
--   1. Rebuild mart_ar_summary same calendar day as OD export
--   2. Export OD Aging of A/R CSV to validation/kpi/aging-of-a-r/golden/
--   3. Enter OD total in od_total CTE (report grand total / section total)
--   4. Set snapshot_date to match OD as-of (usually CURRENT_DATE)
--
-- Tolerance (registry): ±$50 or ±0.5%

WITH params AS (
    SELECT CURRENT_DATE AS snapshot_date  -- lock to OD as-of date
),
od_total AS (
    -- 2026-10-12 source proxy, same-day patient full refresh.
    -- Live MySQL patient.BalTotal > 0 AND PatStatus IN (0, 2), guarantor rows.
    -- Printed Aging of A/R CSV is still not exported. Replace this number if that
    -- grand total differs from stored BalTotal.
    SELECT 282624.23::numeric(18, 2) AS total_ar
),
mart_total AS (
    SELECT
        MAX(m.snapshot_date) AS snapshot_date,
        SUM(m.total_balance)::numeric(18, 2) AS total_ar,
        COUNT(DISTINCT m.patient_id) AS patient_count
    FROM marts.mart_ar_summary m
    CROSS JOIN params p
    WHERE m.snapshot_date = p.snapshot_date
),
comparison AS (
    SELECT
        m.snapshot_date,
        o.total_ar AS od_total_ar,
        m.total_ar AS mart_total_ar,
        m.patient_count,
        m.total_ar - o.total_ar AS difference,
        CASE
            WHEN o.total_ar IS NULL THEN 'PENDING — set od_total from golden export'
            WHEN o.total_ar = 0 AND m.total_ar = 0 THEN 'PASS'
            WHEN ABS(m.total_ar - o.total_ar) <= 50.00 THEN 'PASS'
            WHEN ABS(m.total_ar - o.total_ar) / NULLIF(ABS(o.total_ar), 0) < 0.005 THEN 'PASS'
            WHEN ABS(m.total_ar - o.total_ar) / NULLIF(ABS(o.total_ar), 0) < 0.01 THEN 'WARN'
            ELSE 'FAIL'
        END AS validation_status
    FROM od_total o
    CROSS JOIN mart_total m
)
SELECT * FROM comparison;
