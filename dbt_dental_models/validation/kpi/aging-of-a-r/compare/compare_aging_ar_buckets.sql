-- KPI validation: aging buckets vs OpenDental Aging of A/R (secondary)
-- Run in DBeaver against PostgreSQL opendental_analytics (analytics_user).
--
-- Fill od_buckets from the OD report column totals once FIELD_MAP names them.
-- Bucket drift alone does not block practice-total sign-off.

WITH params AS (
    SELECT CURRENT_DATE AS snapshot_date
),
od_buckets AS (
    -- 2026-10-12 stored guarantor buckets for the same population as the total
    -- (PatStatus 0 and 2, BalTotal > 0). Secondary. These are not a printed report.
    SELECT
        93531.82::numeric(18, 2) AS bal_0_30,
        38326.36::numeric(18, 2) AS bal_31_60,
        28388.60::numeric(18, 2) AS bal_61_90,
        122377.45::numeric(18, 2) AS bal_over_90
),
mart_buckets AS (
    SELECT
        MAX(m.snapshot_date) AS snapshot_date,
        SUM(m.balance_0_30_days)::numeric(18, 2) AS bal_0_30,
        SUM(m.balance_31_60_days)::numeric(18, 2) AS bal_31_60,
        SUM(m.balance_61_90_days)::numeric(18, 2) AS bal_61_90,
        SUM(m.balance_over_90_days)::numeric(18, 2) AS bal_over_90,
        SUM(m.total_balance)::numeric(18, 2) AS total_ar
    FROM marts.mart_ar_summary m
    CROSS JOIN params p
    WHERE m.snapshot_date = p.snapshot_date
),
unpivoted AS (
    SELECT '0-30 / Current' AS bucket, m.bal_0_30 AS mart_amount, o.bal_0_30 AS od_amount
    FROM mart_buckets m CROSS JOIN od_buckets o
    UNION ALL
    SELECT '31-60', m.bal_31_60, o.bal_31_60
    FROM mart_buckets m CROSS JOIN od_buckets o
    UNION ALL
    SELECT '61-90', m.bal_61_90, o.bal_61_90
    FROM mart_buckets m CROSS JOIN od_buckets o
    UNION ALL
    SELECT '90+', m.bal_over_90, o.bal_over_90
    FROM mart_buckets m CROSS JOIN od_buckets o
)
SELECT
    u.bucket,
    u.od_amount,
    u.mart_amount,
    u.mart_amount - u.od_amount AS difference,
    CASE
        WHEN u.od_amount IS NULL THEN 'PENDING — set od_buckets from golden export'
        WHEN u.od_amount = 0 AND u.mart_amount = 0 THEN 'PASS'
        WHEN ABS(u.mart_amount - u.od_amount) <= 50.00 THEN 'PASS'
        WHEN ABS(u.mart_amount - u.od_amount) / NULLIF(ABS(u.od_amount), 0) < 0.005 THEN 'PASS'
        WHEN ABS(u.mart_amount - u.od_amount) / NULLIF(ABS(u.od_amount), 0) < 0.01 THEN 'WARN'
        ELSE 'FAIL'
    END AS validation_status
FROM unpivoted u
ORDER BY
    CASE u.bucket
        WHEN '0-30 / Current' THEN 1
        WHEN '31-60' THEN 2
        WHEN '61-90' THEN 3
        ELSE 4
    END;
