-- KPI validation: API-equivalent SQL for Aging of A/R (layer 4)
-- Mirrors api/services/ar_service.py get_ar_kpi_summary / get_ar_aging_summary
-- against marts.mart_ar_summary. Run in DBeaver (analytics_user).
--
-- Compare these totals to GET /ar/kpi-summary and GET /ar/aging-summary responses.

WITH latest AS (
    SELECT MAX(snapshot_date) AS snapshot_date
    FROM marts.mart_ar_summary
),
kpi AS (
    SELECT
        l.snapshot_date,
        SUM(m.total_balance)::numeric(18, 2) AS total_ar_outstanding,
        SUM(m.balance_0_30_days)::numeric(18, 2) AS current_amount,
        SUM(m.balance_over_90_days)::numeric(18, 2) AS over_90_amount,
        SUM(m.patient_responsibility)::numeric(18, 2) AS patient_ar,
        SUM(m.insurance_estimate)::numeric(18, 2) AS insurance_ar
    FROM marts.mart_ar_summary m
    CROSS JOIN latest l
    WHERE m.snapshot_date = l.snapshot_date
    GROUP BY l.snapshot_date
),
aging AS (
    SELECT * FROM (
        SELECT 'Current' AS aging_bucket, SUM(m.balance_0_30_days)::numeric(18, 2) AS amount
        FROM marts.mart_ar_summary m CROSS JOIN latest l
        WHERE m.snapshot_date = l.snapshot_date
        UNION ALL
        SELECT '30 Day', SUM(m.balance_31_60_days)::numeric(18, 2)
        FROM marts.mart_ar_summary m CROSS JOIN latest l
        WHERE m.snapshot_date = l.snapshot_date
        UNION ALL
        SELECT '60 Day', SUM(m.balance_61_90_days)::numeric(18, 2)
        FROM marts.mart_ar_summary m CROSS JOIN latest l
        WHERE m.snapshot_date = l.snapshot_date
        UNION ALL
        SELECT '90 Day', SUM(m.balance_over_90_days)::numeric(18, 2)
        FROM marts.mart_ar_summary m CROSS JOIN latest l
        WHERE m.snapshot_date = l.snapshot_date
    ) b
)
SELECT 'kpi-summary' AS endpoint, k.snapshot_date::text AS key,
       k.total_ar_outstanding AS amount, NULL::numeric AS pct
FROM kpi k
UNION ALL
SELECT 'kpi-summary', 'current_amount', k.current_amount, NULL
FROM kpi k
UNION ALL
SELECT 'kpi-summary', 'over_90_amount', k.over_90_amount, NULL
FROM kpi k
UNION ALL
SELECT 'aging-summary', a.aging_bucket, a.amount,
       ROUND(100.0 * a.amount / NULLIF((SELECT total_ar_outstanding FROM kpi), 0), 2)
FROM aging a
ORDER BY 1, 2;
