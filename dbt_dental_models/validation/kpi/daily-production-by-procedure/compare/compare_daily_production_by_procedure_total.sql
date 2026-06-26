-- KPI validation: daily production total vs OpenDental Production by Procedure export
-- Run in DBeaver against PostgreSQL opendental_analytics (analytics_user).
--
-- Golden: golden/od_daily_production_by_procedure_06102026_06102026.csv
-- OD total (2026-06-10): $15,239.00 — from parser snapshot
--
-- OD logic: DateComplete + ProcStatus = 2 (Complete only)

WITH params AS (
    SELECT DATE '2026-06-10' AS production_date
),
od_total AS (
    SELECT 15239.00::numeric(18, 2) AS total_fees
),
mart_fact AS (
    SELECT round(sum(fp.actual_fee)::numeric, 2) AS total_fees
    FROM marts.fact_procedure fp
    CROSS JOIN params p
    WHERE fp.date_id = p.production_date
),
staging AS (
    SELECT round(sum(pl.procedure_fee)::numeric, 2) AS total_fees
    FROM staging.stg_opendental__procedurelog pl
    CROSS JOIN params p
    WHERE pl.date_complete::date = p.production_date
      AND pl.procedure_status = 2
)
SELECT
    p.production_date,
    o.total_fees AS od_total_fees,
    m.total_fees AS mart_fact_total_fees,
    s.total_fees AS staging_total_fees,
    m.total_fees - o.total_fees AS mart_vs_od_diff,
    s.total_fees - o.total_fees AS staging_vs_od_diff,
    CASE
        WHEN s.total_fees IS NULL OR s.total_fees = 0 AND o.total_fees > 0
            THEN 'PENDING — check date_complete / procedure_status filter'
        WHEN abs(s.total_fees - o.total_fees) <= 10.00 THEN 'PASS'
        WHEN abs(s.total_fees - o.total_fees) / nullif(abs(o.total_fees), 0) < 0.005 THEN 'PASS'
        WHEN abs(s.total_fees - o.total_fees) / nullif(abs(o.total_fees), 0) < 0.01 THEN 'WARN'
        ELSE 'FAIL'
    END AS staging_validation_status,
    CASE
        WHEN m.total_fees IS NULL OR m.total_fees = 0
            THEN 'MART_MISMATCH — mart uses ProcDate + status 2,4'
        WHEN abs(m.total_fees - o.total_fees) <= 10.00 THEN 'PASS'
        WHEN abs(m.total_fees - o.total_fees) / nullif(abs(o.total_fees), 0) < 0.005 THEN 'PASS'
        WHEN abs(m.total_fees - o.total_fees) / nullif(abs(o.total_fees), 0) < 0.01 THEN 'WARN'
        ELSE 'FAIL'
    END AS mart_validation_status
FROM params p
CROSS JOIN od_total o
CROSS JOIN mart_fact m
CROSS JOIN staging s;
