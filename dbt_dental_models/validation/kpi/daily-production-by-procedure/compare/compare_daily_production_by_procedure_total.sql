-- KPI validation: daily production total vs OpenDental Production by Procedure export
-- Run against PostgreSQL opendental_analytics (analytics_user).
--
-- Subject: marts.mart_daily_production_by_procedure
-- OD logic: DateComplete + ProcStatus = 2 (Complete only)
--
-- Spot-check: change params + od_total for each golden date
--   2026-06-10 → 15239.00 | 2025-11-18 → 36589.00 | 2026-02-07 → 22344.00

WITH params AS (
    SELECT DATE '2026-06-10' AS production_date
),
od_total AS (
    SELECT 15239.00::numeric(18, 2) AS total_fees
),
mart_kpi AS (
    SELECT
        round(sum(m.total_fees)::numeric, 2) AS total_fees,
        sum(m.procedure_quantity) AS procedure_quantity,
        count(*) AS procedure_code_count
    FROM marts.mart_daily_production_by_procedure m
    CROSS JOIN params p
    WHERE m.production_date = p.production_date
),
staging AS (
    SELECT
        round(sum(pl.procedure_fee)::numeric, 2) AS total_fees,
        count(*) AS procedure_quantity
    FROM staging.stg_opendental__procedurelog pl
    CROSS JOIN params p
    WHERE pl.date_complete::date = p.production_date
      AND pl.procedure_status = 2
)
SELECT
    p.production_date,
    o.total_fees AS od_total_fees,
    k.total_fees AS mart_kpi_total_fees,
    k.procedure_quantity AS mart_kpi_qty,
    k.procedure_code_count AS mart_kpi_codes,
    s.total_fees AS staging_total_fees,
    s.procedure_quantity AS staging_qty,
    k.total_fees - o.total_fees AS mart_vs_od_diff,
    s.total_fees - o.total_fees AS staging_vs_od_diff,
    k.total_fees - s.total_fees AS mart_vs_staging_diff,
    CASE
        WHEN s.total_fees IS NULL OR (s.total_fees = 0 AND o.total_fees > 0)
            THEN 'PENDING — check date_complete / procedure_status filter'
        WHEN abs(s.total_fees - o.total_fees) <= 10.00 THEN 'PASS'
        WHEN abs(s.total_fees - o.total_fees) / nullif(abs(o.total_fees), 0) < 0.005 THEN 'PASS'
        WHEN abs(s.total_fees - o.total_fees) / nullif(abs(o.total_fees), 0) < 0.01 THEN 'WARN'
        ELSE 'FAIL'
    END AS staging_validation_status,
    CASE
        WHEN k.total_fees IS NULL OR (k.total_fees = 0 AND o.total_fees > 0)
            THEN 'FAIL — no mart rows'
        WHEN abs(k.total_fees - o.total_fees) <= 10.00 THEN 'PASS'
        WHEN abs(k.total_fees - o.total_fees) / nullif(abs(o.total_fees), 0) < 0.005 THEN 'PASS'
        WHEN abs(k.total_fees - o.total_fees) / nullif(abs(o.total_fees), 0) < 0.01 THEN 'WARN'
        ELSE 'FAIL'
    END AS mart_validation_status
FROM params p
CROSS JOIN od_total o
CROSS JOIN mart_kpi k
CROSS JOIN staging s;
