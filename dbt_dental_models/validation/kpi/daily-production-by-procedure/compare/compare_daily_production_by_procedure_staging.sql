-- KPI validation: mart_daily_production_by_procedure vs staging (DateComplete + status 2)
-- PASS when mart_vs_staging_diff = 0.

WITH params AS (
    SELECT DATE '2026-06-10' AS production_date
),
staging AS (
    SELECT round(sum(pl.procedure_fee)::numeric, 2) AS total_fees
    FROM staging.stg_opendental__procedurelog pl
    CROSS JOIN params p
    WHERE pl.date_complete::date = p.production_date
      AND pl.procedure_status = 2
),
mart AS (
    SELECT round(sum(m.total_fees)::numeric, 2) AS total_fees
    FROM marts.mart_daily_production_by_procedure m
    CROSS JOIN params p
    WHERE m.production_date = p.production_date
)
SELECT
    p.production_date,
    s.total_fees AS staging_total_fees,
    m.total_fees AS mart_total_fees,
    m.total_fees - s.total_fees AS mart_vs_staging_diff,
    CASE
        WHEN abs(m.total_fees - s.total_fees) < 0.01 THEN 'PASS'
        ELSE 'FAIL'
    END AS validation_status
FROM params p
CROSS JOIN staging s
CROSS JOIN mart m;
