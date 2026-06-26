-- KPI validation: mart fact_procedure vs staging procedurelog (production by date)
-- Run before OD golden compare. PASS when mart_vs_staging_diff = 0.

WITH params AS (
    SELECT DATE '2025-06-10' AS production_date
),
staging AS (
    SELECT round(sum(pl.procedure_fee)::numeric, 2) AS total_fees
    FROM staging.stg_opendental__procedurelog pl
    CROSS JOIN params p
    WHERE pl.procedure_date::date = p.production_date
      AND pl.procedure_status IN (2, 4)
),
mart AS (
    SELECT round(sum(fp.actual_fee)::numeric, 2) AS total_fees
    FROM marts.fact_procedure fp
    INNER JOIN marts.dim_date d ON fp.date_id = d.date_id
    CROSS JOIN params p
    WHERE d.date_day = p.production_date
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
