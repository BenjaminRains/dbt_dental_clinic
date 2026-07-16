-- KPI validation layer 4: API / frontend path vs mart vs OD golden
-- Mirrors get_daily_production_kpi() day rollup in api/services/kpi_service.py
--
-- PASS when api day totals = mart rollup = OD golden on each spot-check date.

WITH golden_dates AS (
    SELECT * FROM (VALUES
        (DATE '2026-06-10', 15239.00::numeric(18, 2), 140, 28),
        (DATE '2025-11-18', 36589.00::numeric(18, 2), 202, 48),
        (DATE '2026-02-07', 22344.00::numeric(18, 2), 79, 25)
    ) AS t(production_date, od_total_fees, od_quantity, od_code_count)
),
-- Mirrors day-summary SELECT in get_daily_production_kpi()
api_equivalent AS (
    SELECT
        m.production_date,
        round(sum(m.total_fees)::numeric, 2) AS total_fees,
        sum(m.procedure_quantity)::int AS procedure_quantity,
        count(*)::int AS procedure_code_count,
        true AS has_data
    FROM marts.mart_daily_production_by_procedure m
    INNER JOIN golden_dates g ON m.production_date = g.production_date
    GROUP BY m.production_date
)
SELECT
    g.production_date,
    g.od_total_fees,
    g.od_quantity,
    g.od_code_count,
    a.total_fees AS api_total_fees,
    a.procedure_quantity AS api_quantity,
    a.procedure_code_count AS api_code_count,
    a.total_fees - g.od_total_fees AS api_vs_od_fee_diff,
    a.procedure_quantity - g.od_quantity AS api_vs_od_qty_diff,
    CASE
        WHEN a.production_date IS NULL THEN 'FAIL — no mart rows (API would return has_data=false)'
        WHEN abs(a.total_fees - g.od_total_fees) <= 10.00
         AND a.procedure_quantity = g.od_quantity
         AND a.procedure_code_count = g.od_code_count THEN 'PASS'
        WHEN abs(a.total_fees - g.od_total_fees) <= 10.00
          OR abs(a.total_fees - g.od_total_fees) / nullif(abs(g.od_total_fees), 0) < 0.005 THEN 'PASS_FEES'
        ELSE 'FAIL'
    END AS validation_status,
    'Frontend shows api_total_fees via GET /kpi/daily-production' AS frontend_field
FROM golden_dates g
LEFT JOIN api_equivalent a ON g.production_date = a.production_date
ORDER BY g.production_date;

-- Latest-date endpoint: MAX(production_date) with activity
SELECT
    'latest_production_date' AS check_name,
    max(production_date) AS api_latest_production_date
FROM marts.mart_daily_production_by_procedure
WHERE procedure_quantity > 0;
