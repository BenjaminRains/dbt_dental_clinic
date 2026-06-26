-- KPI validation: production by procedure code vs OD golden snapshot (2026-06-10)
--
-- OD Daily → Production by Procedure aligns with the Daily Procedures report (grouped by code):
--   - Date = DateComplete (date procedure was set complete), NOT ProcDate
--   - Status = Complete only (ProcStatus = 2)
--   - Fee = ProcFee (procedure_fee in staging)
--
-- Primary compare uses STAGING. Mart fact_procedure uses procedure_date + status (2,4) — expect
-- mart_status to lag until the mart date/status logic matches OD.
--
-- Golden: golden/snapshots/od_daily_production_by_procedure_06102026_06102026.snapshot.yml

WITH params AS (
    SELECT DATE '2026-06-10' AS production_date
),
od_rows AS (
    SELECT * FROM (VALUES
        ('D0120', 25, 1548.00::numeric(18, 2)),
        ('D0140', 2, 184.00),
        ('D0150', 1, 120.00),
        ('D0171', 2, 0.00),
        ('D0210', 2, 400.00),
        ('D0220', 9, 315.00),
        ('D0230', 1, 30.00),
        ('D0272', 3, 177.00),
        ('D0274', 8, 629.00),
        ('D0330', 4, 556.00),
        ('D0350', 5, 0.00),
        ('D0801', 2, 0.00),
        ('D1110', 15, 1740.00),
        ('D1120', 6, 522.00),
        ('D1206', 21, 672.00),
        ('D4346', 3, 606.00),
        ('D4267', 1, 725.00),
        ('D4910', 5, 810.00),
        ('D5214', 1, 2100.00),
        ('D2799', 1, 0.00),
        ('D7140', 6, 2100.00),
        ('D7210', 4, 1400.00),
        ('D7953', 1, 350.00),
        ('09902', 1, 7.00),
        ('D2919', 1, 0.00),
        ('D9230', 1, 98.00),
        ('D9986', 2, 50.00),
        ('D9987', 7, 100.00)
    ) AS t(procedure_code, od_quantity, od_total_fees)
),
staging AS (
    SELECT
        trim(pc.procedure_code) AS procedure_code,
        count(*) AS staging_quantity,
        round(sum(pl.procedure_fee)::numeric, 2) AS staging_total_fees
    FROM staging.stg_opendental__procedurelog pl
    INNER JOIN staging.stg_opendental__procedurecode pc
        ON pl.procedure_code_id = pc.procedure_code_id
    CROSS JOIN params p
    WHERE pl.date_complete::date = p.production_date
      AND pl.procedure_status = 2
    GROUP BY 1
),
mart_fact AS (
    SELECT
        trim(pc.procedure_code) AS procedure_code,
        count(*) AS mart_quantity,
        round(sum(fp.actual_fee)::numeric, 2) AS mart_total_fees
    FROM marts.fact_procedure fp
    INNER JOIN marts.dim_procedure pc
        ON fp.procedure_type_id = pc.procedure_code_id
    CROSS JOIN params p
    WHERE fp.date_id = p.production_date
    GROUP BY 1
)
SELECT
    o.procedure_code,
    o.od_quantity,
    o.od_total_fees,
    s.staging_quantity,
    s.staging_total_fees,
    m.mart_quantity,
    m.mart_total_fees,
    coalesce(s.staging_total_fees, 0) - o.od_total_fees AS staging_fee_diff,
    coalesce(m.mart_total_fees, 0) - o.od_total_fees AS mart_fee_diff,
    CASE
        WHEN s.procedure_code IS NULL THEN 'FAIL — missing in staging'
        WHEN abs(coalesce(s.staging_total_fees, 0) - o.od_total_fees) < 0.01
         AND s.staging_quantity = o.od_quantity THEN 'PASS'
        WHEN abs(coalesce(s.staging_total_fees, 0) - o.od_total_fees) < 0.01 THEN 'PASS_FEES'
        ELSE 'FAIL'
    END AS staging_status,
    CASE
        WHEN m.procedure_code IS NULL THEN 'MART_MISMATCH — mart uses ProcDate + status 2,4'
        WHEN abs(coalesce(m.mart_total_fees, 0) - o.od_total_fees) < 0.01
         AND m.mart_quantity = o.od_quantity THEN 'PASS'
        WHEN abs(coalesce(m.mart_total_fees, 0) - o.od_total_fees) < 0.01 THEN 'PASS_FEES'
        ELSE 'FAIL'
    END AS mart_status
FROM od_rows o
LEFT JOIN staging s ON s.procedure_code = o.procedure_code
LEFT JOIN mart_fact m ON m.procedure_code = o.procedure_code
ORDER BY abs(coalesce(s.staging_total_fees, 0) - o.od_total_fees) DESC NULLS FIRST;

-- =============================================================================
-- Diagnostic: ProcDate vs DateComplete (run if staging still fails)
-- =============================================================================
-- WITH p AS (SELECT DATE '2026-06-10' AS production_date)
-- SELECT 'procdate_status_2_4' AS layer, count(*) AS rows, round(sum(pl.procedure_fee)::numeric, 2) AS total_fees
-- FROM staging.stg_opendental__procedurelog pl CROSS JOIN p
-- WHERE pl.procedure_date::date = p.production_date AND pl.procedure_status IN (2, 4)
-- UNION ALL
-- SELECT 'datecomplete_status_2', count(*), round(sum(pl.procedure_fee)::numeric, 2)
-- FROM staging.stg_opendental__procedurelog pl CROSS JOIN p
-- WHERE pl.date_complete::date = p.production_date AND pl.procedure_status = 2;
