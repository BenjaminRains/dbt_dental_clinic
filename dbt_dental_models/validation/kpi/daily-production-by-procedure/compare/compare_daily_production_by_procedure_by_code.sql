-- KPI validation: production by procedure code vs OD golden snapshot
-- Subject: marts.mart_daily_production_by_procedure
--
-- OD Daily → Production by Procedure: DateComplete + ProcStatus = 2
-- Update od_rows VALUES from golden/snapshots/*.snapshot.yml for each date.
-- Default below: 2026-06-10

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
mart_kpi AS (
    SELECT
        trim(m.procedure_code) AS procedure_code,
        m.procedure_quantity AS mart_quantity,
        m.total_fees AS mart_total_fees
    FROM marts.mart_daily_production_by_procedure m
    CROSS JOIN params p
    WHERE m.production_date = p.production_date
),
staging AS (
    SELECT
        trim(pc.procedure_code) AS procedure_code,
        count(*) AS staging_quantity,
        round(sum(pl.procedure_fee)::numeric, 2) AS staging_total_fees
    FROM staging.stg_opendental__procedurelog pl
    INNER JOIN staging.stg_opendental__procedurecode pc
        on pl.procedure_code_id = pc.procedure_code_id
    CROSS JOIN params p
    WHERE pl.date_complete::date = p.production_date
      AND pl.procedure_status = 2
    GROUP BY 1
)
SELECT
    o.procedure_code,
    o.od_quantity,
    o.od_total_fees,
    s.staging_quantity,
    s.staging_total_fees,
    k.mart_quantity,
    k.mart_total_fees,
    coalesce(s.staging_total_fees, 0) - o.od_total_fees AS staging_fee_diff,
    coalesce(k.mart_total_fees, 0) - o.od_total_fees AS mart_fee_diff,
    CASE
        WHEN s.procedure_code IS NULL THEN 'FAIL — missing in staging'
        WHEN abs(coalesce(s.staging_total_fees, 0) - o.od_total_fees) < 0.01
         AND s.staging_quantity = o.od_quantity THEN 'PASS'
        WHEN abs(coalesce(s.staging_total_fees, 0) - o.od_total_fees) < 0.01 THEN 'PASS_FEES'
        ELSE 'FAIL'
    END AS staging_status,
    CASE
        WHEN k.procedure_code IS NULL THEN 'FAIL — missing in mart'
        WHEN abs(coalesce(k.mart_total_fees, 0) - o.od_total_fees) < 0.01
         AND k.mart_quantity = o.od_quantity THEN 'PASS'
        WHEN abs(coalesce(k.mart_total_fees, 0) - o.od_total_fees) < 0.01 THEN 'PASS_FEES'
        ELSE 'FAIL'
    END AS mart_status
FROM od_rows o
LEFT JOIN staging s ON s.procedure_code = o.procedure_code
LEFT JOIN mart_kpi k ON k.procedure_code = o.procedure_code
ORDER BY abs(coalesce(k.mart_total_fees, 0) - o.od_total_fees) DESC NULLS FIRST;
