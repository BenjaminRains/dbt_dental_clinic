-- Production and Income — monthly golden vs warehouse
-- PostgreSQL, database opendental_analytics, user analytics_user.
--
-- Subject: raw reconstruction of Reports → Standard → Production and Income.
-- Rules: validation/kpi/production-and-income/FIELD_MAP.md
--
-- Golden months:
--   2025-01 .. 2025-12          closed year
--   2026-01 .. 2026-09          closed months
--   2026-10                     open; as of the report exported 2026-10-08
--
-- source_status PASS means the raw rules match the OpenDental grid (±$0.01).
-- Mart and staging diffs are diagnostic. They are known deltas until a model
-- implements FIELD_MAP.md. Do not "fix" them by changing these OD rules.

WITH od AS (
    SELECT *
    FROM (
        VALUES
            -- 2025
            (DATE '2025-01-01', 286865.00,  -9076.00,    0.00, 141448.39,  35003.92, 101206.51, 'closed'),
            (DATE '2025-02-01', 301943.62, -24877.59,    0.00, 175026.56,  19038.26,  92560.32, 'closed'),
            (DATE '2025-03-01', 250726.06, -93163.66,    0.00, 146802.65, -27877.98, 128361.49, 'closed'),
            (DATE '2025-04-01', 330528.00, -42145.38,    0.00, 209112.33, -29801.47, 102962.44, 'closed'),
            (DATE '2025-05-01', 302290.39, -38218.61,    0.00, 170586.48, -18534.55, 100410.11, 'closed'),
            (DATE '2025-06-01', 271002.00, -36481.98,    0.00, 122573.00,   6726.13,  86940.85, 'closed'),
            (DATE '2025-07-01', 266112.07, -54657.06, -169.00, 150385.31,  -5963.27, 124410.59, 'closed'),
            (DATE '2025-08-01', 255976.80, -20022.14,  169.00, 128800.86,  -8376.51,  84959.23, 'closed'),
            (DATE '2025-09-01', 314143.11, -28921.30,    0.00, 147514.16,   5546.21,  91506.56, 'closed'),
            (DATE '2025-10-01', 356454.00, -23718.05,    0.00, 177961.68,  -8327.40,  96276.37, 'closed'),
            (DATE '2025-11-01', 306825.00, -42493.70,    0.00, 152890.18,  12494.54,  75189.40, 'closed'),
            (DATE '2025-12-01', 388290.77, -25970.95,    0.00, 161077.32,  15218.93, 133927.03, 'closed'),
            -- 2026 closed months, then October as of 2026-10-08
            (DATE '2026-01-01', 360514.95, -34566.27,    0.00, 210215.11,   3249.45, 125581.59, 'closed'),
            (DATE '2026-02-01', 347366.00, -25962.30,    0.00, 181873.03,  32871.63, 128624.13, 'closed'),
            (DATE '2026-03-01', 352483.00, -38787.77,    0.00, 246735.10, -43364.93, 163644.74, 'closed'),
            (DATE '2026-04-01', 315030.00, -26990.00,    0.00, 182827.36,  28177.52, 101872.86, 'closed'),
            (DATE '2026-05-01', 295072.00, -30495.12,    0.00, 158875.41,  -5038.50, 116346.64, 'closed'),
            (DATE '2026-06-01', 287282.00, -42084.31,  -16.00, 176607.93, -17622.88, 120105.36, 'closed'),
            (DATE '2026-07-01', 272367.25, -27606.82,    0.00, 136671.24,   1529.29,  87981.34, 'closed'),
            (DATE '2026-08-01', 289770.00, -24217.61,    0.00, 157943.18, -14307.72, 103048.76, 'closed'),
            (DATE '2026-09-01', 292168.00, -26833.54,    0.00, 172070.24,  -2712.64,  92412.23, 'closed'),
            (DATE '2026-10-01',  97903.50,  -7099.00,    0.00,  46858.49,  46156.16,  25500.38, 'asof_2026-10-08')
    ) AS t(
        month_start,
        od_production,
        od_adjustments,
        od_writeoff,
        od_pt_income,
        od_unearned,
        od_ins_income,
        golden_stability
    )
),

production AS (
    SELECT date_trunc('month', "ProcDate")::date AS month_start,
           round(sum("ProcFee")::numeric, 2) AS production
    FROM raw.procedurelog
    WHERE "ProcStatus" = 2
      AND "ProcDate" >= DATE '2025-01-01'
      AND "ProcDate" < DATE '2026-11-01'
    GROUP BY 1
),

adjustments AS (
    SELECT date_trunc('month', "AdjDate")::date AS month_start,
           round(sum("AdjAmt")::numeric, 2) AS adjustments
    FROM raw.adjustment
    WHERE "AdjDate" >= DATE '2025-01-01'
      AND "AdjDate" < DATE '2026-11-01'
    GROUP BY 1
),

writeoff AS (
    SELECT date_trunc('month', "DateCP")::date AS month_start,
           round((-sum("WriteOff"))::numeric, 2) AS writeoff_display
    FROM raw.claimproc
    WHERE "DateCP" >= DATE '2025-01-01'
      AND "DateCP" < DATE '2026-11-01'
      AND "WriteOff" <> 0
    GROUP BY 1
),

paysplits AS (
    SELECT date_trunc('month', ps."DatePay")::date AS month_start,
           round(sum(ps."SplitAmt") FILTER (WHERE ps."UnearnedType" = 0)::numeric, 2) AS pt_income,
           round(sum(ps."SplitAmt") FILTER (
               WHERE ps."UnearnedType" <> 0
                 AND coalesce(def."ItemValue", '') <> 'X'
           )::numeric, 2) AS unearned,
           round(sum(ps."SplitAmt") FILTER (
               WHERE ps."UnearnedType" <> 0
                 AND def."ItemValue" = 'X'
           )::numeric, 2) AS excluded_unearned
    FROM raw.paysplit ps
    LEFT JOIN raw.definition def
        ON def."DefNum" = ps."UnearnedType"
    WHERE ps."DatePay" >= DATE '2025-01-01'
      AND ps."DatePay" < DATE '2026-11-01'
    GROUP BY 1
),

insurance AS (
    SELECT date_trunc('month', "DateCP")::date AS month_start,
           round(sum("InsPayAmt")::numeric, 2) AS ins_income
    FROM raw.claimproc
    WHERE "DateCP" >= DATE '2025-01-01'
      AND "DateCP" < DATE '2026-11-01'
      AND "Status" IN (1, 4)
    GROUP BY 1
),

mart_prod AS (
    SELECT date_trunc('month', production_date)::date AS month_start,
           round(sum(total_fees)::numeric, 2) AS mart_production
    FROM marts.mart_daily_production_by_procedure
    WHERE production_date >= DATE '2025-01-01'
      AND production_date < DATE '2026-11-01'
    GROUP BY 1
),

mart_pay AS (
    SELECT date_trunc('month', payment_date)::date AS month_start,
           round(sum(net_collections_amount)::numeric, 2) AS mart_total_income,
           round(sum(insurance_payment_amount)::numeric, 2) AS mart_ins_income
    FROM marts.mart_daily_payments
    WHERE payment_date >= DATE '2025-01-01'
      AND payment_date < DATE '2026-11-01'
    GROUP BY 1
),

recon AS (
    SELECT
        od.month_start,
        od.golden_stability,
        od.od_production,
        coalesce(production.production, 0) AS production,
        od.od_adjustments,
        coalesce(adjustments.adjustments, 0) AS adjustments,
        od.od_writeoff,
        coalesce(writeoff.writeoff_display, 0) AS writeoff_display,
        od.od_pt_income,
        coalesce(paysplits.pt_income, 0) AS pt_income,
        od.od_unearned,
        coalesce(paysplits.unearned, 0) AS unearned,
        od.od_ins_income,
        coalesce(insurance.ins_income, 0) AS ins_income,
        coalesce(paysplits.excluded_unearned, 0) AS excluded_unearned,
        coalesce(mart_prod.mart_production, 0) AS mart_production,
        coalesce(mart_pay.mart_total_income, 0) AS mart_total_income,
        coalesce(mart_pay.mart_ins_income, 0) AS mart_ins_income
    FROM od
    LEFT JOIN production USING (month_start)
    LEFT JOIN adjustments USING (month_start)
    LEFT JOIN writeoff USING (month_start)
    LEFT JOIN paysplits USING (month_start)
    LEFT JOIN insurance USING (month_start)
    LEFT JOIN mart_prod USING (month_start)
    LEFT JOIN mart_pay USING (month_start)
)

SELECT
    to_char(month_start, 'YYYY-MM') AS month,
    golden_stability,

    production - od_production AS production_diff,
    adjustments - od_adjustments AS adjustment_diff,
    writeoff_display - od_writeoff AS writeoff_diff,
    pt_income - od_pt_income AS pt_income_diff,
    unearned - od_unearned AS unearned_diff,
    ins_income - od_ins_income AS ins_income_diff,

    (production + adjustments + writeoff_display)
        - (od_production + od_adjustments + od_writeoff) AS tot_prod_diff,
    (pt_income + unearned + ins_income)
        - (od_pt_income + od_unearned + od_ins_income) AS total_income_diff,

    excluded_unearned,
    mart_production - od_production AS mart_production_diff,
    mart_total_income - (od_pt_income + od_unearned + od_ins_income) AS mart_total_income_diff,
    mart_ins_income - od_ins_income AS mart_ins_diff,

    CASE
        WHEN abs(production - od_production) <= 0.01
         AND abs(adjustments - od_adjustments) <= 0.01
         AND abs(writeoff_display - od_writeoff) <= 0.01
         AND abs(pt_income - od_pt_income) <= 0.01
         AND abs(unearned - od_unearned) <= 0.01
         AND abs(ins_income - od_ins_income) <= 0.01
        THEN 'PASS'
        ELSE 'FAIL'
    END AS source_status
FROM recon
ORDER BY month_start;
