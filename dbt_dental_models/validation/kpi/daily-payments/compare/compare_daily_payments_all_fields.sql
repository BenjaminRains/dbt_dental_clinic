-- Daily Payments: validate ALL mart_daily_payments columns + OD section totals
-- Run in DBeaver on clinic warehouse (opendental_analytics). Golden CSV retains PHI locally.
--
-- 1. Set payment_date below.
-- 2. Load od_sections from golden/snapshots/daily_payments_{date}.snapshot.yml
--    (from daily-payments/: python scripts/parse_od_daily_payments_golden.py golden/od_daily_payments_....csv -o golden/snapshots/...)
-- 3. Run each result block; every row should show PASS.

-- =============================================================================
-- CONFIG — 2025-10-07 example (swap date + od_sections for other golden days)
-- =============================================================================
WITH params AS (
    SELECT DATE '2025-10-07' AS payment_date
),
od_sections AS (
    SELECT * FROM (VALUES
        ('Check (insurance)', 13169.48::numeric(18, 2), 52, 'carrier_split'),
        ('Income Transfer',           0.00, 12, 'provider_split'),
        ('Check (patient)',         265.05,  2, 'provider_split'),
        ('Cash',                    180.00,  3, 'provider_split'),
        ('Credit Card',            7843.37, 52, 'provider_split'),
        ('Care Credit',             289.40,  2, 'provider_split')
    ) AS t(od_section, od_amount, od_detail_rows, grain)
),
od_totals AS (
    SELECT
        round(sum(od_amount) FILTER (WHERE od_section <> 'Income Transfer'), 2) AS net_collections,
        round(sum(od_amount) FILTER (WHERE grain = 'provider_split' AND od_section <> 'Income Transfer'), 2)
            AS patient_sections_amount,
        round(sum(od_amount) FILTER (WHERE grain = 'carrier_split'), 2) AS insurance_sections_amount,
        max(od_amount) FILTER (WHERE od_section = 'Income Transfer') AS income_transfer_amount
    FROM od_sections
),
mart AS (
    SELECT m.*
    FROM marts.mart_daily_payments m
    CROSS JOIN params p
    WHERE m.payment_date = p.payment_date
),
staging_patient AS (
    SELECT
        round(coalesce(sum(payment_amount) FILTER (WHERE payment_amount <> 0), 0), 2) AS patient_total,
        round(coalesce(sum(payment_amount) FILTER (WHERE payment_amount > 0), 0), 2) AS income_positive,
        round(coalesce(sum(payment_amount) FILTER (WHERE payment_amount < 0), 0), 2) AS refund_negative,
        count(*) FILTER (WHERE payment_amount <> 0) AS header_count,
        count(*) FILTER (
            WHERE payment_amount <> 0
              AND payment_type_id NOT IN (69, 70, 71, 391, 412, 417, 574, 634, 676)
        ) AS other_count,
        round(coalesce(sum(payment_amount) FILTER (
            WHERE payment_amount <> 0
              AND payment_type_id NOT IN (69, 70, 71, 391, 412, 417, 574, 634, 676)
        ), 0), 2) AS other_total
    FROM staging.stg_opendental__payment p
    CROSS JOIN params x
    WHERE p.payment_date = x.payment_date
),
staging_insurance AS (
    SELECT
        round(coalesce(sum(check_amount::numeric), 0), 2) AS insurance_total,
        round(coalesce(sum(check_amount::numeric) FILTER (WHERE check_amount > 0), 0), 2) AS income_positive,
        round(coalesce(sum(check_amount::numeric) FILTER (WHERE check_amount < 0), 0), 2) AS refund_negative,
        count(*) AS claim_count
    FROM staging.stg_opendental__claimpayment c
    CROSS JOIN params x
    WHERE c.check_date = x.payment_date
),
staging_paysplits AS (
    SELECT count(*) AS paysplit_detail_rows
    FROM staging.stg_opendental__paysplit ps
    INNER JOIN staging.stg_opendental__payment p ON ps.payment_id = p.payment_id
    CROSS JOIN params x
    WHERE p.payment_date = x.payment_date
      AND p.payment_amount <> 0
)

-- =============================================================================
-- Block A: Mart internal consistency
-- =============================================================================
SELECT
    'A_mart_internal' AS block,
    field_name,
    mart_value,
    expected_value,
    mart_value - expected_value AS difference,
    CASE WHEN abs(mart_value - expected_value) < 0.01 THEN 'PASS' ELSE 'FAIL' END AS status
FROM (
    SELECT 'total_payment_amount' AS field_name,
        m.total_payment_amount AS mart_value,
        m.patient_payment_amount + m.insurance_payment_amount AS expected_value
    FROM mart m
    UNION ALL
    SELECT 'net_collections_amount', m.net_collections_amount,
        m.patient_payment_amount + m.insurance_payment_amount FROM mart m
    UNION ALL
    SELECT 'payment_count', m.payment_count::numeric,
        (m.patient_payment_count + m.insurance_payment_count)::numeric FROM mart m
) checks;

-- =============================================================================
-- Block B: Mart columns vs staging (warehouse fidelity)
-- =============================================================================
SELECT
    'B_mart_vs_staging' AS block,
    field_name,
    mart_value,
    staging_value,
    mart_value - staging_value AS difference,
    CASE WHEN abs(mart_value - staging_value) < 0.01 THEN 'PASS' ELSE 'FAIL' END AS status
FROM (
    SELECT 'patient_payment_amount' AS field_name,
        m.patient_payment_amount AS mart_value, sp.patient_total AS staging_value
    FROM mart m CROSS JOIN staging_patient sp
    UNION ALL
    SELECT 'insurance_payment_amount', m.insurance_payment_amount, si.insurance_total
    FROM mart m CROSS JOIN staging_insurance si
    UNION ALL
    SELECT 'other_payment_amount', m.other_payment_amount, sp.other_total
    FROM mart m CROSS JOIN staging_patient sp
    UNION ALL
    SELECT 'income_amount', m.income_amount,
        sp.income_positive + si.income_positive
    FROM mart m CROSS JOIN staging_patient sp CROSS JOIN staging_insurance si
    UNION ALL
    SELECT 'refund_amount', m.refund_amount,
        sp.refund_negative + si.refund_negative
    FROM mart m CROSS JOIN staging_patient sp CROSS JOIN staging_insurance si
    UNION ALL
    SELECT 'patient_payment_count', m.patient_payment_count::numeric, sp.header_count::numeric
    FROM mart m CROSS JOIN staging_patient sp
    UNION ALL
    SELECT 'insurance_payment_count', m.insurance_payment_count::numeric, si.claim_count::numeric
    FROM mart m CROSS JOIN staging_insurance si
    UNION ALL
    SELECT 'other_payment_count', m.other_payment_count::numeric, sp.other_count::numeric
    FROM mart m CROSS JOIN staging_patient sp
) checks;

-- =============================================================================
-- Block C: Mart vs OD golden totals (from snapshot / parser)
-- =============================================================================
SELECT
    'C_mart_vs_od' AS block,
    field_name,
    mart_value,
    od_value,
    mart_value - od_value AS difference,
    CASE
        WHEN abs(mart_value - od_value) <= 10.00 THEN 'PASS'
        WHEN abs(mart_value - od_value) / nullif(abs(od_value), 0) < 0.005 THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM (
    SELECT 'net_collections_amount' AS field_name,
        m.net_collections_amount AS mart_value, o.net_collections AS od_value
    FROM mart m CROSS JOIN od_totals o
    UNION ALL
    SELECT 'patient_payment_amount', m.patient_payment_amount, o.patient_sections_amount
    FROM mart m CROSS JOIN od_totals o
    UNION ALL
    SELECT 'insurance_payment_amount', m.insurance_payment_amount, o.insurance_sections_amount
    FROM mart m CROSS JOIN od_totals o
    UNION ALL
    SELECT 'income_transfer_section', 0::numeric, o.income_transfer_amount
    FROM mart m CROSS JOIN od_totals o
) checks;

-- =============================================================================
-- Block D: OD section subtotals vs staging PayTypes (amounts only)
-- =============================================================================
WITH warehouse_sections AS (
    SELECT
        d.item_name || CASE WHEN dc.claim_n > 0 THEN ' (insurance)' ELSE '' END AS section_key,
        coalesce(dp.header_total, 0) + coalesce(dc.check_total, 0) AS wh_amount,
        coalesce(dp.n, 0) + coalesce(dc.n, 0) AS wh_header_or_claim_rows,
        coalesce(ps.split_rows, 0) AS wh_paysplit_rows
    FROM (
        SELECT DISTINCT payment_type_id FROM staging.stg_opendental__payment p
        CROSS JOIN params x WHERE p.payment_date = x.payment_date AND p.payment_amount <> 0
        UNION
        SELECT DISTINCT payment_type_id FROM staging.stg_opendental__claimpayment c
        CROSS JOIN params x WHERE c.check_date = x.payment_date
    ) types
    LEFT JOIN staging.stg_opendental__definition d ON d.definition_id = types.payment_type_id
    LEFT JOIN LATERAL (
        SELECT count(*) AS n, sum(payment_amount)::numeric(18,2) AS header_total
        FROM staging.stg_opendental__payment p
        CROSS JOIN params x
        WHERE p.payment_date = x.payment_date AND p.payment_type_id = types.payment_type_id
          AND p.payment_amount <> 0
    ) dp ON true
    LEFT JOIN LATERAL (
        SELECT count(*) AS n, sum(check_amount)::numeric(18,2) AS check_total
        FROM staging.stg_opendental__claimpayment c
        CROSS JOIN params x
        WHERE c.check_date = x.payment_date AND c.payment_type_id = types.payment_type_id
    ) dc ON true
    LEFT JOIN LATERAL (
        SELECT count(*) AS split_rows
        FROM staging.stg_opendental__paysplit ps
        INNER JOIN staging.stg_opendental__payment p ON ps.payment_id = p.payment_id
        CROSS JOIN params x
        WHERE p.payment_date = x.payment_date AND p.payment_type_id = types.payment_type_id
          AND p.payment_amount <> 0
    ) ps ON true
    WHERE coalesce(dp.header_total, 0) + coalesce(dc.check_total, 0) <> 0
)
SELECT
    'D_od_section_amounts' AS block,
    o.od_section,
    o.od_amount,
    w.wh_amount AS warehouse_amount,
    w.wh_amount - o.od_amount AS difference,
    CASE WHEN abs(w.wh_amount - o.od_amount) < 0.01 THEN 'PASS' ELSE 'FAIL' END AS amount_status,
    o.od_detail_rows AS od_detail_rows,
    w.wh_paysplit_rows AS warehouse_paysplit_rows,
    w.wh_header_or_claim_rows AS warehouse_header_rows,
    'detail_row_count is OD provider-split grain; see ../FIELD_MAP.md' AS count_note
FROM od_sections o
LEFT JOIN warehouse_sections w ON (
    (o.grain = 'carrier_split' AND w.section_key LIKE '%' || split_part(o.od_section, ' ', 1) || '%')
    OR (o.grain = 'provider_split' AND w.section_key = o.od_section)
)
WHERE o.od_section NOT IN ('Income Transfer')
ORDER BY o.od_amount DESC;

-- =============================================================================
-- Block E: Count grain reference (informational — not FAIL if OD detail rows differ)
-- =============================================================================
SELECT
    'E_count_grains' AS block,
    metric,
    count_value,
    notes
FROM (
    SELECT 'mart_payment_count' AS metric, m.payment_count::bigint AS count_value,
        'patient headers + claim payments' AS notes FROM mart m
    UNION ALL
    SELECT 'mart_patient_payment_count', m.patient_payment_count::bigint,
        'non-zero payment headers' FROM mart m
    UNION ALL
    SELECT 'mart_insurance_payment_count', m.insurance_payment_count::bigint,
        'claimpayment rows' FROM mart m
    UNION ALL
    SELECT 'staging_paysplit_rows', s.paysplit_detail_rows,
        'closest to OD patient section detail row count' FROM staging_paysplits s
    UNION ALL
    SELECT 'od_patient_detail_rows',
        sum(od_detail_rows) FILTER (WHERE grain = 'provider_split' AND od_section <> 'Income Transfer')::bigint,
        'sum of OD patient section detail rows' FROM od_sections
    UNION ALL
    SELECT 'od_insurance_detail_rows',
        sum(od_detail_rows) FILTER (WHERE grain = 'carrier_split')::bigint,
        'sum of OD insurance section detail rows' FROM od_sections
) grains;
