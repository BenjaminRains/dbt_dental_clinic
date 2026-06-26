-- KPI investigation: mart_daily_payments vs OD Daily Payments (2025-10-07, Tuesday)
-- Run in DBeaver against PostgreSQL opendental_analytics (analytics_user).
--
-- Mart vs staging (post mart_daily_payments rebuild): PASS $21,747.30
-- OD golden: od_daily_payments_10072025_10072025.csv — PASS $21,747.30 (exact match)

-- =============================================================================
-- Query 1: Mart breakdown
-- =============================================================================
SELECT *
FROM marts.mart_daily_payments
WHERE payment_date = DATE '2025-10-07';

-- =============================================================================
-- Query 2: Staging reconstruction (OD recipe)
-- =============================================================================
WITH patient AS (
    SELECT sum(payment_amount)::numeric(18, 2) AS total
    FROM staging.stg_opendental__payment
    WHERE payment_date = DATE '2025-10-07'
      AND payment_amount <> 0
),
insurance AS (
    SELECT sum(check_amount)::numeric(18, 2) AS total
    FROM staging.stg_opendental__claimpayment
    WHERE check_date = DATE '2025-10-07'
)
SELECT
    p.total AS patient_staging,
    i.total AS insurance_staging,
    p.total + i.total AS reconstructed_net
FROM patient p
CROSS JOIN insurance i;

-- =============================================================================
-- Query 3: PayType breakdown (compare to OD section subtotals)
-- =============================================================================
WITH day_payments AS (
    SELECT
        payment_type_id,
        count(*) AS payment_count,
        sum(payment_amount)::numeric(18, 2) AS header_total
    FROM staging.stg_opendental__payment
    WHERE payment_date = DATE '2025-10-07'
      AND payment_amount <> 0
    GROUP BY payment_type_id
),
day_claim AS (
    SELECT
        payment_type_id,
        count(*) AS claim_payment_count,
        sum(check_amount)::numeric(18, 2) AS check_total
    FROM staging.stg_opendental__claimpayment
    WHERE check_date = DATE '2025-10-07'
    GROUP BY payment_type_id
),
active_types AS (
    SELECT payment_type_id FROM day_payments
    UNION
    SELECT payment_type_id FROM day_claim
)
SELECT
    at.payment_type_id,
    d.item_name,
    coalesce(dp.payment_count, 0) AS patient_payment_count,
    coalesce(dp.header_total, 0)::numeric(18, 2) AS patient_header_total,
    coalesce(dc.claim_payment_count, 0) AS claim_payment_count,
    coalesce(dc.check_total, 0)::numeric(18, 2) AS claim_check_total
FROM active_types at
LEFT JOIN staging.stg_opendental__definition d
    ON d.definition_id = at.payment_type_id
LEFT JOIN day_payments dp
    ON dp.payment_type_id = at.payment_type_id
LEFT JOIN day_claim dc
    ON dc.payment_type_id = at.payment_type_id
ORDER BY coalesce(dp.header_total, 0) + coalesce(dc.check_total, 0) DESC;

-- =============================================================================
-- Query 4: OD golden section totals vs warehouse (od_daily_payments_10072025_10072025.csv)
-- Income Transfer nets to $0.00 in OD — excluded from net_collections_amount.
-- =============================================================================
WITH od_sections AS (
    SELECT * FROM (VALUES
        ('Check (insurance)', 13169.48::numeric(18, 2)),
        ('Check (patient)',      265.05),
        ('Cash',                 180.00),
        ('Credit Card',         7843.37),
        ('Care Credit',          289.40)
    ) AS t(section_name, od_amount)
),
warehouse AS (
    SELECT
        'Check (insurance)' AS section_name,
        coalesce(sum(check_amount) filter (where payment_type_id = 261), 0)::numeric(18, 2) AS wh_amount
    FROM staging.stg_opendental__claimpayment
    WHERE check_date = DATE '2025-10-07'
    UNION ALL
    SELECT
        CASE payment_type_id
            WHEN 69 THEN 'Check (patient)'
            WHEN 70 THEN 'Cash'
            WHEN 71 THEN 'Credit Card'
            WHEN 391 THEN 'Care Credit'
        END,
        sum(payment_amount)::numeric(18, 2)
    FROM staging.stg_opendental__payment
    WHERE payment_date = DATE '2025-10-07'
      AND payment_amount <> 0
      AND payment_type_id IN (69, 70, 71, 391)
    GROUP BY 1
)
SELECT
    o.section_name,
    o.od_amount,
    w.wh_amount AS warehouse_amount,
    w.wh_amount - o.od_amount AS difference,
    CASE WHEN w.wh_amount = o.od_amount THEN 'PASS' ELSE 'FAIL' END AS status
FROM od_sections o
JOIN warehouse w ON w.section_name = o.section_name
ORDER BY o.od_amount DESC;

-- =============================================================================
-- Query 5: Net total — OD vs mart
-- =============================================================================
SELECT
    21747.30::numeric(18, 2) AS od_net_collections,
    m.net_collections_amount AS mart_net_collections,
    m.net_collections_amount - 21747.30 AS difference,
    CASE
        WHEN m.net_collections_amount = 21747.30 THEN 'PASS'
        WHEN ABS(m.net_collections_amount - 21747.30) <= 10.00 THEN 'PASS'
        ELSE 'FAIL'
    END AS validation_status
FROM marts.mart_daily_payments m
WHERE m.payment_date = DATE '2025-10-07';
