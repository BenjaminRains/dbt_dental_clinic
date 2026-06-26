-- KPI validation: daily net collections vs OpenDental Daily Payments export
-- Run in DBeaver against PostgreSQL opendental_analytics (analytics_user).
--
-- Prerequisites:
--   1. Export OD report CSV to validation/kpi/golden/ (see golden_manifest.example.yml)
--   2. Enter OD total in the od_total CTE below (from export total row or sum)
--   3. Match date and filters documented in KPI_VALIDATION_REGISTRY.md
--      Golden: golden/daily_payments_06242026_ins_pat_split.csv (2026-06-24)
--      Mart logic: patient stg_opendental__payment + insurance stg_opendental__claimpayment

WITH params AS (
    SELECT DATE '2026-06-24' AS payment_date
),
od_total AS (
    -- Replace with aggregate from golden CSV / golden_manifest.yml
    SELECT 11197.40::numeric(18, 2) AS net_collections
),
mart_total AS (
    SELECT SUM(m.net_collections_amount)::numeric(18, 2) AS net_collections
    FROM marts.mart_daily_payments m
    CROSS JOIN params p
    WHERE m.payment_date = p.payment_date
),
comparison AS (
    SELECT
        o.net_collections AS od_net_collections,
        m.net_collections AS mart_net_collections,
        m.net_collections - o.net_collections AS difference,
        CASE
            WHEN o.net_collections IS NULL THEN 'PENDING — set od_total from golden export'
            WHEN o.net_collections = 0 AND m.net_collections = 0 THEN 'PASS'
            WHEN ABS(m.net_collections - o.net_collections) <= 10.00 THEN 'PASS'
            WHEN ABS(m.net_collections - o.net_collections) / NULLIF(ABS(o.net_collections), 0) < 0.005 THEN 'PASS'
            WHEN ABS(m.net_collections - o.net_collections) / NULLIF(ABS(o.net_collections), 0) < 0.01 THEN 'WARN'
            ELSE 'FAIL'
        END AS validation_status
    FROM od_total o
    CROSS JOIN mart_total m
)
SELECT * FROM comparison;
