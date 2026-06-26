-- KPI validation: daily net collections vs OpenDental Daily Payments export
-- Run in DBeaver against PostgreSQL opendental_analytics (analytics_user).
--
-- Prerequisites:
--   1. Export OD report CSV to validation/kpi/daily-payments/golden/
--   2. Enter OD total in the od_total CTE below (from export total row or sum)
--   3. Match date and filters documented in KPI_VALIDATION_REGISTRY.md
--
-- Validated dates:
--   2026-06-24 — golden od_daily_payments_06242026_06242026.csv — OD $11,197.40 PASS
--   2025-10-07 — golden od_daily_payments_10072025_10072025.csv — OD $21,747.30 PASS
--   2025-11-08 — golden od_daily_payments_11082025_11082025.csv — OD $3,791.65 PASS
--
-- Internal mart vs staging check (no OD export needed):
--   compare/compare_daily_collections_staging.sql

WITH params AS (
    SELECT DATE '2026-06-24' AS payment_date  -- change date per validation run
),
od_total AS (
    -- Replace with aggregate from golden CSV / golden_manifest.yml
    -- 2026-06-24: 11197.40 | 2025-10-07: 21747.30 | 2025-11-08: 3791.65
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
