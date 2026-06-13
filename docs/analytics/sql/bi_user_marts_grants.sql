-- PostgreSQL (Amazon Aurora): read-only BI user with SELECT on named marts tables only.
-- Aurora is expected to live in a private VPC (no public cluster access). Apply grants from
-- a bastion, Session Manager, or CI job that can run SQL against the analytics database.
-- Run as superuser or a role that can grant on these tables.
--
-- Replace:
--   opendental_analytics     if your analytics database name differs
--   <strong_password>        use secret manager + rotation in production
--
-- USAGE on schema marts is required to resolve names; SELECT is per-table so bi_user cannot
-- read other mart objects until you GRANT them explicitly.

-- \c opendental_analytics

CREATE ROLE bi_user WITH LOGIN PASSWORD '<strong_password>';
COMMENT ON ROLE bi_user IS 'Read-only BI: USAGE on marts; SELECT only on approved mart tables.';

GRANT CONNECT ON DATABASE opendental_analytics TO bi_user;

GRANT USAGE ON SCHEMA marts TO bi_user;

-- Pilot / executive KPI slice — keep list in sync with docs/analytics/power_bi_pilot_dataset.md
GRANT SELECT ON TABLE marts.mart_revenue_lost TO bi_user;
GRANT SELECT ON TABLE marts.mart_provider_performance TO bi_user;
GRANT SELECT ON TABLE marts.dim_provider TO bi_user;
GRANT SELECT ON TABLE marts.dim_date TO bi_user;

-- When a new table is approved for Power BI, add explicitly, e.g.:
-- GRANT SELECT ON TABLE marts.mart_ar_summary TO bi_user;

-- Do NOT use GRANT SELECT ON ALL TABLES IN SCHEMA marts for this user if the goal is least privilege.
