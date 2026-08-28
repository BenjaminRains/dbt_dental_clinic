-- =============================================================================
-- One-time cleanup: drop leftover uppercase *model* schemas from first bootstrap.
-- Keep OPENDENTAL_SF.RAW (export + dbt sources). Do NOT drop PUBLIC.
-- Role: ACCOUNTADMIN. Run one statement at a time.
-- =============================================================================

DROP SCHEMA IF EXISTS OPENDENTAL_SF.STAGING CASCADE;
DROP SCHEMA IF EXISTS OPENDENTAL_SF.INT CASCADE;
DROP SCHEMA IF EXISTS OPENDENTAL_SF.MARTS CASCADE;
DROP SCHEMA IF EXISTS OPENDENTAL_SF.DBT CASCADE;

-- Optional: only if you also created a lowercase quoted "raw" you no longer need
-- DROP SCHEMA IF EXISTS "OPENDENTAL_SF"."raw" CASCADE;

SHOW SCHEMAS IN DATABASE OPENDENTAL_SF;
