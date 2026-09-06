-- =============================================================================
-- One-time cleanup: drop leftover uppercase schemas from first bootstrap.
-- Keep quoted lowercase "raw"/"staging"/"int"/"marts"/"dbt". Do NOT drop PUBLIC.
-- Unquoted RAW.PAYMENT is not the object dbt source() reads (quoting: true).
-- Role: ACCOUNTADMIN. Run one statement at a time.
-- =============================================================================

DROP SCHEMA IF EXISTS OPENDENTAL_SF.RAW CASCADE;
DROP SCHEMA IF EXISTS OPENDENTAL_SF.STAGING CASCADE;
DROP SCHEMA IF EXISTS OPENDENTAL_SF.INT CASCADE;
DROP SCHEMA IF EXISTS OPENDENTAL_SF.MARTS CASCADE;
DROP SCHEMA IF EXISTS OPENDENTAL_SF.DBT CASCADE;

SHOW SCHEMAS IN DATABASE OPENDENTAL_SF;
