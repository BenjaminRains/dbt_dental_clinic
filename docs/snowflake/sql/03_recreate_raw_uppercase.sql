-- =============================================================================
-- Recreate uppercase RAW only (dbt source() compiles unquoted → RAW.PAYMENT).
-- Keep lowercase "staging"/"int"/"marts"/"dbt" for dbt models (quoting: true).
-- Role: ACCOUNTADMIN. One statement at a time.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS OPENDENTAL_SF.RAW
  COMMENT = 'Export landing — unquoted name matches dbt source() folding';

GRANT ALL ON SCHEMA OPENDENTAL_SF.RAW TO ROLE TRANSFORMER;
GRANT ALL ON FUTURE TABLES IN SCHEMA OPENDENTAL_SF.RAW TO ROLE TRANSFORMER;

CREATE STAGE IF NOT EXISTS OPENDENTAL_SF.RAW.DEMO_EXPORT
  COMMENT = 'Internal stage for demo Postgres CSV exports';

GRANT READ, WRITE ON STAGE OPENDENTAL_SF.RAW.DEMO_EXPORT TO ROLE TRANSFORMER;

SHOW SCHEMAS IN DATABASE OPENDENTAL_SF;
