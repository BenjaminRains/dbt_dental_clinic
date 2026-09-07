-- =============================================================================
-- Recreate quoted lowercase "raw" (dbt source() with quoting: true → "raw"."payment").
-- Drop unquoted RAW if it exists — that object is not what staging reads.
-- Keep lowercase "staging"/"int"/"marts"/"dbt" for dbt models.
-- Role: ACCOUNTADMIN. One statement at a time.
-- =============================================================================

DROP SCHEMA IF EXISTS OPENDENTAL_SF.RAW CASCADE;

CREATE SCHEMA IF NOT EXISTS "OPENDENTAL_SF"."raw"
  COMMENT = 'Export landing — quoted name matches dbt source() quoting: true';

GRANT ALL ON SCHEMA "OPENDENTAL_SF"."raw" TO ROLE TRANSFORMER;
GRANT ALL ON FUTURE TABLES IN SCHEMA "OPENDENTAL_SF"."raw" TO ROLE TRANSFORMER;

CREATE STAGE IF NOT EXISTS "OPENDENTAL_SF"."raw".DEMO_EXPORT
  COMMENT = 'Internal stage for demo Postgres CSV exports';

GRANT READ, WRITE ON STAGE "OPENDENTAL_SF"."raw".DEMO_EXPORT TO ROLE TRANSFORMER;

SHOW SCHEMAS IN DATABASE OPENDENTAL_SF;
