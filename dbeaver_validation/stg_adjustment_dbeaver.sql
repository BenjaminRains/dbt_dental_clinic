/*
  DEVELOPMENT VERSION FOR DBEAVER TESTING
  DBT Version: models/staging/sql_validation/stg_adjustment_validation.sql
  
  Changes needed for DBT:
  - Replace direct table references with {{ ref() }}
  - Remove _dev suffix from filename
*/

-- First, let's analyze adjustment types for current data
