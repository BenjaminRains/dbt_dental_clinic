# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Database Architecture
- PostgreSQL database containing dental clinic data migrated from MariaDB
- ELT/ETL pipeline in `etl_job/mariadb_postgre_pipe.py` handles type conversions
- Data transformation flow: MariaDB source → PostgreSQL source → staging models → intermediate models → marts
- Example: patient data evolution
  - Raw MariaDB DDL: `analysis/patient/patient_ddl.sql`
  - PostgreSQL schema: `analysis/patient/patient_pg_ddl.sql`
  - dbt staging: `models/staging/opendental/stg_opendental__patient.sql`
  - dbt intermediate: `models/intermediate/foundation/int_patient_profile.sql`

## Schema Structure (for DBeaver Queries)
- Raw source data: `public.<table_name>` (e.g., `public.patient`)
- Staging models: `public_staging.<model_name>` (e.g., `public_staging.stg_opendental__patient`) 
- Intermediate models: `public_intermediate.<model_name>` (e.g., `public_intermediate.int_patient_profile`)
- Mart models: `public_marts.<model_name>` (e.g., `public_marts.dim_patients`)

## Commands

### Build & Run
- Run DBT models: `./scripts/run_dbt.sh run` (Linux/Mac) or `.\scripts\run_dbt.bat run` (Windows)
- Run specific models: `./scripts/run_dbt.sh run --select model_name`
- Test all models: `./scripts/run_dbt.sh test`
- Test specific model: `./scripts/run_dbt.sh test --select model_name`
- Python formatting: `pipenv run format` (Black)
- Python linting: `pipenv run lint` (Pylint)
- Sort imports: `pipenv run sort-imports` (isort)
- Run tests: `pipenv run test` (pytest)

## Code Style Guidelines

### SQL 
- **Source System Columns**: Use **CamelCase** (with quotes) for raw database columns (e.g., "PatNum")
- **ID Fields**: Convert suffixes from `_num` in source to `_id` in output (e.g., "PatNum" → patient_id)
- **Derived Fields**: Use **snake_case** for transformed and calculated fields
- **CTEs**: Use **CamelCase** for ALL CTEs (project-specific convention)
- **SQL Files**: Use **snake_case** for all SQL file names
- **Boolean Conversions**: Use CASE statements for smallint(0/1) to boolean conversions:
  ```sql
  CASE WHEN "IsHidden" = 1 THEN true WHEN "IsHidden" = 0 THEN false ELSE null END as is_hidden
  ```
- **Model Names**: Follow patterns:
  - Staging: `stg_[source]__[entity]`
  - Intermediate: `int_[entity]_[verb]`
  - Marts: `[mart]_[entity]`

### Query Example for DBeaver
```sql
-- Query that joins raw source to staging model
SELECT 
    p."PatNum", 
    s.patient_id, 
    p."LName", 
    p."FName",
    s.total_balance
FROM public.patient p
JOIN public_staging.stg_opendental__patient s
    ON p."PatNum" = s.patient_id
WHERE s.patient_status = 0
LIMIT 10;
```

### Python
- Follow standard Python style (Black formatter)
- Python version: 3.11
- Use proper type annotations
- Sort imports with isort

### Directory Structure
- models/staging/: Raw data transformations
- models/intermediate/: Business logic by system (A-G)
- models/marts/: Business-specific data marts
- macros/: Reusable SQL templates
- tests/: Data quality tests