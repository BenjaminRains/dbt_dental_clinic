# OpenDental DBT Validation Workflow

## Overview

This document outlines the workflow for validating and implementing staging models in our dental practice analytics DBT project. This process ensures that each staging model is properly validated against source data, follows consistent patterns, and has appropriate tests and documentation.

## Development Progression

The workflow follows a clear progression from analysis to final model:

1. **Analysis Phase** (`analysis/`): Initial data exploration and pattern discovery
2. **Validation Phase** (`tests/`): Business rule development and validation
3. **Implementation Phase** (`models/`): Final, validated staging models

## Directory Structure

Our project follows this progression-based directory structure:

```
dbt_dental_practice/
├── analysis
│   └── stg_<table_name>_dbeaver.sql        # Initial DBeaver exploratory SQL
├── docs/
│   └── validation_logs/
│       └── staging/
│           └── opendental/
│               └── stg_opendental_<table_name>_validation.md  # Validation results
├── models/
│   ├── staging/
│   │   └── opendental/
│   │       ├── stg_<table_name>.sql                  # Initial unvalidated staging models
│   │       ├── stg_opendental__<table_name>.sql      # Validated final staging models
│   │       └── _stg_opendental__<table_name>.yml     # Model tests and documentation
│   └── _opendental_sources.yml                       # Source definitions
├── tests/
│   └── staging/
│       └── stg_<table_name>_validation.sql           # Validation test SQL scripts
└── dbt_stg_models_plan.md                            # Overall staging model plan
```

**Important**: For DBT tests, three files work together:
1. `models/staging/opendental/stg_opendental__<table_name>.sql` - The model SQL
2. `models/staging/opendental/_stg_opendental__<table_name>.yml` - Tests and documentation
3. `models/_opendental_sources.yml` - Source definition

## Reference Documents

- **dbt_stg_models_plan.md**: Overall staging models plan with standards and validation requirements
- **dbt_project.yml**: Project configuration with materialization settings
- **_opendental_sources.yml**: Definition of all OpenDental source tables
- **stg_opendental_payment_validation.md**: Example of validation results documentation
- **sql_naming_conventions.md**: SQL naming conventions for consistency

## Validation Workflow

### Phase 1: Exploratory Analysis in DBeaver

1. **Create exploratory SQL script** in DBeaver
   - File location: `analysis/stg_<table_name>_dbeaver.sql`
   - Start with data profiling to understand table structure
   - Analyze field distributions and patterns
   - Identify potential business rules and validation checks

   ```sql
   -- Example from stg_payment_dbeaver.sql
   with current_payment_types as (
       select 
           PayType as payment_type_id,         -- CamelCase original DB column to snake_case
           count(*) as payment_count,          -- Derived field in snake_case
           round(avg(PayAmt), 2) as avg_amount, -- Derived field in snake_case
           min(PayAmt) as min_amount,          -- Derived field in snake_case
           max(PayAmt) as max_amount,          -- Derived field in snake_case
           count(case when PayAmt < 0 then 1 end) as negative_count, -- Derived field in snake_case
           min(PayDate) as first_seen,         -- Derived field in snake_case
           max(PayDate) as last_seen           -- Derived field in snake_case
       from opendental_analytics_opendentalbackup_02_28_2025.payment
       where PayDate >= '2023-01-01'           -- CamelCase DB column reference
           and PayDate <= current_date()
       group by PayType                        -- CamelCase DB column reference
       order by payment_count desc
   )
   select * from current_payment_types;        -- CamelCase CTE name
   ```

2. **Identify validation rules** based on data patterns
   - Document field constraints and expected ranges
   - Note any business rules discovered (e.g., Type 0 payments must be $0)
   - Identify any data quality issues to handle

3. **Document findings** for reference in DBT implementation

### Phase 2: DBT Model Implementation

1. **Create initial staging model** with basic transformations
   - Location: `models/staging/opendental/stg_<table_name>.sql` 
   - Include standard field renaming and data type conversions
   - Apply business rules identified in Phase 1
   - Add field-level SQL comments for important transformations

2. **Implement standard transformations** as defined in `dbt_stg_models_plan.md`:
   - Convert 0 values to NULL for ID/reference fields
   - Standardize boolean fields to true/false
   - Ensure consistent date/timestamp formats
   - Apply standardized column naming (snake_case for all output fields)
   - Categorize fields (keys, dates, amounts, etc.)
   - Follow SQL naming conventions (see dedicated section below)

3. **Run initial dbt model** and verify against DBeaver SQL results

### Phase 3: Test and Documentation Implementation

1. **Create model YAML file** with tests and documentation
   - Location: `models/staging/opendental/_stg_opendental__<table_name>.yml`
   - Include model description
   - Document column descriptions and business context
   - Add standard tests (uniqueness, not null, etc.)
   - Add custom tests for business rules

   ```yaml
   # Example from _stg_opendental__payment.yml
   version: 2
   
   models:
     - name: stg_opendental__payment
       description: >
         Staged payment data from OpenDental system.
         Analysis based on 2023-current data.
       tests:
         - dbt_utils.expression_is_true:
             expression: "payment_date >= '2023-01-01'"
       columns:
         - name: payment_id
           description: Primary key for payments
           tests:
             - unique
             - not_null
   ```

2. **Add business-specific tests** based on exploratory findings
   - Document data patterns in column descriptions
   - Implement custom tests for specific business rules
   - Add warnings for potential data issues

### Phase 4: Final Validation and Documentation

1. **Create validation test SQL**
   - Location: `tests/staging/stg_<table_name>_validation.sql`
   - Implement specific validation logic
   - Test business rules and data quality aspects

2. **Run complete validation tests**
   - Execute DBT tests: `dbt test --select stg_opendental__<table_name>`
   - Document test results and any failures
   - Update model if needed based on test results

3. **Create validation documentation**
   - Location: `