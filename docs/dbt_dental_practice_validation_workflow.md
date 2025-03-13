# OpenDental DBT Validation Workflow

## Overview

This document outlines the workflow for validating and implementing staging models in our dental practice analytics DBT project. This process ensures that each staging model is properly validated against source data, follows consistent patterns, and has appropriate tests and documentation.

This workflow incorporates our SQL naming conventions to maintain consistency across all database interactions.

> **Technical Environment**: MariaDB v11.6 is used for the initial exploratory development in DBeaver. Final DBT models are then implemented and tested using the DBT framework.

## Files Structure

Our project follows this directory structure for staging model validation:

```
dbt_dental_practice/
├── dbeaver_validation/
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
   - File location: `dbeaver_validation/stg_<table_name>_dbeaver.sql`
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
   - Location: `docs/validation_logs/staging/opendental/stg_opendental_<table_name>_validation.md`
   - Include key statistics (record count, date ranges)
   - Document data distributions
   - Note any business rules implemented
   - Detail any data quality issues

4. **Finalize the staging model**
   - Rename from `models/staging/opendental/stg_<table_name>.sql` to `models/staging/opendental/stg_opendental__<table_name>.sql` 
   - Ensure all comments and documentation are complete
   - Add to DBT project dependencies if needed

## Validation Checklist

Use this checklist for each table validation:

1. **Basic Data Profiling**
   - [ ] Record count and date range analysis
   - [ ] Primary key validation (uniqueness, nulls)
   - [ ] Foreign key relationship checks
   - [ ] Missing/null value analysis for critical fields

2. **Data Type & Range Validation**
   - [ ] Date fields (valid range, no future dates unless appropriate)
   - [ ] Numeric fields (expected ranges, sign checks)
   - [ ] Code/type fields (valid values, distribution)

3. **Business Rule Validation**
   - [ ] Table-specific rules identified
   - [ ] Cross-field validation rules checked
   - [ ] Look for outliers or unusual patterns

4. **Documentation**
   - [ ] Key patterns documented in YML file
   - [ ] Special handling/transformations noted
   - [ ] Validation results documented in markdown

## SQL Patterns for Common Validations

### 1. Date Range Validation

```sql
WITH DateRangeCheck AS (  -- CamelCase for CTE name
    SELECT
        MIN(DateField) AS min_date,  -- CamelCase DB column to snake_case result
        MAX(DateField) AS max_date,
        COUNT(*) AS total_records,
        COUNT(CASE WHEN DateField < '2022-01-01' THEN 1 END) AS pre_2022_count,
        COUNT(CASE WHEN DateField > CURRENT_DATE() THEN 1 END) AS future_date_count
    FROM source_table
)
```

### 2. Distribution Analysis

```sql
WITH DistributionAnalysis AS (  -- CamelCase for CTE name
    SELECT 
        CategoryField,           -- CamelCase DB column
        COUNT(*) AS record_count,  -- Derived field in snake_case
        ROUND(AVG(NumericField), 2) AS avg_amount,  -- CamelCase to snake_case
        MIN(NumericField) AS min_amount,
        MAX(NumericField) AS max_amount,
        COUNT(CASE WHEN NumericField < 0 THEN 1 END) AS negative_count
    FROM source_table
    GROUP BY CategoryField
    ORDER BY record_count DESC
)
```

### 3. Validation Failures

```sql
WITH ValidationFailures AS (  -- CamelCase for CTE name
    SELECT 
        IdField,               -- CamelCase DB column
        'rule_name' AS check_name,  -- Derived field in snake_case
        'Description of validation failure' AS validation_message
    FROM source_table
    WHERE [validation condition]  -- Use CamelCase for DB column references

    UNION ALL

    -- Add other validation rules
)
```

## Business Rule Documentation Template

In your YML files, document business rules clearly:

```yaml
- name: field_name
  description: |
    Description of the field. Current patterns:
    - Value X: Meaning and statistics (count, avg)
    - Value Y: Meaning and statistics (count, avg)
    
    Business rules:
    - Rule 1: Description
    - Rule 2: Description
```

## Prioritization Guidelines

Prioritize validation efforts based on:

1. **Financial Impact Tables**:
   - Payment (already done)
   - Procedure
   - Claim
   - Insurance

2. **Clinical Core Tables**:
   - Patient
   - Appointment
   - ProcedureLog

3. **Support/Reference Tables**:
   - Provider
   - FeeSched
   - ProcedureCode

## SQL Naming Conventions

Our project follows specific naming conventions for SQL code as defined in `sql_naming_conventions.md`:

### Raw Database Column References

- **Rule**: Use CamelCase for all references to raw database columns
- **Example**: `PayType`, `PatNum`, `DatePay`, `SplitAmt`
- **Rationale**: Maintains consistency with the actual database schema

### Derived/Calculated Fields

- **Rule**: Use snake_case for all derived or calculated fields
- **Example**: `total_payment`, `percent_current`, `days_since_payment`
- **Rationale**: Visually distinguishes derived data from raw database columns

### Common Table Expression (CTE) Names

- **Rule**: Use CamelCase for CTE definition names
- **Example**: `PaymentTypeDef`, `PatientBalances`, `UnearnedTypeDefinition`
- **Rationale**: CTEs represent database-like objects/entities

### SQL File Names

- **Rule**: Use snake_case for all SQL file names
- **Example**: `unearned_income_payment_type.sql`, `payment_split_analysis.sql`
- **Rationale**: Follows Pythonic conventions for file naming

### Example of Proper SQL Styling

```sql
-- CTE with CamelCase name
WITH PatientPayments AS (
    SELECT
        pt.PatNum,                              -- Raw DB column (CamelCase)
        pt.LName,                               -- Raw DB column (CamelCase)
        SUM(ps.SplitAmt) AS total_payments,     -- Calculated (snake_case)
        COUNT(*) AS payment_count,              -- Calculated (snake_case)
        AVG(ps.SplitAmt) AS average_payment     -- Calculated (snake_case)
    FROM patient pt
    JOIN paysplit ps ON ps.PatNum = pt.PatNum
    GROUP BY pt.PatNum, pt.LName
)
```

## Notes for Other Developers

- The `paste.txt` file contains a list of all staging model files in the project
- All staging models follow the same validation workflow
- Validated models use the naming convention `stg_opendental__<table_name>.sql`
- Unvalidated models use the naming convention `stg_<table_name>.sql`
- The `dbt_stg_models_plan.md` document contains overall standards and requirements
- Adhere to the SQL naming conventions for all database interactions