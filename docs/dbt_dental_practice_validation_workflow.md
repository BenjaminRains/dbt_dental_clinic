# PostgreSQL-Based OpenDental DBT Validation Workflow

## Overview

This document outlines our end-to-end workflow for developing and validating DBT staging models for 
OpenDental data that has been migrated to PostgreSQL. The process moves through four distinct 
phases: PostgreSQL Schema Analysis, Validation Rules Development, Staging Model Implementation, and 
Automated Validation.

This workflow incorporates our project's SQL naming conventions, which include:
- **CamelCase** for raw database column references and CTEs
- **snake_case** for derived/calculated fields, file names, and dbt objects
- Clear distinction between original database fields and transformed data

## Project Structure

```
dbt_dental_practice/
├── analysis/
│   └── <table_name>/                           # One directory per source table
│       ├── <table_name>_pg_ddl.sql             # PostgreSQL table DDL (ground truth)
│       ├── <table_name>_pg_analysis.sql        # PostgreSQL-specific analysis queries
│       ├── analysis_output.csv                 # Analysis query results
│       └── *.ipynb                             # Analysis notebooks
├── tests/
│   ├── generic/
│   │   └── test_postgres_*.sql                 # PostgreSQL-specific generic tests
│   └── staging/
│       └── <table_name>_pg_validation_rules.sql  # PostgreSQL validation rules
├── models/
│   ├── staging/
│   │   └── opendental/
│   │       ├── stg_opendental__<table_name>.sql  # PostgreSQL-ready staging model
│   │       └── _stg_opendental__<table_name>.yml # Model definition and tests
│   └── sources.yml                             # PostgreSQL source definitions
```

## Phase 1: PostgreSQL Schema Analysis

### 1.1 PostgreSQL DDL as Ground Truth
- Extract or generate DDL from your PostgreSQL tables using:
  ```bash
  pg_dump -s -t public.<table_name> database_name > <table_name>_pg_ddl.sql
  ```
- Save as `<table_name>_pg_ddl.sql` in the analysis directory
- Document differences from the original MariaDB schema, noting:
  - Column naming conventions (PascalCase vs snake_case)
  - Data type mappings (e.g., `DOUBLE` to `DOUBLE PRECISION`)
  - Index structures and performance implications
  - Constraint implementations

### 1.2 PostgreSQL-Based Exploratory Analysis
- Create `<table_name>_pg_analysis.sql` with PostgreSQL-specific syntax
- Focus analysis on:
  - Data distributions and patterns
  - PostgreSQL-specific data type behavior
  - Value ranges and outliers
  - Relationship validations
  - Business rule discovery

Example PostgreSQL Analysis Query (following naming conventions):
```sql
-- PostgreSQL analysis for payment table
WITH DataProfile AS (
    SELECT 
        COUNT(*) AS total_records,
        MIN("PayDate") AS earliest_date,
        MAX("PayDate") AS latest_date,
        COUNT(DISTINCT "PatNum") AS unique_patients
    FROM public.payment
    WHERE "PayDate" >= '2023-01-01'::date
),

PaymentTypeAnalysis AS (
    SELECT 
        "PayType" AS payment_type_id,  -- Raw column to snake_case derived field
        COUNT(*) AS payment_count,
        ROUND(AVG("PayAmt")::numeric, 2) AS avg_amount,
        MIN("PayAmt") AS min_amount,
        MAX("PayAmt") AS max_amount,
        COUNT(CASE WHEN "PayAmt" < 0 THEN 1 END) AS negative_count,
        MIN("PayDate") AS first_seen,
        MAX("PayDate") AS last_seen
    FROM public.payment
    WHERE "PayDate" >= '2023-01-01'::date
    GROUP BY "PayType"
    ORDER BY payment_count DESC
)

SELECT * FROM DataProfile;
SELECT * FROM PaymentTypeAnalysis;
```

## Phase 2: PostgreSQL-Aware Validation Rules

### 2.1 Developing PostgreSQL Validation Rules
- Create `<table_name>_pg_validation_rules.sql` in tests/staging/
- Adapt validation logic for PostgreSQL syntax and data types
- Transform discovered patterns into explicit validation rules
- Focus on:
  - Data integrity rules
  - Business logic validation
  - Edge case handling
  - PostgreSQL-specific data type considerations

Example PostgreSQL Validation Rules (following naming conventions):
```sql
-- PostgreSQL-specific validation for payment table
WITH PaymentData AS (
    SELECT 
        "PayNum" AS payment_id,       -- Raw DB column to derived field (snake_case)
        "PayType" AS payment_type_id,
        "PayAmt" AS payment_amount,
        "PayDate" AS payment_date,
        "PayNote" AS payment_notes
    FROM public.payment
    WHERE "PayDate" >= '2023-01-01'::date
),

ValidationFailures AS (
    -- Type 0 validation (Administrative)
    SELECT 
        'type_0_nonzero' AS check_name,
        payment_id,
        payment_type_id,
        payment_amount,
        payment_date,
        payment_notes,
        'Error: Type 0 payment with non-zero amount' AS validation_message
    FROM PaymentData
    WHERE payment_type_id = 0 AND payment_amount != 0

    UNION ALL

    -- Type 72 validation (Refunds)
    SELECT 
        'type_72_positive' AS check_name,
        payment_id,
        payment_type_id,
        payment_amount,
        payment_date,
        payment_notes,
        'Error: Type 72 payment with non-negative amount' AS validation_message
    FROM PaymentData
    WHERE payment_type_id = 72 AND payment_amount >= 0
    
    UNION ALL
    
    -- High value payment warnings
    SELECT 
        'high_value_payment' AS check_name,
        payment_id,
        payment_type_id,
        payment_amount,
        payment_date,
        payment_notes,
        'Warning: Unusually high payment amount for type' AS validation_message
    FROM PaymentData
    WHERE (
        (payment_type_id = 69 AND payment_amount > 5000) OR
        (payment_type_id = 574 AND payment_amount > 50000)
    )
)

-- Summary of validation failures
SELECT 
    check_name,
    validation_message,
    COUNT(*) AS failure_count
FROM ValidationFailures
GROUP BY check_name, validation_message

UNION ALL

-- Detailed failures for review
SELECT 
    check_name,
    validation_message,
    1 AS failure_count
FROM ValidationFailures
ORDER BY check_name, payment_id;
```

## Phase 3: DBT Staging Model Implementation

### 3.1 PostgreSQL-Ready DBT Models
- Update `stg_opendental__<table_name>.sql` for PostgreSQL compatibility
- Replace source references with PostgreSQL schema references
- Account for PostgreSQL-specific syntax and functions
- Implement proper incremental logic for PostgreSQL

Example PostgreSQL-Ready DBT Model (following naming conventions):
```sql
{{ config(
    materialized='incremental',
    unique_key='payment_id'
) }}

WITH Source AS (  -- CamelCase for CTE names
    SELECT * 
    FROM public.payment
    WHERE "PayDate" >= '2023-01-01'::date  
        AND "PayDate" <= CURRENT_DATE
        AND "PayDate" > '2000-01-01'::date  -- Exclude obviously invalid dates
    {% if is_incremental() %}
        -- Only get new records if this is an incremental run
        AND "PayDate" > (SELECT max(payment_date) FROM {{ this }})
    {% endif %}
),

Renamed AS (  -- CamelCase for CTE names
    SELECT
        -- Primary key
        "PayNum" AS payment_id,  -- CamelCase DB column to snake_case derived field
        
        -- Relationships
        "PatNum" AS patient_id,
        "ClinicNum" AS clinic_id,
        "PayType" AS payment_type_id,
        "DepositNum" AS deposit_id,
        "SecUserNumEntry" AS created_by_user_id,

        -- Payment details
        "PayDate" AS payment_date,
        "PayAmt" AS payment_amount,
        "MerchantFee" AS merchant_fee,
        "CheckNum" AS check_number,
        "BankBranch" AS bank_branch,
        "ExternalId" AS external_id,

        -- Status flags
        "IsSplit" AS is_split_flag,
        "IsRecurringCC" AS is_recurring_cc_flag,
        "IsCcCompleted" AS is_cc_completed_flag,
        "PaymentStatus" AS payment_status,
        "ProcessStatus" AS process_status,
        "PaymentSource" AS payment_source,

        -- Recurring payment info
        "RecurringChargeDate" AS recurring_charge_date,

        -- Dates
        "DateEntry" AS entry_date,
        "SecDateTEdit" AS updated_at,

        -- Notes (may contain PHI)
        "PayNote" AS payment_notes,
        "Receipt" AS receipt_text,

        -- Add metadata
        current_timestamp AS _loaded_at
    FROM Source  -- Reference to CamelCase CTE
)

SELECT * FROM Renamed  -- Reference to CamelCase CTE
```
```

### 3.2 Update YML Configs for PostgreSQL
- Modify `_stg_opendental__<table_name>.yml` to reflect PostgreSQL schema
- Update sources.yml to point to PostgreSQL environment:

```yaml
version: 2

sources:
  - name: opendental
    description: "OpenDental data in PostgreSQL"
    database: postgres  # Or your actual database name
    schema: public      # Or your actual schema
    tables:
      - name: payment
        description: "Patient payments for services"
        meta:
          data_quality_results:
            last_tested: '2025-03-14'
            tests_passed: 8
            tests_total: 8
            quality_checks:
              - test: "unique_payment_id"
                status: "pass"
              - test: "not_null_critical_fields"
                status: "pass"
                fields: ["payment_id", "patient_id", "payment_amount", "payment_date", "payment_type_id"]
              - test: "payment_type_validation"
                status: "pass"
                valid_values: [0, 69, 70, 71, 72, 391, 412, 417, 574, 634]
                description: "Payment types validated with warning severity for flexibility"
```

## Phase 4: Automated Validation

### 4.1 DBT Tests for PostgreSQL Data
- Create custom PostgreSQL-aware tests in tests/generic/
- Implement data quality tests for PostgreSQL specifics
- Apply tests in model YML files

Example PostgreSQL-Specific Test:
```sql
-- In tests/generic/test_postgres_date_validation.sql
{% test postgres_date_validation(model, column_name) %}

WITH validation AS (
    SELECT
        {{ column_name }} AS date_value
    FROM {{ model }}
    WHERE {{ column_name }} IS NOT NULL
        -- PostgreSQL-specific date validation
        AND ({{ column_name }} < '1900-01-01'::date 
             OR {{ column_name }} > CURRENT_DATE)
)

SELECT *
FROM validation

{% endtest %}
```

Usage in YML:
```yaml
models:
  - name: stg_opendental__payment
    columns:
      - name: payment_date
        tests:
          - postgres_date_validation
```

### 4.2 Integration Testing
- Develop end-to-end tests from PostgreSQL raw tables through staging to final models
- Compare output with expected results from analysis phase
- Document any discrepancies and resolution decisions
- Create dbt macros for PostgreSQL-specific validation patterns

## Best Practices

### SQL Naming Conventions

1. **CamelCase for Database References and CTEs**
   - Use CamelCase for all references to raw database columns: `"PayNum"`, `"PayType"`, `"PatNum"`
   - Use CamelCase for all CTEs: `Source`, `Renamed`, `PaymentTypeDef`
   - This matches our database schema style and provides visual consistency

2. **snake_case for Derived Fields and dbt Objects**
   - Use snake_case for all derived or calculated fields: `payment_id`, `total_amount`, `is_refund`
   - Use snake_case for dbt model names: `stg_opendental__payment`
   - Use snake_case for SQL file names: `payment_analysis.sql`

3. **Visual Differentiation**
   - The multi-case approach provides immediate visual cues about each element's nature
   - Makes it clear which fields come from the database vs. which are calculated

4. **Consistency Across Files**
   - Maintain consistent naming patterns across analysis, validation, and model files
   - Ensure all team members follow the same conventions

### PostgreSQL-Specific Considerations

1. **Data Type Handling**
   - Be careful with PostgreSQL's strict type handling
   - Use explicit casts (::type) when needed
   - Pay special attention to date/time types

2. **Performance Optimization**
   - Leverage PostgreSQL's advanced indexing capabilities
   - Consider using PostgreSQL-specific features like CTEs
   - Use EXPLAIN ANALYZE to verify query performance

3. **Field Naming Conventions**
   - Maintain consistent PascalCase to snake_case conversions
   - Document column name transformations
   - Consider using views to standardize naming conventions

4. **Migration Documentation**
   - Document any data inconsistencies found during migration
   - Note field type conversions and their impacts
   - Maintain mapping between original and PostgreSQL schemas

### Development Process

1. **Version Control**
   - Commit PostgreSQL DDL files as the new ground truth
   - Track analysis query evolution
   - Document PostgreSQL-specific findings in commits

2. **Documentation**
   - Link PostgreSQL analysis findings to validation rules
   - Document PostgreSQL-specific decisions
   - Maintain clear transformation logic

3. **Testing**
   - Start with PostgreSQL structure tests
   - Add business rule validations
   - Include PostgreSQL-specific data quality checks

4. **Collaboration**
   - Regular stakeholder reviews
   - Clear documentation of PostgreSQL migration decisions
   - Traceable evolution of analysis and adaptations

## Validation and Naming Workflow Summary

1. **Extract PostgreSQL Schema** → Document current state with proper casing
2. **Analyze PostgreSQL Data** → Understand patterns using proper CTE naming (CamelCase)
3. **Develop PostgreSQL Validation Rules** → Ensure data quality with consistent naming
4. **Create PostgreSQL-Ready DBT Models** → Implement transformations with proper naming conventions
   - CamelCase for raw database references and CTEs
   - snake_case for derived fields and dbt objects
5. **Add PostgreSQL-Specific Tests** → Validate continuously
6. **Document Migration Decisions** → Maintain knowledge base with clear naming standards

## Example Complete Workflow

Here's an example of the complete workflow for the `payment` table:

1. **Generate PostgreSQL DDL**
   ```bash
   pg_dump -s -t public.payment database_name > analysis/payment/payment_pg_ddl.sql
   ```

2. **Create Analysis File** (`payment_pg_analysis.sql`)
   ```sql
   -- Following proper naming conventions
   WITH PaymentTypes AS (
       SELECT 
           "PayType" AS payment_type_id,
           COUNT(*) AS payment_count
       FROM public.payment
       GROUP BY "PayType"
   )
   SELECT * FROM PaymentTypes;
   ```

3. **Create Validation Rules** (`payment_pg_validation_rules.sql`)
   ```sql
   WITH PaymentData AS (
       SELECT 
           "PayNum" AS payment_id,
           "PayAmt" AS payment_amount,
           "PayType" AS payment_type_id
       FROM public.payment
   ),
   
   ValidationResults AS (
       -- Validation logic here
   )
   
   SELECT * FROM ValidationResults;
   ```

4. **Create DBT Model** (`stg_opendental__payment.sql`)
   ```sql
   WITH Source AS (
       SELECT * FROM public.payment
   ),
   
   Renamed AS (
       SELECT
           "PayNum" AS payment_id,
           -- Other fields
       FROM Source
   )
   
   SELECT * FROM Renamed;
   ```

Following this workflow with consistent naming conventions will enhance code readability, 
reduce errors, and make maintenance significantly easier.