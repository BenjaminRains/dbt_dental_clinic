# Validation Framework

**Last Updated**: 2026-01-23  
**Purpose**: Comprehensive guide to data validation processes for staging, intermediate, and mart models

---

## Overview

This directory contains validation queries, investigation documents, and analysis for validating data quality, business logic correctness, and completeness across all dbt model layers.

**Validation Scope**:
- **Staging Models**: Source data quality, completeness, and transformation accuracy
- **Intermediate Models**: Business logic validation, data relationships, and calculated fields
- **Mart Models**: Final data quality, status category mappings, and business rule compliance

---

## Directory Structure

```
validation/
├── README.md (this file)
├── VALIDATION_TEMPLATE.md (generalized template for new validations)
├── staging/              # Validation for staging models
│   └── {model_name}/
│       ├── investigate_{issue}.sql
│       └── {ISSUE}_FINDINGS.md
├── intermediate/         # Validation for intermediate models
│   └── {model_name}/
│       ├── investigate_{issue}.sql
│       └── {ISSUE}_ANALYSIS.md
└── marts/               # Validation for mart models
    └── {model_name}/
        ├── investigate_{issue}.sql
        ├── {ISSUE}_ANALYSIS.md
        └── {MODEL}_VALIDATION_PLAN.md (comprehensive validation plans)
```

---

## How Validation Works

### 1. **Issue Identification**

Validation typically starts when:
- **dbt tests fail** (e.g., `accepted_values` test finds 'Unknown' values)
- **Data quality concerns** are raised (e.g., unexpected NULL values, missing mappings)
- **Business logic questions** arise (e.g., status category completeness)
- **Performance issues** are discovered (e.g., slow queries, missing indexes)

**Example**: `fact_claim` had 62,289 records with `payment_status_category = 'Unknown'`, indicating incomplete CASE statement mappings.

---

### 2. **Investigation Phase**

#### Step 2.1: Create Investigation SQL File

**File Pattern**: `investigate_{issue_description}.sql`

**Location**: `validation/{layer}/{model_name}/investigate_{issue_description}.sql`

**Structure**:
```sql
-- Investigation Queries for [Issue Description]
-- Date: YYYY-MM-DD
-- Purpose: [Brief description of what is being investigated]
-- Related: [Related documentation or validation plan]

-- ============================================================================
-- Query 1: [Category] Analysis
-- ============================================================================

-- Query 1.1: [Specific investigation]
SELECT 
    '[Investigation Type]' as investigation_type,
    [columns],
    COUNT(*) as record_count,
    ...
FROM [schema].[model]
WHERE [conditions]
GROUP BY [grouping]
ORDER BY [ordering];

-- Query 1.2: [Next investigation]
...
```

**Key Query Types**:
1. **Distribution Analysis**: Count records by category/status
2. **NULL Value Checks**: Identify missing data
3. **Edge Case Detection**: Find unexpected values or combinations
4. **Source Comparison**: Compare staging vs intermediate vs mart
5. **Sample Records**: Extract representative examples for manual review
6. **Cross-tabulation**: Analyze relationships between fields
7. **Summary Statistics**: Overall counts and percentages

#### Step 2.2: Create Analysis Document

**File Pattern**: `{ISSUE}_ANALYSIS.md` or `{ISSUE}_FINDINGS.md`

**Location**: `validation/{layer}/{model_name}/{ISSUE}_ANALYSIS.md`

**Structure** (see `VALIDATION_TEMPLATE.md` for full template):
- Executive Summary
- Current Logic Documentation
- Investigation Goals
- Investigation Queries (reference to SQL file)
- Expected Findings
- Investigation Results (populated after running queries)
- Root Cause Analysis
- Recommended Actions
- Related Documentation

---

### 3. **Query Execution**

**Where to Run**: DBeaver or database client connected to analytics database

**Database**: `opendental_analytics` (PostgreSQL)  
**User**: `analytics_user`  
**Schemas**: 
- `raw` - Source OpenDental data (MySQL source, loaded to PostgreSQL)
- `staging` - Staging models
- `intermediate` - Intermediate models  
- `marts` - Mart models

**Process**:
1. Copy queries from investigation SQL file
2. Run each query sequentially
3. Copy results and paste into analysis document
4. Analyze patterns and identify root causes

---

### 4. **Documentation and Analysis**

#### Step 4.1: Document Findings

Update the analysis document with:
- **Investigation Results**: Query outputs with counts, distributions, samples
- **Root Cause Analysis**: Why the issue exists (e.g., incomplete CASE statement, missing NULL handling)
- **Business Impact**: How this affects reporting, analytics, or operations

#### Step 4.2: Recommend Actions

Document recommended fixes:
- **Immediate Actions**: Quick fixes (e.g., add missing CASE statement branches)
- **Model Updates**: Changes to SQL logic
- **Documentation Updates**: Update YAML schema files with complete mappings
- **Data Quality**: Add dbt tests or data quality checks
- **Long-term**: Workflow improvements, process changes

---

### 5. **Implementation**

#### Step 5.1: Update Model Logic

Modify the model SQL file (e.g., `models/marts/fact_claim.sql`):
- Add missing CASE statement branches
- Handle NULL values explicitly
- Fix edge cases
- Add comments explaining business rules

#### Step 5.2: Update Documentation

Update YAML schema files (e.g., `models/marts/_fact_claim.yml`):
- Document all valid values
- Add data quality notes
- Update descriptions with complete mapping logic
- Add business rules and edge case handling

#### Step 5.3: Map Findings to Business Rules and dbt Tests

**Process**: Convert validation findings into automated dbt tests

**Mapping Process**:
1. **Identify Business Rules**: Extract business rules from investigation findings
2. **Categorize Test Types**: Determine appropriate dbt test type for each rule
3. **Create Test Definitions**: Add tests to YAML schema files
4. **Document Business Context**: Include business rule descriptions in test configs

**Test Categories**:

**Category 1: Simple Schema Tests** (Easy Wins)
- `accepted_values` - Status categories, enumerated values
- `not_null` - Critical fields that must have values
- `relationships` - Foreign key integrity
- `unique` / `unique_combination_of_columns` - Uniqueness constraints

**Category 2: Expression Tests** (Business Logic)
- `dbt_utils.expression_is_true` - Simple business rule expressions
- `dbt_expectations.expression_is_true` - Complex business logic validation
- `dbt_expectations.expect_column_values_to_be_between` - Value range checks
- `dbt_expectations.expect_column_pair_values_A_to_be_greater_than_B` - Relationship validations

**Category 3: Custom Macros** (Complex Business Rules)
- Create reusable test macros for complex logic
- Examples: Financial balance checks, status category logic, calculation validations
- Location: `macros/tests/domain/`

**Category 4: Data Tests** (Source Comparison)
- Custom SQL tests that compare source vs target
- Examples: Row count reconciliation, amount accuracy, status value accuracy
- Location: `tests/{layer}/{model_name}/`

**Example Mapping**:

**Finding**: "62,289 records have `payment_status_category = 'Unknown'` due to incomplete CASE statement"

**Business Rule**: "All valid `claim_status` values should map to defined payment status categories"

**dbt Test**:
```yaml
- name: payment_status_category
  tests:
    - accepted_values:
        values: ['Paid', 'Pending', 'Denied', 'Rejected', 'Pre-auth', 'Unknown']
        config:
          severity: warn
          description: >
            Payment status must be one of the valid business categories.
            'Unknown' values indicate unmapped claim_status values that need investigation.
```

**Reference**: See `validation/marts/fact_claim/BUSINESS_RULES_TO_DBT_TESTS.md` for complete example of mapping validation findings to dbt tests.

---

### 6. **Verification**

After implementation:
1. **Re-run investigation queries** to verify fixes
2. **Run dbt tests** to ensure all tests pass
3. **Update analysis document** with implementation status
4. **Document lessons learned** for future reference

---

## Common Validation Patterns

### Pattern 1: Status Category Mapping Validation

**When to Use**: Models with CASE statements that map source values to categories

**Example**: `fact_claim.payment_status_category`, `fact_appointment.appointment_outcome_status`

**Process**:
1. Identify all source values (from staging/intermediate models)
2. Check which values map to 'Unknown' or catch-all categories
3. Verify all documented values are handled
4. Test edge cases (NULL values, boundary conditions)

**Files Created**:
- `investigate_{field}_mappings.sql`
- `{FIELD}_MAPPING_ANALYSIS.md`

---

### Pattern 2: Data Completeness Validation

**When to Use**: Verify all expected records are present

**Example**: Row count reconciliation, missing foreign keys

**Process**:
1. Compare source vs target row counts
2. Identify missing records
3. Analyze why records are missing
4. Verify filtering logic is correct

**Files Created**:
- `investigate_{issue}_completeness.sql`
- `{ISSUE}_COMPLETENESS_ANALYSIS.md`

---

### Pattern 3: Business Logic Validation

**When to Use**: Verify calculated fields, aggregations, and business rules

**Example**: Financial calculations, date calculations, status determinations

**Process**:
1. Document expected business logic
2. Create test queries that validate calculations
3. Compare calculated vs expected values
4. Identify discrepancies and root causes

**Files Created**:
- `investigate_{calculation}_logic.sql`
- `{CALCULATION}_LOGIC_ANALYSIS.md`

---

### Pattern 4: Data Quality Validation

**When to Use**: Check for data quality issues (NULLs, outliers, unexpected values)

**Example**: NULL provider_id, negative amounts, future dates

**Process**:
1. Identify data quality concerns
2. Query for problematic records
3. Analyze patterns and root causes
4. Recommend data quality improvements

**Files Created**:
- `investigate_{quality_issue}.sql`
- `{QUALITY_ISSUE}_ANALYSIS.md`

---

### Pattern 5: Business Rules to dbt Tests Mapping

**When to Use**: Convert validation findings into automated, repeatable dbt tests

**Example**: Mapping status category validation findings to `accepted_values` tests

**Process**:
1. **Document Business Rules**: Extract rules from investigation findings
   - What should the data look like?
   - What are the valid values?
   - What relationships must hold?
   - What calculations must be correct?

2. **Categorize Test Types**: Determine appropriate dbt test for each rule
   - **Simple Schema Tests**: `accepted_values`, `not_null`, `relationships`
   - **Expression Tests**: `dbt_utils.expression_is_true`, `dbt_expectations.*`
   - **Custom Macros**: Complex reusable business logic
   - **Data Tests**: Source comparison and reconciliation

3. **Create Test Definitions**: Add tests to YAML schema files
   - Include business rule descriptions in test configs
   - Set appropriate severity (error vs warn)
   - Add exclusion conditions where needed
   - Document business impact

4. **Implement Tests**: Add tests to model YAML files
   - Column-level tests for field validations
   - Model-level tests for cross-field validations
   - Custom test files for complex logic

5. **Verify Tests**: Run dbt tests to ensure they work correctly
   - Fix any test syntax errors
   - Adjust severity if needed
   - Document known exceptions

**Files Created**:
- `{MODEL}_BUSINESS_RULES_TO_DBT_TESTS.md` - Mapping document
- Updates to `models/{layer}/_{model_name}.yml` - Test definitions
- `macros/tests/domain/test_{rule_name}.sql` - Custom test macros (if needed)
- `tests/{layer}/{model_name}/test_{rule_name}.sql` - Data tests (if needed)

**Example**: See `validation/marts/fact_claim/BUSINESS_RULES_TO_DBT_TESTS.md` for complete example

---

## Where Mappings Are Documented

### 1. **YAML Schema Files** (Primary Documentation)
- **Location Pattern**: `models/{layer}/{model_name}.yml` or `models/{layer}/_{model_name}.yml`
- **Purpose**: Documents valid values, business rules, and data quality notes
- **Examples**:
  - `models/marts/_fact_claim.yml` - Status category mappings
  - `models/staging/opendental/_stg_opendental__appointment.yml` - Source value mappings

### 2. **Staging Model YAML Files** (Source System Mappings)
- **Location**: `models/staging/opendental/_stg_opendental__*.yml`
- **Purpose**: Documents raw values from OpenDental source system
- **Key Information**: Source codes, status values, type IDs

### 3. **Analysis Documentation** (Business Context)
- **Location**: `analysis/{table_name}/` directories
- **Purpose**: Business context and definitions
- **Examples**: `analysis/appointment/appt_business_logic.md`

### 4. **Validation Documentation** (Investigation Results)
- **Location**: `validation/{layer}/{model_name}/` directories
- **Purpose**: Investigation findings, root cause analysis, recommended actions

---

## Best Practices

### 1. **Documentation First**
- Always document current logic before investigating
- Include line numbers and code snippets
- Reference related documentation

### 2. **Systematic Investigation**
- Start with summary statistics
- Drill down into specific issues
- Include sample records for manual review
- Cross-reference with source data

### 3. **Clear Naming Conventions**
- Investigation SQL: `investigate_{issue_description}.sql`
- Analysis Document: `{ISSUE}_ANALYSIS.md` or `{ISSUE}_FINDINGS.md`
- Use descriptive, consistent names

### 4. **Comprehensive Queries**
- Include NULL checks
- Check edge cases
- Compare source vs target
- Provide diagnostic information

### 5. **Map Findings to Business Rules**
- Extract business rules from investigation findings
- Document rules clearly in analysis document
- Identify which rules can be automated as dbt tests
- Create test definitions in YAML schema files
- Reference `BUSINESS_RULES_TO_DBT_TESTS.md` for mapping examples

### 6. **Actionable Recommendations**
- Prioritize fixes (immediate vs long-term)
- Link to TODO.md for tracking
- Document business impact
- Include implementation steps

### 6. **Version Control**
- Keep validation queries in sync with model changes
- Update analysis documents as issues are resolved
- Document implementation dates

---

## Getting Started with Validation

### For New Validations

1. **Copy the Template**: Start with `validation/VALIDATION_TEMPLATE.md`
2. **Customize for Your Issue**: Fill in model name, fields, and investigation goals
3. **Create Investigation SQL**: Use the template structure to create your SQL file
4. **Follow the Process**: Use the 6-step process outlined in "How Validation Works" above
5. **Map to Business Rules**: Extract business rules from findings and map them to dbt tests (see Pattern 5)

### Quick Reference

- **Template**: `validation/VALIDATION_TEMPLATE.md` - Starting point for new validations
- **Process Guide**: This README - "How Validation Works" section
- **Example**: `validation/marts/fact_claim/` - Complete example of status mapping validation

---

## Related Documentation

- **Validation Template**: `validation/VALIDATION_TEMPLATE.md` - Starting point for new validations
- **Business Rules Mapping Example**: `validation/marts/fact_claim/BUSINESS_RULES_TO_DBT_TESTS.md` - Example of mapping validation findings to dbt tests
- **dbt Tests**: `tests/` - Automated dbt tests
- **Analysis Queries**: `analysis/` - Exploratory analysis queries
- **Model Documentation**: `models/{layer}/_{model_name}.yml` - Schema and test definitions

---

## Example: Status Category Mapping Validation

**Reference**: `validation/marts/fact_claim/` - Complete example of status category mapping validation

**Key Files**:
- `investigate_unknown_status_categories.sql` - Investigation queries
- `fact_claim_validation_plan.md` - Comprehensive validation plan
- `BUSINESS_RULES_TO_DBT_TESTS.md` - Mapping validation findings to dbt tests
- See `VALIDATION_TEMPLATE.md` for template used for similar validations

**Process Applied**:
1. ✅ Identified 'Unknown' status categories from test failures
2. ✅ Created investigation queries
3. ✅ Documented findings and root causes
4. ✅ Mapped findings to business rules and dbt tests
5. ✅ Implemented fixes (updated CASE statements)
6. ✅ Updated documentation (YAML files)
7. ✅ Added/updated dbt tests based on business rules
8. ✅ Verified fixes with re-run queries
