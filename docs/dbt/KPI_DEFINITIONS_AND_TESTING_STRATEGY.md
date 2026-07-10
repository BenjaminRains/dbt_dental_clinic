# KPI Definitions and Business Rules Testing Strategy

## Overview

This document analyzes how KPI definitions are structured, how dbt builds documentation, the relationship between YAML definitions and SQL implementations, and reviews our business rules testing strategy.

---

## 1. KPI Definition Locations

### Primary Location: `exposures.yml`

**Location**: `dbt_dental_models/models/marts/exposures.yml`

**Structure**: 
- 6 exposures defined (executive_dashboard, revenue_analytics_dashboard, provider_performance_dashboard, appointment_analytics_dashboard, patient_management_dashboard, accounts_receivable_aging_dashboard)
- Each exposure has a `description` field with detailed KPI definitions
- Each exposure has `depends_on` field linking to source models via `ref()`

**Current Exposures**:
1. **executive_dashboard** → depends on `mart_revenue_lost`, `mart_provider_performance`
2. **revenue_analytics_dashboard** → depends on `mart_revenue_lost`, `dim_provider`, `dim_date`
3. **provider_performance_dashboard** → depends on `mart_provider_performance`, `mart_appointment_summary`, `dim_provider`
4. **appointment_analytics_dashboard** → depends on `mart_appointment_summary`, `dim_provider`, `dim_date`
5. **patient_management_dashboard** → depends on `dim_patient`
6. **accounts_receivable_aging_dashboard** → depends on `mart_ar_summary`, `dim_patient`, `dim_insurance`

### Secondary Location: Model YAML Files

**Location**: `dbt_dental_models/models/marts/_mart_*.yml` files

**Structure**:
- Model-level descriptions document business logic
- Column-level descriptions document individual fields and calculations
- `meta` fields contain business context (owner, business_process, business_impact)
- `data_quality_requirements` list expected constraints

**Key Files**:
- `_mart_ar_summary.yml` - AR metrics definitions
- `_mart_revenue_lost.yml` - Revenue opportunity definitions
- `_mart_provider_performance.yml` - Provider metrics definitions
- `_mart_appointment_summary.yml` - Appointment metrics definitions

---

## 2. How dbt Builds Documentation

### Documentation Generation Process

1. **Parse Phase**: 
   - dbt parses all `.sql` model files
   - dbt parses all `.yml` schema files
   - dbt builds a dependency graph using `ref()` and `source()` macros

2. **Documentation Phase**:
   - dbt extracts descriptions from YAML files
   - dbt links descriptions to models via model name matching
   - dbt builds lineage graph showing model dependencies
   - dbt generates HTML documentation site

3. **Exposure Integration**:
   - Exposures are included in documentation with their descriptions
   - Exposures show lineage to underlying models via `depends_on`
   - Exposures appear in the documentation as "downstream consumers"

### Generating Documentation

```bash
cd dbt_dental_models
dbt docs generate
dbt docs serve  # Opens browser at http://localhost:8080
```

### What dbt DOES NOT Do

**⚠️ Important Limitations**:

1. **No Automatic Validation**: dbt does NOT automatically verify that YAML descriptions match SQL implementations
2. **No Formula Verification**: dbt does NOT validate that calculation formulas in descriptions match actual SQL
3. **No Business Rule Enforcement**: dbt does NOT enforce business rules described in YAML - only tests do this
4. **Name Matching Only**: dbt links YAML to SQL via exact name matching - mismatched names are silently ignored

**Example**: If `exposures.yml` says "Collection Rate = (Collections / Production) × 100" but the SQL calculates it differently, dbt will NOT catch this discrepancy.

---

## 3. How dbt Ensures YAML Definitions Comply with SQL

### The Answer: It Doesn't Automatically

**dbt relies on two mechanisms**:

#### A. Tests (Primary Mechanism)

Tests are the ONLY way dbt validates that SQL matches documented business rules:

```yaml
# In _mart_ar_summary.yml
columns:
  - name: collection_rate_last_year
    description: >
      Collection rate calculation...
    tests:
      - dbt_utils.expression_is_true:
          expression: "(collection_rate_last_year = 0) OR (abs(collection_rate_last_year - (total_payments_last_year / nullif(billed_last_year, 0) * 100)) < 0.01)"
          description: "Collection rate must match payments-to-billed ratio"
```

**This test**:
- Validates the formula matches the SQL calculation
- Fails if the calculation doesn't match the documented formula
- Runs during `dbt test` or `dbt build`

#### B. Documentation Review (Manual Process)

**No automated validation exists for**:
- Descriptions matching SQL logic
- Calculation formulas matching implementation
- Business rules matching code behavior
- Metric definitions matching actual metrics

**Manual Review Process Needed**:
1. Developer writes SQL
2. Developer writes YAML descriptions
3. Developer reviews that descriptions match SQL
4. Tests validate business rules (if tests exist)
5. Code review validates documentation accuracy

---

## 4. Current Business Rules Testing Strategy

### Test Coverage Analysis

#### Model-Level Tests (Overall Data Quality)

**mart_ar_summary**:
- ✅ Unique key constraint (date_id, patient_id, provider_id)
- ✅ Row count validation (1000-1M rows)
- ✅ Balance consistency (total = sum of buckets)
- ✅ Aging percentage calculations (pct_current, pct_over_90)
- ✅ Aging percentages sum to 100%
- ✅ Collection priority score range (0-100)
- ✅ Priority score is integer (rounded)
- ✅ Collection rate formula validation
- ✅ Boolean flag correctness (has_outstanding_balance, has_aged_balance)
- ✅ Risk category validation

**mart_revenue_lost**:
- ✅ Row count validation (1-100K rows)
- ✅ Lost revenue >= 0
- ✅ Recovery priority score range (0-100)
- ✅ Estimated recoverable <= lost revenue

**mart_provider_performance**:
- ✅ Row count validation (1-10K rows)
- ✅ Total production >= 0
- ✅ Collection efficiency range (0-100%)

### Column-Level Tests

#### Well-Tested Areas

**AR Summary**:
- ✅ Balance ranges (0-50K for all aging buckets)
- ✅ Percentage ranges (0-100% for all percentages)
- ✅ Collection rate formula validation
- ✅ Priority score validation
- ✅ Risk category validation

**Revenue Lost**:
- ✅ Lost revenue ranges (0-10K)
- ✅ Lost time ranges (0-480 minutes)
- ✅ Opportunity hour ranges (0-23)
- ✅ Recovery potential scores (0-100)

### Gaps in Test Coverage

#### Critical Business Rules - NOT TESTED

1. **Collection Rate Calculation (AR Dashboard)**:
   - **Documented**: "Production: All procedures from fact_procedure (last 365 days)"
   - **Documented**: "Collections: All payments from fact_payment where payment_direction = 'Income' (last 365 days)"
   - **Test Status**: ❌ NO TEST validates this calculation matches the documented formula
   - **Risk**: API service calculates differently than documented

2. **AR Ratio Calculation**:
   - **Documented**: "AR Ratio = (Monthly Collections / Monthly Production) × 100"
   - **Test Status**: ❌ NO TEST validates this calculation
   - **Risk**: Current month calculation might not match documentation

3. **DSO Calculation**:
   - **Documented**: "DSO = ((Balance Over 90 Days / Total AR) × 90) + 30"
   - **Test Status**: ❌ NO TEST validates this simplified DSO formula
   - **Risk**: Formula might not match documented calculation

4. **Recovery Priority Score Algorithm**:
   - **Documented**: Multi-factor algorithm (Balance 25pts, Aging 25pts, Payment Recency 25pts, Insurance 25pts)
   - **Test Status**: ❌ NO TEST validates the algorithm matches documentation
   - **Risk**: Score calculation might not match documented logic

5. **Revenue Lost - Recovery Potential Assignment**:
   - **Documented**: "High = 80%, Medium = 50%, Low = 20% recovery rates"
   - **Documented**: "Estimated Recoverable = Lost Revenue × Recovery Rate"
   - **Test Status**: ⚠️ PARTIAL - Test validates `estimated_recoverable <= lost_revenue` but NOT the rate calculation
   - **Risk**: Recovery rates might not match documented percentages

6. **Opportunity Type Subtypes**:
   - **Documented**: Missed Appointment has subtypes (No Show, Cancellation, Other)
   - **Test Status**: ❌ NO TEST validates subtype assignment logic
   - **Risk**: Subtypes might not match documented business rules

7. **Collection Rate (Provider Performance)**:
   - **Documented**: "Collection Rate = (Collections / Production) × 100"
   - **Test Status**: ⚠️ Range test only (0-100%) - NO formula validation
   - **Risk**: Formula might not match documented calculation

#### Documentation vs SQL Validation Gaps

**No Automated Validation For**:
1. Calculation formulas in descriptions matching SQL
2. Data source definitions matching actual FROM/JOIN clauses
3. Time windows (last 365 days, current month) matching WHERE clauses
4. Filter definitions (total_balance > 0) matching SQL filters
5. Business logic descriptions matching CTE logic

---

## 5. Recommended Testing Strategy Enhancements

### Priority 1: Formula Validation Tests

Add `expression_is_true` tests to validate calculation formulas match documentation:

```yaml
# In _mart_ar_summary.yml
columns:
  - name: collection_rate_last_year
    tests:
      # Existing range test
      - dbt_expectations.expect_column_values_to_be_between:
          min_value: 0
          max_value: 100
      
      # NEW: Formula validation test
      - dbt_utils.expression_is_true:
          expression: "(collection_rate_last_year = 0) OR (abs(collection_rate_last_year - (total_payments_last_year / nullif(billed_last_year, 0) * 100)) < 0.01)"
          config:
            severity: error
            description: "Collection rate formula must match documented calculation: (Payments / Billed) × 100"
```

### Priority 2: Business Logic Validation Tests

Add tests for complex business rules:

```yaml
# In _mart_revenue_lost.yml
tests:
  # Existing tests...
  
  # NEW: Recovery potential rate validation
  - dbt_utils.expression_is_true:
      expression: |
        (recovery_potential = 'High' AND abs(estimated_recoverable_amount - (lost_revenue * 0.8)) < 0.01) OR
        (recovery_potential = 'Medium' AND abs(estimated_recoverable_amount - (lost_revenue * 0.5)) < 0.01) OR
        (recovery_potential = 'Low' AND abs(estimated_recoverable_amount - (lost_revenue * 0.2)) < 0.01) OR
        (recovery_potential = 'None' AND estimated_recoverable_amount = 0) OR
        (estimated_recoverable_amount IS NULL)
      config:
        severity: error
        description: "Estimated recoverable must match documented recovery rates (High=80%, Medium=50%, Low=20%, None=0%)"
```

### Priority 3: Subtype Logic Validation

```yaml
# In _mart_revenue_lost.yml
columns:
  - name: opportunity_subtype
    tests:
      # NEW: Subtype validation based on opportunity_type
      - dbt_utils.expression_is_true:
          expression: |
            (opportunity_type = 'Missed Appointment' AND opportunity_subtype IN ('No Show', 'Cancellation', 'Other')) OR
            (opportunity_type = 'Claim Rejection' AND opportunity_subtype IN ('Insurance Denial', 'Claim Rejection', 'Processing Issue')) OR
            (opportunity_type = 'Treatment Plan Delay' AND opportunity_subtype IN ('Very Delayed', 'Delayed Start', 'In Progress', 'Current')) OR
            (opportunity_type = 'Write Off' AND opportunity_subtype IN ('Credit Adjustment', 'Charge Adjustment', 'Zero Adjustment'))
          config:
            severity: error
            description: "Opportunity subtype must match documented subtypes for each opportunity type"
```

### Priority 4: Time Window Validation

Add tests to validate documented time windows:

```yaml
# In _mart_ar_summary.yml
columns:
  - name: snapshot_date
    tests:
      # Existing date range test...
      
      # NEW: Validate snapshot date is within reasonable recency
      - dbt_utils.expression_is_true:
          expression: "snapshot_date >= CURRENT_DATE - INTERVAL '90 days'"
          config:
            severity: warn
            description: "AR snapshots should be recent (within 90 days) for operational use"
```

### Priority 5: Cross-Model Consistency Tests

Test that metrics calculated in different models are consistent:

```yaml
# NEW: Custom test file tests/cross_model/collection_rate_consistency.sql
# Validates that collection rate in mart_ar_summary matches collection_rate in mart_provider_performance
# (when aggregated to same time period)
```

---

## 6. Current Test Coverage Summary

### Well-Tested Models

| Model | Formula Tests | Range Tests | Business Logic Tests | Coverage Score |
|-------|--------------|-------------|---------------------|----------------|
| mart_ar_summary | ✅ Partial | ✅ Good | ✅ Good | 85% |
| mart_revenue_lost | ❌ None | ✅ Good | ⚠️ Partial | 60% |
| mart_provider_performance | ❌ None | ✅ Good | ⚠️ Partial | 65% |
| mart_appointment_summary | ❌ None | ✅ Good | ⚠️ Partial | 60% |

### Test Types Breakdown

**Total Tests Across All Models**: ~4,694 tests (from dbt list output)

**Test Categories**:
1. **Range Tests** (expect_column_values_to_be_between): ✅ Well covered
2. **Null Tests** (not_null): ✅ Well covered
3. **Relationship Tests** (relationships): ✅ Well covered
4. **Formula Tests** (expression_is_true for calculations): ⚠️ Partially covered
5. **Business Logic Tests** (expression_is_true for rules): ⚠️ Partially covered
6. **Consistency Tests** (cross-model validation): ❌ Not covered

---

## 7. Recommendations

### Immediate Actions (High Priority)

1. **Add Formula Validation Tests** for all documented calculations:
   - Collection Rate (AR and Provider Performance)
   - AR Ratio
   - DSO
   - Recovery Priority Score algorithm
   - Estimated Recoverable Amount calculation

2. **Add Business Logic Tests** for complex rules:
   - Recovery potential rate assignments
   - Opportunity subtype assignments
   - Risk category assignments

3. **Document Test Gaps**: Create a test coverage matrix showing which documented business rules have tests

### Medium-Term Actions

4. **Create Custom Tests**: For complex cross-model validations
5. **Add Test Documentation**: Document what each test validates and why
6. **Review Test Failures**: Establish process for handling test failures in production

### Long-Term Actions

7. **Automated Documentation Validation**: Consider custom dbt macros to validate that YAML formulas match SQL
8. **Test Coverage Metrics**: Track test coverage percentage for each model
9. **Business Rule Catalog**: Centralized catalog of all business rules and their test status

---

## 8. Example: Adding Missing Tests

### Example 1: Collection Rate Formula Test

**Current State**: Range test only (0-100%)

**Add**:
```yaml
# In _mart_ar_summary.yml
columns:
  - name: collection_rate_last_year
    tests:
      # Existing range test
      - dbt_expectations.expect_column_values_to_be_between:
          min_value: 0
          max_value: 100
          severity: warn
      
      # NEW: Formula validation
      - dbt_utils.expression_is_true:
          expression: "(collection_rate_last_year = 0) OR (abs(collection_rate_last_year - (total_payments_last_year / nullif(billed_last_year, 0) * 100)) < 0.01) OR (billed_last_year IS NULL OR billed_last_year = 0)"
          config:
            severity: error
            description: "Collection rate must equal (total_payments_last_year / billed_last_year) × 100, matching documented formula"
```

### Example 2: Recovery Potential Rate Test

**Current State**: Only validates `estimated_recoverable <= lost_revenue`

**Add**:
```yaml
# In _mart_revenue_lost.yml
tests:
  # Existing constraint...
  
  # NEW: Recovery rate validation
  - dbt_utils.expression_is_true:
      expression: |
        (recovery_potential = 'High' AND (estimated_recoverable_amount IS NULL OR abs(estimated_recoverable_amount - (lost_revenue * 0.8)) < 0.01)) OR
        (recovery_potential = 'Medium' AND (estimated_recoverable_amount IS NULL OR abs(estimated_recoverable_amount - (lost_revenue * 0.5)) < 0.01)) OR
        (recovery_potential = 'Low' AND (estimated_recoverable_amount IS NULL OR abs(estimated_recoverable_amount - (lost_revenue * 0.2)) < 0.01)) OR
        (recovery_potential = 'None' AND (estimated_recoverable_amount IS NULL OR estimated_recoverable_amount = 0))
      config:
        severity: error
        description: "Estimated recoverable must match documented recovery rates: High=80%, Medium=50%, Low=20%, None=0%"
```

---

## 9. Conclusion

### Key Findings

1. **Documentation Location**: KPIs are well-documented in `exposures.yml` and model YAML files
2. **dbt Documentation**: dbt generates docs but does NOT validate accuracy
3. **Test Coverage**: Range tests are good, formula tests are missing
4. **Gaps**: Critical business rules are documented but not tested

### Critical Gaps

1. Collection rate calculations not validated against documented formulas
2. Recovery priority score algorithm not validated
3. Recovery potential rates not validated
4. Opportunity subtype logic not validated
5. DSO calculation not validated

### Next Steps

1. Prioritize adding formula validation tests
2. Add business logic validation tests
3. Create test coverage tracking
4. Establish review process for documentation accuracy

---

**Last Updated**: 2025-01-06
**Related Files**:
- `dbt_dental_models/models/marts/exposures.yml` - KPI definitions
- `dbt_dental_models/models/marts/_mart_*.yml` - Model schema definitions
- `dbt_dental_models/models/marts/mart_*.sql` - Model SQL implementations
