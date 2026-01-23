# Business Rules to dbt Tests - Discussion Document

## Overview

This document analyzes the validation plan for `fact_claim` and identifies which business rules can be converted to automated dbt tests in `_fact_claim.yml`. The goal is to move from manual validation queries to automated, repeatable tests that run with every dbt run.

---

## Current Test Coverage

### Already Tested in `_fact_claim.yml`:

1. **Composite Key Uniqueness** ✅
   - `dbt_utils.unique_combination_of_columns: [claim_id, procedure_id, claim_procedure_id]`

2. **Not Null Constraints** ✅
   - `claim_id`, `procedure_id`, `claim_procedure_id`, `patient_id`, `claim_date`, `claim_status`, `claim_type`, `claim_procedure_status`
   - All financial fields: `billed_amount`, `allowed_amount`, `paid_amount`, `write_off_amount`, `adjustment_write_off_amount`, `patient_responsibility`

3. **Accepted Values** ✅
   - `claim_status`: ['S', 'W', 'R', 'H']
   - `claim_type`: ['P', 'S', 'Other']
   - `claim_procedure_status`: [0, 1, 4]

4. **Value Ranges** ✅
   - All financial fields have `dbt_expectations.expect_column_values_to_be_between` with min=0, max=10000-20000

5. **Referential Integrity** ✅
   - Relationships to `dim_patient`, `dim_provider`, `dim_insurance`, `dim_procedure`

---

## Business Rules to Convert to Tests

### Category 1: Simple Schema Tests (Easy Wins)

These can be added directly to `_fact_claim.yml` using standard dbt tests:

#### 1.1 Payment Status Category Values
**Validation**: Query 3.1 - `payment_status_category` should be one of: 'Paid', 'Pending', 'Denied', 'Rejected', 'Unknown', 'Pre-auth'

**Test Type**: `accepted_values`
```yaml
- name: payment_status_category
  tests:
    - accepted_values:
        values: ['Paid', 'Pending', 'Denied', 'Rejected', 'Unknown', 'Pre-auth']
        config:
          severity: error
```

**Status**: ✅ Ready to add

---

#### 1.2 Payment Completion Status Values
**Validation**: Query 3.2 - `payment_completion_status` should be one of: 'Write-off', 'Patient Balance', 'Fully Paid', 'Partial Payment'

**Test Type**: `accepted_values`
```yaml
- name: payment_completion_status
  tests:
    - accepted_values:
        values: ['Write-off', 'Patient Balance', 'Fully Paid', 'Partial Payment']
        config:
          severity: error
```

**Status**: ✅ Ready to add

---

#### 1.3 Billing Status Category Values
**Validation**: `billing_status_category` should be one of: 'Billable', 'Non-Billable', 'Unknown'

**Test Type**: `accepted_values`
```yaml
- name: billing_status_category
  tests:
    - accepted_values:
        values: ['Billable', 'Non-Billable', 'Unknown']
        config:
          severity: error
```

**Status**: ✅ Ready to add

---

#### 1.4 EOB Documentation Status Values
**Validation**: `eob_documentation_status` should be one of: 'Documented', 'Payment Without EOB', 'No Documentation'

**Test Type**: `accepted_values`
```yaml
- name: eob_documentation_status
  tests:
    - accepted_values:
        values: ['Documented', 'Payment Without EOB', 'No Documentation']
        config:
          severity: error
```

**Status**: ✅ Ready to add

---

### Category 2: dbt_expectations Tests (Medium Complexity)

These use the `dbt_expectations` package for more advanced validations:

#### 2.1 Financial Balance Validation
**Validation**: Query 4.1 - `billed_amount = paid_amount + write_off_amount + patient_responsibility` (with exclusions)

**Test Type**: `dbt_expectations.expect_column_pair_values_A_to_be_greater_than_or_equal_to_B`
**Challenge**: Need to exclude `claim_id = 0` and `patient_responsibility = -1.0`

**Proposed Test**:
```yaml
models:
  - name: fact_claim
    tests:
      - dbt_expectations.expect_column_pair_values_A_to_be_greater_than_or_equal_to_B:
          column_A: billed_amount
          column_B: "paid_amount + write_off_amount + patient_responsibility"
          row_condition: "claim_id != 0 AND patient_responsibility != -1.0"
          config:
            severity: warn
            description: "Financial amounts should balance (excluding pre-auth claims and placeholder values)"
```

**Alternative**: Custom macro test (see Category 3)

**Status**: ⚠️ Needs custom macro for complex expression

---

#### 2.2 Payment Amount Relationships
**Validation**: Query 4.2 - `paid_amount <= allowed_amount <= billed_amount` (with exclusions)

**Test Type**: `dbt_expectations.expect_column_pair_values_A_to_be_greater_than_or_equal_to_B`

**Proposed Tests**:
```yaml
- name: paid_amount
  tests:
    - dbt_expectations.expect_column_pair_values_A_to_be_greater_than_or_equal_to_B:
        column_A: allowed_amount
        column_B: paid_amount
        row_condition: "claim_id != 0 AND allowed_amount != -1.0"
        config:
          severity: warn
          description: "Paid amount should not exceed allowed amount (excluding pre-auth and placeholder values)"
    
    - dbt_expectations.expect_column_pair_values_A_to_be_greater_than_or_equal_to_B:
        column_A: billed_amount
        column_B: allowed_amount
        row_condition: "claim_id != 0 AND allowed_amount != -1.0 AND billed_amount > 0"
        config:
          severity: warn
          description: "Allowed amount should not exceed billed amount (excluding pre-auth, placeholder, and zero-billed)"
```

**Status**: ✅ Can be added with row_condition

---

#### 2.3 Date Range Validations
**Validation**: Query 2.3 - Dates should be within reasonable ranges

**Test Type**: `dbt_expectations.expect_column_values_to_be_between`

**Proposed Tests**:
```yaml
- name: claim_date
  tests:
    - dbt_expectations.expect_column_values_to_be_between:
        min_value: "'2020-01-01'"
        max_value: "CURRENT_DATE + INTERVAL '1 year'"
        row_condition: "claim_id != 0"
        config:
          severity: warn
          description: "Claim dates should be within reasonable range (excluding pre-auth claims which may have future dates)"
```

**Status**: ✅ Can be added

---

#### 2.4 Payment Days Calculation
**Validation**: Query 3.3 - `payment_days_from_claim` should equal `check_date - claim_date`

**Test Type**: Custom expression test

**Proposed Test**:
```yaml
models:
  - name: fact_claim
    tests:
      - dbt_utils.expression_is_true:
          expression: |
            CASE 
              WHEN check_date IS NULL THEN payment_days_from_claim IS NULL
              WHEN check_date IS NOT NULL THEN payment_days_from_claim = (check_date - claim_date)
              ELSE true
            END
          config:
            severity: error
            description: "Payment days calculation should match check_date - claim_date"
```

**Status**: ✅ Can be added

---

### Category 3: Custom Test Macros (Complex Business Logic)

These require creating custom test macros in `macros/tests/`:

#### 3.1 Financial Balance Validation (Complex)
**Validation**: Query 4.1 - Complex balance equation with exclusions

**Custom Macro**: `macros/tests/domain/test_financial_balance.sql`
```sql
{% test test_financial_balance(model) %}
    SELECT 
        claim_id,
        procedure_id,
        claim_procedure_id,
        billed_amount,
        paid_amount,
        write_off_amount,
        adjustment_write_off_amount,
        patient_responsibility,
        (paid_amount + write_off_amount + patient_responsibility) as calculated_total,
        ABS(billed_amount - (paid_amount + write_off_amount + patient_responsibility)) as balance_difference
    FROM {{ model }}
    WHERE claim_id != 0  -- Exclude pre-auth/draft claims
        AND patient_responsibility != -1.0  -- Exclude placeholder values
        AND ABS(billed_amount - (paid_amount + write_off_amount + patient_responsibility)) > 0.01
{% endtest %}
```

**Usage in YAML**:
```yaml
models:
  - name: fact_claim
    tests:
      - test_financial_balance
```

**Status**: ⚠️ Needs custom macro creation

---

#### 3.2 Payment Status Category Logic
**Validation**: Query 3.1 - Verify `payment_status_category` matches business rules

**Custom Macro**: `macros/tests/domain/test_payment_status_category_logic.sql`
```sql
{% test test_payment_status_category_logic(model) %}
    SELECT 
        claim_id,
        paid_amount,
        claim_status,
        claim_id,
        payment_status_category
    FROM {{ model }}
    WHERE (paid_amount > 0 AND payment_status_category != 'Paid')
        OR (claim_status = 'Denied' AND payment_status_category != 'Denied')
        OR (claim_status = 'Rejected' AND payment_status_category != 'Rejected')
        OR (claim_status = 'Submitted' AND payment_status_category != 'Pending')
        OR (claim_status = 'R' AND paid_amount = 0 AND payment_status_category != 'Pending')
        OR (claim_status = 'S' AND paid_amount = 0 AND payment_status_category != 'Pending')
        OR (claim_status = 'W' AND paid_amount = 0 AND payment_status_category != 'Pending')
        OR (claim_status = 'H' AND paid_amount = 0 AND payment_status_category != 'Pending')
        OR (claim_id = 0 AND payment_status_category != 'Pre-auth')
{% endtest %}
```

**Status**: ⚠️ Needs custom macro creation (complex logic)

---

#### 3.3 Payment Completion Status Logic
**Validation**: Query 3.2 - Verify `payment_completion_status` matches business rules

**Custom Macro**: Similar to 3.2, but for payment completion status

**Status**: ⚠️ Needs custom macro creation

---

#### 3.4 Collection Rate Validation
**Validation**: Query 4.3 - Collection rate should be <= 100% (with exceptions)

**Custom Macro**: `macros/tests/domain/test_collection_rate.sql`
```sql
{% test test_collection_rate(model) %}
    SELECT 
        claim_id,
        billed_amount,
        paid_amount,
        (paid_amount / NULLIF(billed_amount, 0) * 100) as collection_rate
    FROM {{ model }}
    WHERE claim_id != 0
        AND billed_amount > 0
        AND (paid_amount / NULLIF(billed_amount, 0) * 100) > 100
        AND (paid_amount / NULLIF(billed_amount, 0) * 100) <= 150  -- Allow up to 150% for overpayments
{% endtest %}
```

**Status**: ⚠️ Needs custom macro creation

---

### Category 4: Data Tests (Source Comparison)

These require comparing against source data and should remain as data tests:

#### 4.1 Row Count Reconciliation
**Validation**: Query 1.1 - Compare row counts between source and fact

**Why Data Test**: Requires comparing against `raw.claimproc` and `raw.claim` tables
**Location**: `tests/marts/fact_claim/test_row_count_reconciliation.sql`

**Status**: ✅ Keep as data test

---

#### 4.2 Financial Amount Accuracy
**Validation**: Query 2.1 - Compare financial amounts between source and fact

**Why Data Test**: Requires comparing against `raw.claimproc` columns
**Location**: `tests/marts/fact_claim/test_financial_amount_accuracy.sql`

**Status**: ✅ Keep as data test

---

#### 4.3 Status Value Accuracy
**Validation**: Query 2.2 - Compare status values between source and fact

**Why Data Test**: Requires comparing against `raw.claim` and `raw.claimproc`
**Location**: `tests/marts/fact_claim/test_status_value_accuracy.sql`

**Status**: ✅ Keep as data test

---

### Category 5: Not Suitable for Tests (Investigation/Reporting)

These are diagnostic queries that should remain in validation plan:

#### 5.1 Diagnostic Queries
- Query 1.1.1-1.1.5: Root cause analysis (already resolved)
- Query 4.2.1-4.2.2h: Investigation queries for overpayments
- Query 10.3.1-10.3.4: Diagnostic queries for unknown status

**Status**: ✅ Keep as diagnostic queries in validation plan

---

## Recommended Implementation Order

### Phase 1: Quick Wins (Simple Schema Tests)
1. Add `accepted_values` tests for calculated status fields (1.1-1.4)
2. Add date range validations (2.3)
3. Add payment amount relationship tests (2.2)

**Estimated Time**: 1-2 hours
**Impact**: High - Catches invalid enum values immediately

---

### Phase 2: Expression Tests
1. Add payment days calculation test (2.4)
2. Add simple expression tests for business logic

**Estimated Time**: 1 hour
**Impact**: Medium - Validates calculated fields

---

### Phase 3: Custom Macros
1. Create `test_financial_balance` macro (3.1)
2. Create `test_payment_status_category_logic` macro (3.2)
3. Create `test_payment_completion_status_logic` macro (3.3)
4. Create `test_collection_rate` macro (3.4)

**Estimated Time**: 4-6 hours
**Impact**: High - Validates complex business rules automatically

---

### Phase 4: Data Tests
1. Create `test_row_count_reconciliation` data test (4.1)
2. Create `test_financial_amount_accuracy` data test (4.2)
3. Create `test_status_value_accuracy` data test (4.3)

**Estimated Time**: 2-3 hours
**Impact**: High - Validates data accuracy against source

---

## Test Organization Structure

```
dbt_dental_models/
├── models/
│   └── marts/
│       └── _fact_claim.yml          # Schema tests (Category 1 & 2)
├── macros/
│   └── tests/
│       └── domain/
│           ├── test_financial_balance.sql
│           ├── test_payment_status_category_logic.sql
│           ├── test_payment_completion_status_logic.sql
│           └── test_collection_rate.sql
└── tests/
    └── marts/
        └── fact_claim/
            ├── test_row_count_reconciliation.sql
            ├── test_financial_amount_accuracy.sql
            └── test_status_value_accuracy.sql
```

---

## Considerations

### Exclusion Patterns
Many tests need to exclude:
- `claim_id = 0` (pre-authorization/draft claims)
- `patient_responsibility = -1.0` (placeholder for undetermined)
- `allowed_amount = -1.0` (placeholder for undetermined)

**Recommendation**: Create reusable macros:
- `exclude_preauth_claims()`: Returns `claim_id != 0`
- `exclude_placeholder_values()`: Returns exclusion conditions

### Severity Levels
- **Error**: Data integrity issues (nulls, invalid values, broken relationships)
- **Warn**: Business rule violations that may be legitimate (overpayments, future dates for pre-auth)

### Test Performance
- Schema tests run on every row - use `row_condition` to limit scope
- Data tests compare against source - may be slower, run less frequently
- Custom macros can be optimized with WHERE clauses

---

## Next Steps

1. **Review this document** with team
2. **Prioritize** which tests to implement first
3. **Create custom macros** for complex business logic
4. **Add schema tests** to `_fact_claim.yml`
5. **Create data tests** in `tests/marts/fact_claim/`
6. **Run tests** and verify they catch expected issues
7. **Document** any legitimate exceptions in test descriptions

---

## Questions for Discussion

1. Should we create reusable exclusion macros, or inline the conditions?
2. What severity level for overpayment tests (warn vs error)?
3. Should data tests run in CI/CD or only on-demand?
4. How do we handle tests that currently have known failures (e.g., overpayments)?
5. Should we add tests for the new `adjustment_write_off_amount` field?
