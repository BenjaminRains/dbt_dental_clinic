# Test Quality Review - dbt_dental_clinic
## Self-Review Checklist for Data Testing Architecture

---

## Current State Snapshot (Baseline Assessment)

**✅ What's Working:**
- 12 singular tests in `tests/staging/` and `tests/intermediate/`
- 25+ custom test macros organized in `macros/tests/` (data_quality, domain, fee)
- Comprehensive YAML tests (e.g., `fact_claim.yml` has extensive column validation)
- Proper use of severity levels (`warn` vs `error`)
- Business-aware tests (e.g., `payment_validation_rules.sql`)

**❌ Critical Gaps:**
- **NO incremental guards** for 10+ incremental models (staging layer)
- **NO temporal foreign key tests** (claims should validate against policy windows)
- **NO test tagging** (can't run selective test suites)
- **NO CI pipeline** for automated testing
- **Inconsistent test organization** (need domain-based structure)

---

## Priority Domains (P0 = Highest Business Impact)

**P0 domains** are your highest-priority business areas that require the most rigorous testing:
- **Claims & Insurance** - Revenue recognition, billing accuracy
- **Payments** - Financial transactions, reconciliation
- **Appointments & Scheduling** - Patient care delivery, provider utilization
- **Procedures** - Clinical documentation, billing foundation

**P1 domains** are important but lower risk:
- AR Analysis, Collections, Communications, Logging

---

## Review Checklist by Priority

### 🔴 **PRIORITY 1: Incremental Model Guards** (MUST FIX)

**Problem:** 10+ incremental staging models have NO incremental-specific tests.

**Models to audit:**
```
stg_opendental__statement.sql
stg_opendental__task.sql
stg_opendental__tasknote.sql
stg_opendental__timeadjust.sql
stg_opendental__treatplan.sql
... and 5 more
```

**For EACH incremental model, verify:**
- [ ] YAML has `unique` + `not_null` on primary key
- [ ] Singular test: No duplicate keys post-merge
- [ ] Singular test: Target `_updated_at` ≥ source `_updated_at` (freshness)
- [ ] Singular test: No stale records (latest record per key only)
- [ ] Tagged: `incremental_guard`

**Example test structure:**
```sql
-- tests/incremental/duplicate_keys__stg_opendental__statement.sql
-- Purpose: Ensure no duplicate statement_ids after incremental merge

{% if is_incremental() %}
SELECT 
    statement_id,
    COUNT(*) as duplicate_count
FROM {{ ref('stg_opendental__statement') }}
GROUP BY statement_id
HAVING COUNT(*) > 1
{% endif %}
```

---

### 🟡 **PRIORITY 2: Temporal Foreign Key Tests** (ADD SOON)

**Problem:** Time-bounded relationships not validated (claims must fall within policy coverage dates).

**Key relationships to test:**
- [ ] `fact_claim.claim_date` within `dim_insurance.effective_date` to `termination_date`
- [ ] `fact_appointment.appointment_date` within provider schedule availability
- [ ] `fact_procedure.procedure_date` within patient active status period

**Note where to add in claims:**
```sql
-- tests/claims/temporal_fk__fact_claim__policy_coverage.sql
-- Purpose: Validate claim dates fall within insurance policy coverage window

SELECT 
    c.claim_id,
    c.claim_date,
    i.effective_date,
    i.termination_date,
    c.insurance_plan_id
FROM {{ ref('fact_claim') }} c
LEFT JOIN {{ ref('dim_insurance') }} i 
    ON c.insurance_plan_id = i.insurance_plan_id
WHERE c.claim_date < i.effective_date 
   OR c.claim_date >= COALESCE(i.termination_date, '9999-12-31')
```

**Best practices for temporal tests:**
- Use half-open intervals: `>= start_date AND < end_date`
- Handle NULL termination dates (treat as active/open-ended)
- Cast DATE vs TIMESTAMP explicitly for comparison
- Tag: `temporal_fk`

---

### 🟢 **PRIORITY 3: Test Organization & Tagging** (IMPROVE STRUCTURE)

**Current state:** Tests scattered in `tests/staging/` and `tests/intermediate/`

**Target structure:**
```
tests/
├── claims/                    # Domain-specific tests
│   ├── temporal_fk__fact_claim__policy_coverage.sql
│   └── business_rule__claim__payment_amounts.sql
├── payments/
│   ├── business_rule__payment__type_validation.sql
│   └── business_rule__paysplit__allocation_sum.sql
├── scheduling/
│   ├── overlap__appointment__schedule_conflicts.sql
│   └── temporal_fk__appointment__provider_schedule.sql
├── incremental/               # Cross-domain incremental guards
│   ├── duplicate_keys__stg_opendental__statement.sql
│   ├── freshness__stg_opendental__task.sql
│   └── stale_records__stg_opendental__timeadjust.sql
└── data_quality/              # Generic quality checks
    └── volume_drift__fact_tables.sql
```

**Naming convention:**
```
<intent>__<model>__<description>.sql

Examples:
- temporal_fk__fact_claim__policy_window.sql
- duplicate_keys__stg_opendental__payment.sql
- business_rule__paysplit__amount_positive.sql
- overlap__appointment__schedule_conflict.sql
```

**Tagging strategy:**
```yaml
# In test config or dbt_project.yml
tests:
  dbt_dental_models:
    claims:
      +tags: ["quality", "claims", "p0"]
    payments:
      +tags: ["quality", "payments", "p0"]
    incremental:
      +tags: ["quality", "incremental_guard", "p0"]
    scheduling:
      +tags: ["quality", "scheduling", "p0"]
```

**Usage:**
```bash
# Run only incremental guards
dbt test --select tag:incremental_guard

# Run only P0 domain tests
dbt test --select tag:p0

# Run claims-specific tests
dbt test --select tag:claims
```

---

### 🔵 **PRIORITY 4: CI & Automation** (FUTURE ENHANCEMENT)

**Current state:** No CI pipeline

**Recommended setup:**

**.github/workflows/dbt_pr.yml** (PR checks):
```yaml
- name: Run dbt tests on changed models
  run: |
    dbt deps
    dbt build --select state:modified+ --fail-fast
    dbt test --select tag:incremental_guard
```

**.github/workflows/dbt_nightly.yml** (Full validation):
```yaml
- name: Run all tests
  run: |
    dbt test --select tag:p0
    dbt test --select tag:incremental_guard
```

**SQLFluff configuration:** Ensure `.sqlfluff` handles dbt/Jinja syntax

---

## Test Coverage by Layer

### **Staging Models (`stg_*`)**
- [ ] Column validation: `not_null` on business-required fields
- [ ] Enum validation: `accepted_values` for status codes
- [ ] Source freshness: Configured in `_sources.yml`
- [ ] **Incremental guards** (see Priority 1 above)

### **Intermediate Models (`int_*`)**
- [ ] Primary key: `not_null` + `unique`
- [ ] Foreign keys: `relationships` to dimensions
- [ ] Business invariants: `expression_is_true` (e.g., `end_date >= start_date`)
- [ ] Cross-row rules: Singular tests for domain logic

### **Marts (`fact_*`, `dim_*`)**
- [ ] Composite keys: `unique_combination_of_columns` (e.g., fact_claim)
- [ ] Foreign keys: `relationships` to all dimensions
- [ ] **Temporal FKs** (see Priority 2 above)
- [ ] Amount validations: `expect_column_values_to_be_between`
- [ ] Status enums: `accepted_values`

---

## Quality Standards for All Tests

### **Flakiness Prevention**
- [ ] No `now()` or `current_date` - use `var('max_valid_date')` instead
- [ ] Deterministic ordering in tests (no random sampling)
- [ ] Tests return violating rows with identifying keys for easy debugging

### **Test Output Quality**
- [ ] Each singular test includes header comment explaining purpose
- [ ] Failed tests show key columns (IDs, dates, amounts)
- [ ] Use `LIMIT 100` to prevent massive failure outputs

### **Severity Tuning**
- [ ] `error` = Blocker issues (data integrity, required fields)
- [ ] `warn` = Monitoring issues (anomalies, thresholds, optional fields)

### **Documentation**
- [ ] All custom macros documented in `macros/README.md`
- [ ] Singular tests have one-line purpose comment
- [ ] Complex tests include example violation scenario

---

## Deferred Items

### 📋 **Unit Tests** (TABLED - Revisit Later)
**Status:** Not currently using dbt native `unit_tests:` (requires dbt ≥1.8)

**Future consideration:** 
When ready to adopt unit tests, focus on:
- Transformation logic (calculations, flags, durations)
- Edge cases (NULL handling, boundary conditions)
- Macro behavior (reusable calculation functions)

**Example placeholder:**
```yaml
# Future: Add to model YAML when adopting unit tests
unit_tests:
  - name: test_payment_days_calculation
    model: fact_claim
    given:
      - input: ref('int_claim_details')
        rows:
          - {claim_id: 1, claim_date: '2024-01-01'}
    expect:
      rows:
        - {claim_id: 1, payment_days_from_claim: 30}
```

**Action:** Revisit after completing Priorities 1-3 and when upgrading to dbt ≥1.8

---

## Quick Action Plan

**Week 1:** Incremental guards
1. Create `tests/incremental/` folder
2. Add 3 tests per incremental model (duplicates, freshness, staleness)
3. Tag all with `incremental_guard`

**Week 2:** Temporal FKs
1. Create `tests/claims/` folder  
2. Add `temporal_fk__fact_claim__policy_coverage.sql`
3. Tag with `temporal_fk` and `claims`

**Week 3:** Reorganize existing tests
1. Move tests into domain folders
2. Rename to follow `<intent>__<model>__<desc>.sql` pattern
3. Add tags to `dbt_project.yml`

**Week 4:** CI setup
1. Create `.github/workflows/dbt_pr.yml`
2. Test with `dbt build --select state:modified+`
3. Document in project README