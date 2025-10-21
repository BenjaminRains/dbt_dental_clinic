# Marts CTE Naming Standardization - Refactoring Plan

## Executive Summary

This document outlines the strategy to standardize CTE (Common Table Expression) naming conventions across all mart models in the dbt_dental_clinic project. The goal is to align with SQL best practices and ensure consistency across the entire codebase.

**Current Status**: 1 out of 20 mart models contains non-standard CamelCase CTE names
**Target Status**: 100% snake_case CTE naming across all marts
**Estimated Effort**: Low (single file refactor)
**Business Risk**: None (internal SQL refactoring with no downstream impact)

---

## Background & Context

### Why This Matters

1. **SQL Industry Standard**: snake_case is the universally accepted standard for SQL identifiers
2. **Team Consistency**: 95% of our marts already follow snake_case conventions
3. **Readability**: `missed_appointments` is more readable than `MissedAppointments` in SQL context
4. **Database Portability**: Avoids potential case-sensitivity issues across different databases
5. **Code Maintainability**: Consistent naming reduces cognitive load for developers

### Discovery Process

During a routine code review of `mart_revenue_lost.sql`, we identified CamelCase CTE naming that was inconsistent with:
- The rest of the marts layer
- SQL best practices
- dbt style guide recommendations
- Our own staging and intermediate layer conventions

---

## Current State Analysis

### Inventory of Mart Models

**Total Mart SQL Files**: 20

#### ✅ Models Following snake_case Standard (19 files)

**Dimension Models (7):**
- `dim_clinic.sql` - 1 CTE (snake_case) ✅
- `dim_date.sql` - 3 CTEs (snake_case) ✅
- `dim_fee_schedule.sql` - 1 CTE (snake_case) ✅
- `dim_insurance.sql` - 4 CTEs (snake_case) ✅
- `dim_patient.sql` - 6 CTEs (snake_case) ✅
- `dim_procedure.sql` - 6 CTEs (snake_case) ✅
- `dim_provider.sql` - 4 CTEs (snake_case) ✅

**Fact Models (5):**
- `fact_appointment.sql` - 2 CTEs (snake_case) ✅
- `fact_claim.sql` - 5 CTEs (snake_case) ✅
- `fact_communication.sql` - 3 CTEs (snake_case) ✅
- `fact_payment.sql` - 3 CTEs (snake_case) ✅
- `fact_procedure.sql` - 2 CTEs (snake_case) ✅

**Summary Marts (7):**
- `mart_appointment_summary.sql` - 5 CTEs (snake_case) ✅
- `mart_ar_summary.sql` - 6 CTEs (snake_case) ✅
- `mart_hygiene_retention.sql` - 7 CTEs (snake_case) ✅
- `mart_new_patient.sql` - 5 CTEs (snake_case) ✅
- `mart_patient_retention.sql` - 5 CTEs (snake_case) ✅
- `mart_production_summary.sql` - 7 CTEs (snake_case) ✅
- `mart_provider_performance.sql` - 6 CTEs (snake_case) ✅

#### ❌ Models Requiring Refactoring (1 file)

**`mart_revenue_lost.sql` - 12 CTEs total:**

**CamelCase CTEs (4) - REQUIRE REFACTORING:**
1. `MissedAppointments` (line 116)
2. `ClaimRejections` (line 159)
3. `TreatmentPlanDelays` (line 191)
4. `WriteOffs` (line 223)

**snake_case CTEs (8) - Already compliant:**
1. `appointment_base` (line 58)
2. `claim_base` (line 65)
3. `treatment_base` (line 76)
4. `adjustment_base` (line 83)
5. `provider_dimension` (line 91)
6. `patient_dimension` (line 102)
7. `date_dimension` (line 111)
8. `opportunities_enhanced` (line 256)
9. `final` (line 365)

---

## CTE Naming Patterns in Practice

### Examples from Our Codebase (Best Practice)

```sql
-- From dim_patient.sql
with patient_notes as (
    ...
),

patient_links as (
    ...
),

patient_enhanced as (
    ...
)

-- From mart_provider_performance.sql
with provider_base as (
    ...
),

provider_dimensions as (
    ...
),

claims_aggregated as (
    ...
)

-- From fact_appointment.sql
with source_appointment as (
    ...
),

appointment_calculated as (
    ...
)
```

### Anti-Pattern Found in mart_revenue_lost.sql

```sql
-- ❌ CamelCase CTEs (non-standard)
MissedAppointments as (
    ...
),

ClaimRejections as (
    ...
),

TreatmentPlanDelays as (
    ...
),

WriteOffs as (
    ...
)
```

---

## Refactoring Plan

### Phase 1: Single File Refactoring

**File**: `mart_revenue_lost.sql`

**Changes Required**:

| Current Name (CamelCase) | New Name (snake_case) | Line | References |
|-------------------------|----------------------|------|-----------|
| `MissedAppointments` | `missed_appointments` | 116 | Line 353 (union) |
| `ClaimRejections` | `claim_rejections` | 159 | Line 355 (union) |
| `TreatmentPlanDelays` | `treatment_plan_delays` | 191 | Line 357 (union) |
| `WriteOffs` | `write_offs` | 223 | Line 359 (union) |

**Detailed Change List**:

1. **Line 116**: Rename CTE definition
   ```sql
   -- BEFORE
   MissedAppointments as (
   
   -- AFTER
   missed_appointments as (
   ```

2. **Line 159**: Rename CTE definition
   ```sql
   -- BEFORE
   ClaimRejections as (
   
   -- AFTER
   claim_rejections as (
   ```

3. **Line 191**: Rename CTE definition
   ```sql
   -- BEFORE
   TreatmentPlanDelays as (
   
   -- AFTER
   treatment_plan_delays as (
   ```

4. **Line 223**: Rename CTE definition
   ```sql
   -- BEFORE
   WriteOffs as (
   
   -- AFTER
   write_offs as (
   ```

5. **Lines 353-359**: Update union references
   ```sql
   -- BEFORE
   select * from MissedAppointments
   union all
   select * from ClaimRejections
   union all
   select * from TreatmentPlanDelays
   union all
   select * from WriteOffs
   
   -- AFTER
   select * from missed_appointments
   union all
   select * from claim_rejections
   union all
   select * from treatment_plan_delays
   union all
   select * from write_offs
   ```

---

## Impact Analysis

### ✅ No External Impact (Safe Refactoring)

**Why This Is Safe:**
1. **CTEs are query-scoped**: CTE names only exist within the SQL file
2. **No column name changes**: Output schema remains identical
3. **No table name changes**: Model still outputs to `raw_marts.mart_revenue_lost`
4. **No API impact**: FastAPI queries the table directly, not internal CTEs
5. **No exposure impact**: Exposures reference the model name, not CTEs
6. **No BI tool impact**: External tools never see CTE names

### Downstream Systems (Unaffected)

**API Layer (`api/routers/reports.py`)**:
- ✅ Queries `raw_marts.mart_revenue_lost` table directly
- ✅ References column names, not CTE names
- ✅ No changes needed

**Exposures (`exposures.yml`)**:
- ✅ References `ref('mart_revenue_lost')` model name
- ✅ No knowledge of internal CTEs
- ✅ No changes needed

**BI Tools / Dashboards**:
- ✅ Query final table, not internal CTEs
- ✅ No changes needed

### Only Internal Impact

**Within `mart_revenue_lost.sql`**:
- ✅ Improves code readability
- ✅ Aligns with codebase standards
- ✅ Makes code more maintainable

---

## Testing Strategy

### Pre-Refactoring Tests

1. **Compile Check**
   ```bash
   dbt compile -s mart_revenue_lost
   ```
   - Verify current model compiles successfully
   - Document any existing warnings

2. **Run Check**
   ```bash
   dbt run -s mart_revenue_lost
   ```
   - Verify current model runs successfully
   - Capture row counts for comparison

3. **Test Check**
   ```bash
   dbt test -s mart_revenue_lost
   ```
   - Document current test results
   - Establish baseline for comparison

4. **Schema Documentation**
   ```bash
   dbt docs generate
   dbt docs serve
   ```
   - Verify current documentation state

### Post-Refactoring Validation

1. **Compilation Validation**
   ```bash
   dbt compile -s mart_revenue_lost
   ```
   - ✅ Should compile without errors
   - ✅ Should have no new warnings

2. **Execution Validation**
   ```bash
   dbt run -s mart_revenue_lost --full-refresh
   ```
   - ✅ Should run successfully
   - ✅ Should produce same row count
   - ✅ Should complete in similar time

3. **Data Quality Validation**
   ```bash
   dbt test -s mart_revenue_lost
   ```
   - ✅ All existing tests should pass
   - ✅ No new test failures

4. **Output Schema Validation**
   ```sql
   -- Query to verify schema unchanged
   SELECT 
       column_name, 
       data_type, 
       ordinal_position
   FROM information_schema.columns
   WHERE table_schema = 'raw_marts'
     AND table_name = 'mart_revenue_lost'
   ORDER BY ordinal_position;
   ```
   - ✅ Should have identical column names and types
   - ✅ Should have identical column order

5. **Data Integrity Validation**
   ```sql
   -- Sample data comparison query
   SELECT 
       COUNT(*) as row_count,
       SUM(lost_revenue) as total_lost_revenue,
       COUNT(DISTINCT opportunity_id) as unique_opportunities,
       COUNT(DISTINCT patient_id) as unique_patients
   FROM raw_marts.mart_revenue_lost;
   ```
   - ✅ Aggregate metrics should match pre-refactor values

6. **Lineage Validation**
   ```bash
   dbt docs generate
   dbt docs serve
   ```
   - ✅ DAG should show unchanged dependencies
   - ✅ Exposures should still reference this model correctly

7. **API Integration Test**
   ```bash
   # Test API endpoints that consume this mart
   curl http://localhost:8000/reports/revenue/trends
   curl http://localhost:8000/reports/revenue/kpi-summary
   ```
   - ✅ API should return data successfully
   - ✅ Response structure should be unchanged

---

## Implementation Checklist

### Pre-Refactoring (Setup)
- [ ] Create feature branch: `refactor/marts-cte-naming-standardization`
- [ ] Document current state:
  - [ ] Run `dbt compile -s mart_revenue_lost` and save output
  - [ ] Run `dbt run -s mart_revenue_lost` and capture row count
  - [ ] Run `dbt test -s mart_revenue_lost` and document results
  - [ ] Query table schema and save results
  - [ ] Query aggregate metrics and save results
- [ ] Notify team of planned refactoring (low risk, but good practice)

### Refactoring (Execution)
- [ ] Backup current `mart_revenue_lost.sql` file
- [ ] Make CTE naming changes:
  - [ ] Line 116: `MissedAppointments` → `missed_appointments`
  - [ ] Line 159: `ClaimRejections` → `claim_rejections`
  - [ ] Line 191: `TreatmentPlanDelays` → `treatment_plan_delays`
  - [ ] Line 223: `WriteOffs` → `write_offs`
  - [ ] Line 353: Update union reference (MissedAppointments)
  - [ ] Line 355: Update union reference (ClaimRejections)
  - [ ] Line 357: Update union reference (TreatmentPlanDelays)
  - [ ] Line 359: Update union reference (WriteOffs)
- [ ] Update SQL comments if they reference old CTE names
- [ ] Save file

### Post-Refactoring (Validation)
- [ ] Compilation validation:
  - [ ] `dbt compile -s mart_revenue_lost` passes
  - [ ] No new compilation warnings
- [ ] Execution validation:
  - [ ] `dbt run -s mart_revenue_lost --full-refresh` succeeds
  - [ ] Row count matches pre-refactor count
  - [ ] Execution time is comparable
- [ ] Data quality validation:
  - [ ] `dbt test -s mart_revenue_lost` passes all tests
  - [ ] Query schema - verify unchanged
  - [ ] Query aggregates - verify match
- [ ] Integration validation:
  - [ ] API endpoints still work
  - [ ] Test dashboard functionality
  - [ ] Verify dbt docs generate successfully
- [ ] Code review validation:
  - [ ] All CTE names are snake_case
  - [ ] All references are updated
  - [ ] Code is more readable

### Documentation & Deployment
- [ ] Update this refactoring document with:
  - [ ] Actual vs estimated effort
  - [ ] Any issues encountered
  - [ ] Validation test results
- [ ] Code review with team member
- [ ] Merge to main branch
- [ ] Deploy to development environment
- [ ] Verify in development
- [ ] Deploy to production
- [ ] Mark refactoring as complete

---

## Risk Assessment

### Risk Level: **LOW**

| Risk Category | Level | Mitigation |
|--------------|-------|------------|
| Data Quality Impact | None | CTEs don't affect output schema |
| API Breaking Change | None | API queries table, not CTEs |
| Dashboard Impact | None | Dashboards query table, not CTEs |
| Performance Impact | None | Query plan unchanged |
| Deployment Risk | Low | Single file change, easily reverted |
| Testing Coverage | Low | Comprehensive test plan in place |

### Rollback Plan

**If issues are discovered:**

1. **Immediate Rollback** (if critical issue found)
   ```bash
   git revert <commit-hash>
   dbt run -s mart_revenue_lost
   ```

2. **Validation After Rollback**
   - Verify model compiles and runs
   - Verify API endpoints work
   - Verify dashboards display data

---

## Success Criteria

### Technical Success
- ✅ All CTE names in `mart_revenue_lost.sql` are snake_case
- ✅ Model compiles without errors
- ✅ Model runs successfully with same row count
- ✅ All tests pass
- ✅ Output schema is unchanged
- ✅ API integration tests pass
- ✅ No performance degradation

### Business Success
- ✅ No user-facing impact
- ✅ Improved code maintainability
- ✅ 100% consistency across marts layer
- ✅ Alignment with SQL best practices

### Team Success
- ✅ Documentation updated
- ✅ Team knowledge shared
- ✅ Standard established for future models

---

## Future Considerations

### Preventive Measures

1. **Code Review Checklist**: Add CTE naming convention check
2. **Style Guide**: Document snake_case requirement for CTEs
3. **Pre-commit Hooks**: Consider automated naming convention checks
4. **New Model Template**: Ensure templates use snake_case examples

### Monitoring

1. **Post-Deployment**: Monitor API response times
2. **Data Quality**: Watch for any test failures
3. **Usage Metrics**: Verify dashboard query performance

---

## Timeline

**Estimated Duration**: 1-2 hours

| Phase | Duration | Description |
|-------|----------|-------------|
| Pre-refactoring validation | 15 min | Run tests and capture baseline |
| Refactoring execution | 15 min | Make naming changes |
| Post-refactoring validation | 30 min | Run all validation tests |
| Code review & deployment | 15 min | Review, merge, and deploy |
| **Total** | **75 min** | **Complete refactoring cycle** |

---

## Conclusion

This refactoring represents a low-risk, high-value improvement to our codebase. By standardizing CTE naming conventions, we:

1. **Improve Code Quality**: Align with SQL industry standards
2. **Enhance Maintainability**: Consistent naming reduces cognitive load
3. **Support Team Growth**: Clear standards help onboarding
4. **Demonstrate Excellence**: Attention to detail in code quality

The single file requiring changes (`mart_revenue_lost.sql`) has zero downstream impact, making this an ideal opportunity to establish consistent standards across our marts layer.

---

## Appendix A: SQL Best Practices Reference

### Industry Standard CTE Naming

**Recommended (snake_case)**:
```sql
with patient_base as (...),
     appointment_metrics as (...),
     revenue_calculations as (...)
```

**Not Recommended (CamelCase)**:
```sql
with PatientBase as (...),
     AppointmentMetrics as (...),
     RevenueCalculations as (...)
```

### Sources
- dbt Style Guide: https://docs.getdbt.com/best-practices/how-we-style/sql-style-guide
- GitLab SQL Style Guide: https://about.gitlab.com/handbook/business-technology/data-team/platform/sql-style-guide/
- Brooklyn Data Co SQL Style Guide: https://github.com/brooklyn-data/co/blob/main/sql_style_guide.md

---

## Appendix B: Reference Examples

### Before and After Comparison

**BEFORE (mart_revenue_lost.sql - Lines 350-360)**:
```sql
from (
    -- Union all opportunity sources
    select * from MissedAppointments
    union all
    select * from ClaimRejections
    union all
    select * from TreatmentPlanDelays
    union all
    select * from WriteOffs
) all_opportunities
```

**AFTER (mart_revenue_lost.sql - Lines 350-360)**:
```sql
from (
    -- Union all opportunity sources
    select * from missed_appointments
    union all
    select * from claim_rejections
    union all
    select * from treatment_plan_delays
    union all
    select * from write_offs
) all_opportunities
```

---

**Document Version**: 1.0  
**Created**: 2025-10-16  
**Author**: Data Engineering Team  
**Status**: Ready for Implementation

