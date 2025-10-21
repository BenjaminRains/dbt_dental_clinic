# Mart Models with Direct Staging References - Analysis & Refactor Plan

**Date:** October 17, 2025  
**Issue:** Mart models are directly referencing staging models, violating dbt best practice layering conventions

## Executive Summary

A comprehensive audit of mart models revealed that **12 of 20 mart models** (60%) are directly referencing staging models instead of intermediate models. This violates the standard dbt layering convention:

```
staging → intermediate → marts
```

This analysis documents all violations, provides a refactor plan, and identifies gaps in the intermediate layer.

---

## 1. Violations by Model Type

### 1.1 Dimension Models (6 violations)

#### `dim_patient.sql`
- **Status:** ❌ Direct staging references
- **Current Dependencies:**
  - `stg_opendental__patient`
  - `stg_opendental__patientnote`
  - `stg_opendental__patientlink`
  - `stg_opendental__disease`
  - `stg_opendental__document`

- **Should Use:** ✅ `int_patient_profile`
  - Already exists at `models/intermediate/foundation/int_patient_profile.sql`
  - Provides: patient, patientnote, patientlink (aggregated)
  - **Missing:** disease, document aggregations

- **Refactor Priority:** HIGH
- **Complexity:** Medium (need to add disease/document to intermediate)

---

#### `dim_provider.sql`
- **Status:** ❌ Direct staging references
- **Current Dependencies:**
  - `stg_opendental__provider`
  - `stg_opendental__definition`

- **Should Use:** ✅ `int_provider_profile`
  - Already exists at `models/intermediate/foundation/int_provider_profile.sql`
  - Need to verify if it includes definition joins

- **Refactor Priority:** HIGH
- **Complexity:** Low

---

#### `dim_procedure.sql`
- **Status:** ❌ Direct staging references
- **Current Dependencies:**
  - `stg_opendental__procedurecode`
  - `stg_opendental__feesched`
  - `stg_opendental__definition`
  - `stg_opendental__fee`

- **Should Use:** ❌ **MISSING** - `int_procedure_codes` (does not exist)
- **Alternative:** Could use `int_fee_model` and `int_procedure_complete`
  - `int_fee_model` exists at `models/intermediate/system_a_fee_processing/int_fee_model.sql`
  - `int_procedure_complete` exists at `models/intermediate/system_a_fee_processing/int_procedure_complete.sql`

- **Refactor Priority:** MEDIUM
- **Complexity:** High (may need to create new intermediate model)

---

#### `dim_fee_schedule.sql`
- **Status:** ❌ Direct staging references
- **Current Dependencies:**
  - `stg_opendental__feesched`

- **Should Use:** Could reference `int_fee_model`
- **Refactor Priority:** LOW
- **Complexity:** Low (simple substitution)

---

#### `dim_clinic.sql`
- **Status:** ❌ Direct staging references
- **Current Dependencies:**
  - `stg_opendental__clinic` (currently unused in exposures and sources)

- **Should Use:** ❌ **MISSING** - `int_clinic_profile` (does not exist)
- **Refactor Priority:** LOW (staging source currently unused)
- **Complexity:** Medium (need to create intermediate model)

---

#### `dim_insurance.sql`
- **Status:** ❌ Direct staging references (from yml)
- **Current Dependencies:**
  - `stg_opendental__carrier`
  - `stg_opendental__inssub`

- **Should Use:** ✅ `int_insurance_coverage`
  - Already exists at `models/intermediate/system_b_insurance/int_insurance_coverage.sql`

- **Refactor Priority:** HIGH
- **Complexity:** Low

---

### 1.2 Fact Models (3 violations)

#### `fact_appointment.sql`
- **Status:** ❌ Direct staging references
- **Current Dependencies:**
  - `stg_opendental__appointment`

- **Should Use:** ✅ `int_appointment_details`
  - Already exists at `models/intermediate/system_g_scheduling/int_appointment_details.sql`
  - Provides comprehensive appointment data with business logic

- **Refactor Priority:** CRITICAL
- **Complexity:** Low (direct substitution available)

---

#### `fact_payment.sql`
- **Status:** ❌ Direct staging references
- **Current Dependencies:**
  - `stg_opendental__payment`
  - `stg_opendental__paysplit`

- **Should Use:** ✅ `int_payment_split` and payment intermediate models
  - `int_payment_split` exists at `models/intermediate/system_c_payment/int_payment_split.sql`
  - `int_patient_payment_allocated` exists
  - `int_insurance_payment_allocated` exists

- **Refactor Priority:** CRITICAL
- **Complexity:** Medium (multiple intermediate models to integrate)

---

#### `fact_communication.sql`
- **Status:** ❌ Direct staging references
- **Current Dependencies:**
  - `stg_opendental__commlog`

- **Should Use:** ✅ `int_patient_communications_base`
  - Already exists at `models/intermediate/system_f_communications/int_patient_communications_base.sql`

- **Refactor Priority:** HIGH
- **Complexity:** Low

---

### 1.3 Summary/Analysis Marts (3 violations)

#### `mart_revenue_lost.sql`
- **Status:** ❌ Direct staging references (mixed with facts)
- **Current Dependencies:**
  - ✅ `fact_appointment` (correct)
  - ✅ `fact_claim` (correct)
  - ❌ `stg_opendental__schedule` (should be intermediate)
  - ❌ `stg_opendental__treatplan` (should be intermediate)
  - ❌ `stg_opendental__adjustment` (should be intermediate)

- **Should Use:**
  - ✅ `int_appointment_schedule` exists
  - ❌ **MISSING** - treatment plan intermediate
  - ✅ `int_adjustments` exists

- **Refactor Priority:** HIGH
- **Complexity:** High (complex model, multiple dependencies)

---

#### `mart_hygiene_retention.sql`
- **Status:** ❌ Direct staging references
- **Current Dependencies:**
  - `stg_opendental__recall`
  - `stg_opendental__recalltype`

- **Should Use:** ❌ **MISSING** - recall intermediate models
- **Refactor Priority:** MEDIUM
- **Complexity:** Medium (need to create recall intermediate)

---

#### `mart_new_patient.sql`
- **Status:** ❌ Direct staging references
- **Current Dependencies:**
  - `stg_opendental__procedurelog`
  - `stg_opendental__procedurecode`

- **Should Use:** ✅ `int_procedure_complete`
  - Already exists with procedure codes joined

- **Refactor Priority:** HIGH
- **Complexity:** Low

---

#### `mart_production_summary.sql`
- **Status:** ❌ Direct staging references
- **Current Dependencies:**
  - `stg_opendental__procedurelog`
  - `stg_opendental__payment`
  - `stg_opendental__paysplit`

- **Should Use:**
  - ✅ `int_procedure_complete`
  - ✅ `int_payment_split` and related payment intermediates

- **Refactor Priority:** HIGH
- **Complexity:** Medium

---

## 2. Missing Intermediate Models

The following intermediate models should be created to support proper layering:

### 2.1 High Priority

1. **`int_treatment_plan`** (System: Treatment Planning)
   - Source: `stg_opendental__treatplan`, `stg_opendental__treatplanattach`
   - Purpose: Treatment plan enrichment with procedures and status logic
   - Needed by: `mart_revenue_lost`


### 2.2 Medium Priority

3. **`int_recall_management`** (System: Patient Engagement)
   - Source: `stg_opendental__recall`, `stg_opendental__recalltype`, `stg_opendental__recalltrigger`
   - Purpose: Recall scheduling and tracking with type definitions
   - Needed by: `mart_hygiene_retention`

4. **`int_procedure_codes`** or enhance `int_procedure_complete` (System: Fee Processing)
   - Source: `stg_opendental__procedurecode`, `stg_opendental__fee`, `stg_opendental__feesched`
   - Purpose: Comprehensive procedure code catalog with fee schedules
   - Needed by: `dim_procedure`

---

## 3. Refactor Prioritization

### Priority 1: CRITICAL (Do First)
- ✅ **`fact_appointment.sql`** → Use `int_appointment_details`
- ✅ **`fact_payment.sql`** → Use `int_payment_split` + payment intermediates

**Rationale:** Facts are the foundation for most analysis marts. Fixing these first creates a domino effect.

### Priority 2: HIGH (Do Second)
- ✅ **`dim_patient.sql`** → Use `int_patient_profile` (enhance with disease/document)
- ✅ **`dim_provider.sql`** → Use `int_provider_profile`
- ✅ **`dim_insurance.sql`** → Use `int_insurance_coverage`
- ✅ **`fact_communication.sql`** → Use `int_patient_communications_base`
- ✅ **`mart_new_patient.sql`** → Use `int_procedure_complete`
- ✅ **`mart_production_summary.sql`** → Use intermediates
- ✅ **`mart_revenue_lost.sql`** → Use `int_adjustments`, create `int_treatment_plan`

**Rationale:** High-visibility business reports and core dimensions used by many models.

### Priority 3: MEDIUM (Do Third)
- **`dim_procedure.sql`** → Use/create procedure intermediate
- **`dim_clinic.sql`** → Create `int_clinic_profile`
- **`mart_hygiene_retention.sql`** → Create recall intermediate

**Rationale:** Less frequently accessed models, but still important for completeness.

### Priority 4: LOW (Do Last)
- **`dim_fee_schedule.sql`** → Use `int_fee_model`

- **`int_clinic_profile`** (System: Foundation)
   - Source: `stg_opendental__clinic`, provider assignments
   - Purpose: Clinic information with provider relationships
   - Needed by: `dim_clinic`

**Rationale:** Simple dimension with single source, minimal impact.

---

## 4. Implementation Strategy

### Phase 1: Create Missing Intermediates (Week 1)
```bash
# Create new intermediate models
dbt_dental_models/models/intermediate/
├── system_a_fee_processing/
│   └── int_treatment_plan.sql          # NEW
├── foundation/
│   └── int_clinic_profile.sql          # NEW
└── system_patient_engagement/          # NEW DIRECTORY
    └── int_recall_management.sql       # NEW
```

### Phase 2: Enhance Existing Intermediates (Week 1-2)
- Add disease/document aggregations to `int_patient_profile`
- Verify `int_provider_profile` includes definition lookups
- Test all intermediate enhancements

### Phase 3: Refactor Facts (Week 2)
1. `fact_appointment.sql` → `int_appointment_details`
2. `fact_payment.sql` → `int_payment_split`
3. `fact_communication.sql` → `int_patient_communications_base`

### Phase 4: Refactor Dimensions (Week 3)
1. `dim_patient.sql` → `int_patient_profile` (enhanced)
2. `dim_provider.sql` → `int_provider_profile`
3. `dim_insurance.sql` → `int_insurance_coverage`
4. `dim_clinic.sql` → `int_clinic_profile` (new)
5. `dim_fee_schedule.sql` → `int_fee_model`
6. `dim_procedure.sql` → procedure intermediate

### Phase 5: Refactor Marts (Week 4)
1. `mart_new_patient.sql` → `int_procedure_complete`
2. `mart_production_summary.sql` → intermediates
3. `mart_revenue_lost.sql` → intermediates + `int_treatment_plan`
4. `mart_hygiene_retention.sql` → `int_recall_management`

### Phase 6: Testing & Validation (Week 5)
- Run full DAG with `dbt build`
- Compare row counts and key metrics before/after
- Validate with stakeholder reports
- Update documentation and lineage

---

## 5. Benefits of Refactor

### 5.1 Architectural Benefits
- ✅ **Proper layering** - Staging → Intermediate → Marts
- ✅ **Single source of truth** - Shared business logic in intermediate layer
- ✅ **Easier to maintain** - Business rules centralized
- ✅ **Better testability** - Test business logic at intermediate level

### 5.2 Performance Benefits
- ✅ **Reduced redundancy** - Intermediate models cached/materialized
- ✅ **Faster mart builds** - Marts can use incremental strategies on intermediates
- ✅ **Parallel execution** - Better DAG parallelization

### 5.3 Business Benefits
- ✅ **Consistency** - Same business rules applied across all marts
- ✅ **Reusability** - Intermediate models serve multiple marts
- ✅ **Auditability** - Clear lineage from source to report

---

## 6. Risk Assessment

### Low Risk
- Models with direct intermediate substitution (e.g., `fact_appointment`)
- No business logic changes required

### Medium Risk
- Models requiring new intermediate models
- May expose hidden business logic differences

### High Risk
- `mart_revenue_lost` - Complex model with multiple dependencies
- `dim_procedure` - Heavy business logic, fee schedule complexity

### Mitigation Strategies
1. **Create branch protection** - Refactor in feature branch
2. **Row count validation** - Before/after comparison
3. **Key metric validation** - Compare critical business metrics
4. **Stakeholder review** - Business users validate results
5. **Gradual rollout** - Deploy one model at a time

---

## 7. Testing Checklist

For each refactored model:

```sql
-- Row count comparison
SELECT COUNT(*) FROM old_model;
SELECT COUNT(*) FROM new_model;

-- Key metric comparison
SELECT 
    SUM(revenue), 
    AVG(metric), 
    MIN(date), 
    MAX(date)
FROM old_model;

-- Join validation
SELECT 
    COUNT(*) as unmatched_rows
FROM old_model o
FULL OUTER JOIN new_model n USING (primary_key)
WHERE o.primary_key IS NULL OR n.primary_key IS NULL;
```

---

## 8. Next Steps

1. **Discuss with team** - Review findings and refactor plan
2. **Create tickets** - Break down into implementable tasks
3. **Prioritize work** - Align with business priorities
4. **Create branch** - `refactor/marts-use-intermediates`
5. **Begin Phase 1** - Create missing intermediate models

---

## Appendix A: Complete Dependency Map

```
FACT MODELS:
fact_appointment → int_appointment_details ← stg_opendental__appointment
fact_payment → int_payment_split ← stg_opendental__payment, stg_opendental__paysplit
fact_communication → int_patient_communications_base ← stg_opendental__commlog

DIMENSION MODELS:
dim_patient → int_patient_profile ← stg_opendental__patient, patientnote, patientlink
dim_provider → int_provider_profile ← stg_opendental__provider
dim_insurance → int_insurance_coverage ← stg_opendental__carrier, inssub
dim_clinic → int_clinic_profile (NEW) ← stg_opendental__clinic
dim_procedure → int_procedure_codes (NEW/ENHANCE) ← stg_opendental__procedurecode, fee
dim_fee_schedule → int_fee_model ← stg_opendental__feesched

MART MODELS:
mart_new_patient → int_procedure_complete ← stg_opendental__procedurelog
mart_production_summary → int_procedure_complete, int_payment_split
mart_revenue_lost → int_adjustments, int_treatment_plan (NEW), fact_appointment
mart_hygiene_retention → int_recall_management (NEW) ← stg_opendental__recall
```

---

## Appendix B: Code Examples

### Example 1: fact_appointment.sql Refactor

**Before:**
```sql
with source_appointment as (
    select * from {{ ref('stg_opendental__appointment') }}
),
```

**After:**
```sql
with source_appointment as (
    select * from {{ ref('int_appointment_details') }}
),
```

### Example 2: dim_patient.sql Refactor

**Before:**
```sql
with source_patient as (
    select * from {{ ref('stg_opendental__patient') }}
),
patient_notes as (
    select * from {{ ref('stg_opendental__patientnote') }}
),
```

**After:**
```sql
with source_patient as (
    select * from {{ ref('int_patient_profile') }}
),
-- patient_notes now included in int_patient_profile
```

---

**End of Analysis**

