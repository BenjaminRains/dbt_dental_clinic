# Explicit Column Selection - Modeling Review Findings

**Date:** 2025-01-27  
**Review Scope:** dbt_dental_models directory  
**Focus:** Use of `select *` vs explicit column selection

## Executive Summary

This review identified widespread use of `select *` in staging models when selecting from source tables. While this pattern is common and works initially, it creates long-term reliability risks when the source system (OpenDental) adds columns or changes data types. This document outlines findings, risks, and recommendations for a potential refactor.

## Findings Overview

### 1. Staging Models - Source Selection (HIGH PRIORITY)

**Pattern Found:** 72 staging models use `select * from {{ source('opendental', ...) }}` in the `source_data` CTE.

**Example:**
```sql
with source_data as (
    select * from {{ source('opendental', 'patient') }}
),
```

**Affected Models:**
- All 72 staging models in `models/staging/opendental/` directory
- Includes critical tables: `patient`, `appointment`, `procedurelog`, `provider`, `payment`, `claim`, etc.

**Risk Level:** **HIGH**
- OpenDental schema changes (new columns, type changes) will break models unexpectedly
- No visibility into which columns are actually used downstream
- Harder to track data lineage and dependencies
- Potential performance impact from loading unused columns

### 2. Intermediate Models - Staging References (MEDIUM PRIORITY)

**Pattern Found:** 42 instances of `select * from {{ ref('stg_...') }}` or `select * from {{ ref('int_...') }}` in intermediate models.

**Examples:**
```sql
-- From int_provider_profile.sql
with source_providers as (
    select * from {{ ref('stg_opendental__provider') }}
),

-- From int_fee_model.sql
source_procedures as (
    select * from {{ ref('stg_opendental__procedurelog') }}
),
source_fees as (
    select * from {{ ref('stg_opendental__fee') }}
),
```

**Affected Models:**
- `int_provider_profile.sql`
- `int_patient_profile.sql` (uses `select distinct on (patient_id) *`)
- `int_fee_model.sql` (multiple source CTEs)
- `int_claim_details.sql`
- `int_insurance_coverage.sql`
- And others in intermediate layer

**Risk Level:** **MEDIUM**
- Less critical than source selection since staging models control the schema
- Still creates coupling - if staging model adds columns, intermediate may break
- Makes it harder to understand what data is actually used in business logic

### 3. Mart Models - Intermediate References (LOW-MEDIUM PRIORITY)

**Pattern Found:** Multiple mart models use `select * from {{ ref('int_...') }}` or `select * from {{ ref('fact_...') }}`.

**Examples:**
```sql
-- From fact_appointment.sql
with source_appointment as (
    select * from {{ ref('int_appointment_details') }}
),

-- From mart_revenue_lost.sql
source_appointments as (
    select * from {{ ref('fact_appointment') }}
),
source_claims as (
    select * from {{ ref('fact_claim') }}
),
```

**Affected Models:**
- `fact_appointment.sql`
- `fact_payment.sql`
- `fact_communication.sql`
- `fact_claim.sql`
- `mart_revenue_lost.sql`
- `dim_patient.sql`
- `dim_provider.sql`
- And others

**Risk Level:** **LOW-MEDIUM**
- Mart models should be more explicit about what they need
- However, intermediate models are more controlled, so risk is lower
- Still worth addressing for clarity and maintainability

### 4. Final CTEs (ACCEPTABLE)

**Pattern Found:** Many models end with `select * from final` or `select * from renamed_columns`.

**Status:** **ACCEPTABLE**
- This is a common pattern and is fine
- The final CTE is explicitly defined in the same file
- No external dependency risk

## Detailed Analysis by Model Type

### Staging Models Pattern

**Current Pattern:**
```sql
{{ config(...) }}

with source_data as (
    select * from {{ source('opendental', 'table_name') }}
    -- Optional filters
),

renamed_columns as (
    select
        -- Explicit column transformations
        "ColumnName" as column_name,
        ...
    from source_data
)

select * from renamed_columns
```

**Issues:**
1. `source_data` CTE pulls all columns from source, even if not used
2. If OpenDental adds a column with a problematic name/type, model may fail
3. No explicit documentation of which source columns are needed
4. Harder to audit what data is being ingested

**Recommended Pattern:**
```sql
{{ config(...) }}

with source_data as (
    select
        -- Explicitly list all columns needed
        "PatNum",
        "Guarantor",
        "PriProv",
        "Gender",
        "Birthdate",
        -- ... all columns used in renamed_columns
        "SecDateEntry",
        "DateTStamp"
    from {{ source('opendental', 'table_name') }}
    -- Optional filters
),

renamed_columns as (
    select
        -- Same transformations as before
        ...
    from source_data
)

select * from renamed_columns
```

### Intermediate Models Pattern

**Current Pattern:**
```sql
with source_staging as (
    select * from {{ ref('stg_opendental__table') }}
),
```

**Issues:**
1. Couples intermediate model to full staging schema
2. If staging model adds columns, intermediate may need updates
3. Less clear what data is actually used in business logic

**Recommended Pattern:**
```sql
with source_staging as (
    select
        -- Only columns needed for this intermediate model
        patient_id,
        provider_id,
        appointment_datetime,
        -- ... explicit list
    from {{ ref('stg_opendental__table') }}
),
```

## Risk Assessment

### High Risk Scenarios

1. **OpenDental Schema Updates**
   - New columns added to source tables
   - Column type changes (e.g., VARCHAR to TEXT, INT to BIGINT)
   - Column renames (less likely but possible)
   - Result: Models may fail during dbt runs

2. **Data Type Incompatibilities**
   - If OpenDental changes a column type that conflicts with transformations
   - Example: Date column changed to timestamp, breaking date macros
   - Result: Runtime errors or incorrect data

3. **Performance Impact**
   - Loading unused columns increases memory/processing
   - Especially problematic for large tables like `procedurelog`, `appointment`
   - Result: Slower dbt runs, higher costs

### Medium Risk Scenarios

1. **Staging Model Changes**
   - If staging models add columns, intermediate models may break
   - Less likely since we control staging models
   - Result: Need to update intermediate models

2. **Maintenance Burden**
   - Harder to understand dependencies
   - Difficult to audit what data is actually used
   - Result: Slower development, more bugs

## Recommendations

### Phase 1: Staging Models (HIGH PRIORITY)

**Target:** All 72 staging models in `models/staging/opendental/`

**Approach:**
1. For each staging model, identify all columns used in `renamed_columns` CTE
2. Update `source_data` CTE to explicitly select only those columns
3. Add comments documenting why each column is included
4. Test each model after refactoring

**Benefits:**
- Prevents breakage from OpenDental schema changes
- Clear documentation of data dependencies
- Better performance (only load needed columns)
- Easier to audit and maintain

**Estimated Effort:**
- ~2-4 hours per model (72 models = 144-288 hours)
- Can be done incrementally, model by model
- Priority: Start with high-traffic tables (patient, appointment, procedurelog, payment, claim)

### Phase 2: Intermediate Models (MEDIUM PRIORITY)

**Target:** Intermediate models that use `select *` from staging

**Approach:**
1. Review each intermediate model
2. Identify which staging columns are actually used
3. Update source CTEs to explicitly select those columns
4. Update tests if needed

**Benefits:**
- Reduced coupling between layers
- Clearer data flow
- Easier to understand business logic

**Estimated Effort:**
- ~1-2 hours per model (42 instances = 42-84 hours)
- Lower priority than staging models

### Phase 3: Mart Models (LOW PRIORITY)

**Target:** Mart models that use `select *` from intermediate/fact models

**Approach:**
1. Review each mart model
2. Explicitly select only needed columns from intermediate/fact models
3. Document business requirements for each column

**Benefits:**
- Better documentation of mart requirements
- Clearer data lineage
- Easier to optimize mart queries

**Estimated Effort:**
- ~1 hour per model
- Can be done as part of normal development

## Implementation Strategy

### Option 1: Incremental Refactor (RECOMMENDED)

**Approach:**
- Refactor models one at a time, starting with highest priority
- Test each model after refactoring
- Deploy incrementally to production
- Track progress in a spreadsheet or project board

**Pros:**
- Lower risk (one model at a time)
- Can prioritize by business impact
- Easier to review and test
- Can pause/resume as needed

**Cons:**
- Takes longer overall
- Mixed patterns during transition

### Option 2: Big Bang Refactor

**Approach:**
- Refactor all models at once
- Comprehensive testing
- Single deployment

**Pros:**
- Consistent pattern immediately
- No mixed patterns

**Cons:**
- High risk (many changes at once)
- Difficult to test comprehensively
- Harder to review
- All-or-nothing deployment

### Option 3: New Models Only

**Approach:**
- Apply explicit column selection to new models only
- Gradually refactor existing models as they're touched

**Pros:**
- No immediate risk
- Natural migration over time

**Cons:**
- Mixed patterns for long time
- Existing models remain at risk

## Priority List for Refactoring

### Tier 1: Critical Business Tables (Start Here)
1. `stg_opendental__patient.sql` - Core patient data
2. `stg_opendental__appointment.sql` - Scheduling data
3. `stg_opendental__procedurelog.sql` - Procedure data
4. `stg_opendental__payment.sql` - Payment data
5. `stg_opendental__claim.sql` - Insurance claims
6. `stg_opendental__provider.sql` - Provider data

### Tier 2: High-Volume Tables
7. `stg_opendental__procedurecode.sql`
8. `stg_opendental__fee.sql`
9. `stg_opendental__adjustment.sql`
10. `stg_opendental__paysplit.sql`

### Tier 3: Supporting Tables
11. All other staging models (alphabetical or by system)

## Testing Strategy

For each refactored model:

1. **Schema Validation**
   - Ensure output schema matches original
   - Verify all expected columns present
   - Check data types match

2. **Data Validation**
   - Compare row counts (should match)
   - Sample data comparison (spot check)
   - Key metric validation (sums, counts, etc.)

3. **Integration Testing**
   - Test downstream models still work
   - Verify no breaking changes
   - Check performance (should be same or better)

4. **Regression Testing**
   - Run full dbt test suite
   - Verify no test failures
   - Check data quality tests pass

## Example Refactor

### Before:
```sql
with source_data as (
    select * from {{ source('opendental', 'patient') }}
),

renamed_columns as (
    select
        {{ transform_id_columns([...]) }},
        "Gender" as gender,
        "Birthdate" as birth_date,
        ...
    from source_data
)

select * from renamed_columns
```

### After:
```sql
with source_data as (
    select
        -- Primary Keys
        "PatNum",
        "Guarantor",
        "PriProv",
        "SecProv",
        "ClinicNum",
        "FeeSched",
        
        -- Demographics
        "Gender",
        "Language",
        
        -- Status
        "PatStatus",
        "Position",
        "StudentStatus",
        "Urgency",
        
        -- Boolean fields
        "Premed",
        "TxtMsgOk",
        "PreferContactConfidential",
        "HasIns",
        
        -- Contact preferences
        "PreferConfirmMethod",
        "PreferContactMethod",
        "PreferRecallMethod",
        
        -- Financial
        "EstBalance",
        "BalTotal",
        "Bal_0_30",
        "Bal_31_60",
        "Bal_61_90",
        "BalOver90",
        "InsEst",
        "PayPlanDue",
        "BillingCycleDay",
        
        -- Dates
        "Birthdate",
        "DateFirstVisit",
        "DateTimeDeceased",
        "AdmitDate",
        
        -- Scheduling
        "SchedBeforeTime",
        "SchedAfterTime",
        "SchedDayOfWeek",
        "AskToArriveEarly",
        
        -- Business logic
        "PlannedIsDone",
        
        -- Metadata
        "SecDateEntry",
        "DateTStamp",
        "SecUserNumEntry"
    from {{ source('opendental', 'patient') }}
),

renamed_columns as (
    select
        {{ transform_id_columns([...]) }},
        "Gender" as gender,
        "Birthdate" as birth_date,
        ...
    from source_data
)

select * from renamed_columns
```

## Questions for Discussion

1. **Scope:** Should we refactor all models or focus on high-priority ones?
2. **Timeline:** What's the target timeline for completion?
3. **Testing:** What level of testing is required before deploying refactored models?
4. **Documentation:** Should we add column-level documentation explaining why each column is needed?
5. **Automation:** Can we create a script to help identify which columns are used in each model?
6. **Standards:** Should we add this as a coding standard for new models?

## Conclusion

While `select *` is convenient and common, explicit column selection provides better long-term reliability, especially when dealing with external source systems like OpenDental. The refactor is significant but can be done incrementally with proper testing. The benefits (reliability, performance, maintainability) outweigh the costs (time, effort).

**Recommendation:** Proceed with Phase 1 (staging models) using incremental refactor approach, starting with Tier 1 critical business tables.
