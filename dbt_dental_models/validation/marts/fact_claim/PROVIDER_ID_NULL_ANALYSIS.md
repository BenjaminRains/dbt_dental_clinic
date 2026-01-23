# Provider ID NULL Analysis for fact_claim

**Valid as of**: 2026-01-23

## Executive Summary

**Issue**: 4,627 records (4.96%) in `fact_claim` have NULL `provider_id` values.

**Root Cause**: All NULL `provider_id` values occur in **pre-authorization claims** (`claim_id = 0`). These claims reference procedures that haven't been completed yet, so they don't exist in `procedurelog`. Since `provider_id` is sourced from `procedurelog` via the `procedure_lookup` CTE in `int_claim_details`, these records cannot have a `provider_id`.

**Status**: ✅ **EXPECTED BEHAVIOR** - This is documented in `_fact_claim.yml` as legitimate business data.

---

## Key Findings

### Query Results Summary

1. **Overall Pattern**:
   - Total records (2023+): 93,215
   - NULL provider_id: 4,627 (4.96%)
   - Non-NULL provider_id: 88,588 (95.04%)

2. **NULL Pattern Correlation**:
   - When `provider_id` is NULL, `procedure_code` is also NULL (4,627 records)
   - This indicates both fields depend on the same join path through `procedure_lookup`

3. **Claim Type Distribution**:
   - **Pre-auth claims** (`claim_id = 0`): 48,233 total records
     - NULL provider_id: 4,627 (9.6% of pre-auth claims)
     - Non-NULL provider_id: 43,606 (90.4% of pre-auth claims)
   - **Regular claims** (`claim_id != 0`): 44,982 total records
     - NULL provider_id: 0 (0%)
     - Non-NULL provider_id: 44,982 (100%)

4. **Sample Records**:
   - All NULL provider_id records have `claim_id = 0`
   - Many have `procedure_id = 0` (placeholder for procedures not yet created)
   - All have NULL `procedure_code` and `procedure_description`
   - Recent dates (2026-01-07 to 2026-01-14) suggest active pre-auth workflow

5. **Procedure Existence Check**:
   - 0 procedures with NULL provider_id exist in `procedurelog`
   - This confirms the root cause: procedures haven't been completed yet

---

## Technical Root Cause Analysis

### Data Flow in `int_claim_details.sql`

1. **Source**: `stg_opendental__claimproc` (claim procedures)
2. **Provider Lookup**: `procedure_lookup` CTE (lines 74-82)
   ```sql
   procedure_lookup as (
       select distinct
           pl.procedure_id,
           pl.procedure_code_id,
           pl.provider_id
       from {{ ref('stg_opendental__procedurelog') }} pl
       inner join {{ ref('stg_opendental__claimproc') }} cp
           on pl.procedure_id = cp.procedure_id
   )
   ```
3. **Join**: LEFT JOIN to `procedure_lookup` (line 219-220)
   ```sql
   left join procedure_lookup pl
       on cp.procedure_id = pl.procedure_id
   ```

### Why NULL Occurs

- **Pre-auth claims** (`claim_id = 0`) are created **before** procedures are performed
- These procedures don't exist in `procedurelog` yet (they're planned, not completed)
- The `procedure_lookup` CTE uses an **INNER JOIN**, so it only includes procedures that exist in both tables
- When a procedure doesn't exist in `procedurelog`, the LEFT JOIN returns NULL for `provider_id`

### Why Some Pre-auth Claims Have Provider IDs

- Some pre-auth claims reference procedures that **have already been completed**
- These procedures exist in `procedurelog`, so `provider_id` can be populated
- This represents cases where:
  - A procedure was completed, then a pre-auth was created retroactively
  - A pre-auth was created for a procedure that was already in progress
  - The procedure was completed between pre-auth creation and data extraction

---

## Business Context

### What are Pre-authorization Claims?

According to `_int_claim_snapshot.yml` and `_fact_claim.yml`:
- `claim_id = 0` represents **pre-authorization requests or draft claims not yet submitted**
- These are sent to insurance **BEFORE** procedures are performed to get approval
- They represent **planned/scheduled procedures** that haven't occurred yet
- They may have **future-dated claim_date values** (representing when procedures are planned)

### Why Provider ID is NULL

- Pre-auth claims are created for **planned procedures** that haven't been performed
- The provider who will perform the procedure may not be assigned yet
- Even if assigned, the procedure doesn't exist in `procedurelog` until it's completed
- Therefore, `provider_id` cannot be determined from the current data model

---

## Documentation Status

The NULL `provider_id` for pre-auth claims is **already documented** in `_fact_claim.yml`:

```yaml
- name: provider_id
  description: >
    ...
    Data Quality Notes:
    - NULL is expected for pre-auth claims (claim_id = 0) where procedures haven't been completed yet
    - These procedures don't exist in procedurelog, so provider_id cannot be determined
```

**Conclusion**: This is **expected behavior**, not a data quality issue.

---

## Potential Solutions (If Provider ID is Needed)

### Option 1: Accept NULL (Recommended)
- **Status**: Current implementation
- **Pros**: Accurate representation of data reality
- **Cons**: Cannot analyze provider performance for pre-auth claims
- **Recommendation**: ✅ **Keep as-is** - This is the most accurate representation

### Option 2: Get Provider from Treatment Plans
- **Approach**: Join to `proctp` table to get planned provider
- **Pros**: May provide provider for some pre-auth claims
- **Cons**: 
  - ❌ **Investigation Results**: 0 procedures found in treatment plans (Query 7-8)
  - Pre-auth claims are not linked to treatment plans
  - Treatment plan provider may differ from actual performing provider
  - Adds complexity to the model
- **Status**: ❌ **Not viable** - No coverage for pre-auth claims

### Option 3: Get Provider from Appointments ⚠️
- **Approach**: Join to `appointment` table by patient_id and date
- **⚠️ Implementation Guide Below**: See detailed SQL implementation and important data quality notes
- **Pros**: 
  - ✅ **Investigation Results**: 592 procedures (13.5%) have appointments with provider information
  - ✅ **Coverage**: 949 records (20.5% of NULL provider records) can get provider from appointments
  - ✅ **5 distinct providers** identified from appointments
  - Provides provider for scheduled procedures
- **Cons**:
  - Only covers ~20% of NULL provider records
  - ⚠️ **Important**: Appointment provider may differ from actual performing provider
  - Date matching may be inaccurate (exact date match required)
  - Adds complexity to the model
- **Status**: ⚠️ **Partially viable** - Can populate ~20% of NULL provider records

#### Implementation Guide for Option 3

If `provider_id` is critical for pre-auth analysis, you can implement a fallback to appointments. Here's how to modify `int_claim_details.sql`:

**Step 1**: Add appointment lookup CTE (after `procedure_lookup`):

```sql
appointment_provider_lookup as (
    select distinct
        a.patient_id,
        a.appointment_datetime::date as appointment_date,
        a.provider_id as appointment_provider_id
    from {{ ref('stg_opendental__appointment') }} a
    where a.provider_id is not null
        and a.appointment_datetime >= '2023-01-01'
),
```

**Step 2**: Modify the provider_id selection in `claim_details_integrated`:

```sql
-- Original: pl.provider_id
-- Modified: Use COALESCE to prefer procedurelog provider, fallback to appointment provider
coalesce(
    pl.provider_id,  -- Primary: from procedurelog (for completed procedures)
    apt.provider_id  -- Fallback: from appointments (for pre-auth claims with scheduled appointments)
) as provider_id,
```

**Step 3**: Add LEFT JOIN to appointment lookup (after procedure_lookup join):

```sql
left join appointment_provider_lookup apt
    on coalesce(cp.patient_id, c.patient_id) = apt.patient_id
    and coalesce(c.claim_date, cp.procedure_date)::date = apt.appointment_date
```

**Complete Example** (relevant section of `int_claim_details.sql`):

```sql
procedure_lookup as (
    select distinct
        pl.procedure_id,
        pl.procedure_code_id,
        pl.provider_id
    from {{ ref('stg_opendental__procedurelog') }} pl
    inner join {{ ref('stg_opendental__claimproc') }} cp
        on pl.procedure_id = cp.procedure_id
),

appointment_provider_lookup as (
    select distinct
        a.patient_id,
        a.appointment_datetime::date as appointment_date,
        a.provider_id as appointment_provider_id
    from {{ ref('stg_opendental__appointment') }} a
    where a.provider_id is not null
        and a.appointment_datetime >= '2023-01-01'
),

-- ... other CTEs ...

claim_details_integrated as (
    select
        -- ... other fields ...
        
        -- Provider ID: prefer procedurelog, fallback to appointment
        coalesce(
            pl.provider_id,           -- Primary source: completed procedures
            apt.appointment_provider_id  -- Fallback: scheduled appointments for pre-auth
        ) as provider_id,
        
        -- ... other fields ...
        
    from source_claim_proc cp
    left join source_claim c
        on cp.claim_id = c.claim_id
    left join procedure_lookup pl
        on cp.procedure_id = pl.procedure_id
    left join appointment_provider_lookup apt
        on coalesce(cp.patient_id, c.patient_id) = apt.patient_id
        and coalesce(c.claim_date, cp.procedure_date)::date = apt.appointment_date
    -- ... other joins ...
)
```

**Important Notes**:
- ⚠️ **Data Quality Consideration**: The appointment provider (`apt.provider_id`) represents the **scheduled provider** for the appointment, which may differ from the **actual performing provider** who completes the procedure. This is especially true if:
  - Appointments are rescheduled or reassigned
  - Procedures are performed by a different provider than scheduled
  - Multiple providers work on the same appointment
- **Coverage**: This will populate `provider_id` for approximately **20% of pre-auth claims** (949 out of 4,627 records)
- **Remaining NULLs**: ~80% of pre-auth claims will still have NULL `provider_id` because they don't have matching appointments
- **Recommendation**: Consider adding a `provider_source` field to track whether provider came from `procedurelog` or `appointment`:
  ```sql
  case 
      when pl.provider_id is not null then 'procedurelog'
      when apt.appointment_provider_id is not null then 'appointment'
      else null
  end as provider_source
  ```

### Option 4: Get Provider from Claimproc/Claim Tables
- **Approach**: Check if source tables have provider information
- **Pros**: Direct from source
- **Cons**: 
  - Likely NULL in source (that's why it's not in procedurelog)
  - Would require source system investigation
- **Investigation Needed**: Run Query 9 to check source data

### Option 5: Use Default/Assigned Provider
- **Approach**: Use patient's primary provider or claim's assigned provider
- **Pros**: Provides a value for all records
- **Cons**:
  - May be inaccurate (not the actual performing provider)
  - Could mislead analysis
- **Recommendation**: ❌ **Not recommended** - Better to have NULL than inaccurate data

---

## Recommended Actions

### Immediate Actions
1. ✅ **Documentation**: Already complete - NULL is documented as expected
2. ✅ **Validation**: Update any tests to allow NULL for `claim_id = 0` records
3. ⚠️ **Analysis Impact**: Document that provider-level analysis excludes pre-auth claims

### Investigation Actions (If Provider ID is Critical)
1. ✅ **Completed**: Ran diagnostic queries (Queries 6-11) to assess alternative data sources
2. **Evaluate business need**: Is `provider_id` required for pre-auth claims?
3. **If needed, implement Option 3** (Appointments):
   - See **Implementation Guide** in Option 3 section above for complete SQL code
   - Can populate ~20% of NULL provider records (949 out of 4,627)
   - ⚠️ **Important**: Appointment provider may differ from actual performing provider
     - Appointment provider = scheduled provider
     - Actual performing provider may be different
     - Use with caution in provider performance analysis

### Long-term Considerations
1. **Business Rule**: Determine if pre-auth claims should have provider_id
2. **Source System**: Check if OpenDental can provide provider for pre-auth claims
3. **Data Model**: Consider adding a `planned_provider_id` field separate from `provider_id`

---

## Additional Diagnostic Queries

The investigation SQL file now includes queries 6-11 to:
- Verify procedures don't exist in procedurelog (Query 6)
- Check treatment plans as alternative source (Queries 7-8)
- Check source claimproc table (Query 9)
- Check appointments as alternative source (Query 10)
- Summary analysis of all potential sources (Query 11)

**Next Steps**: Run queries 6-11 to assess if alternative data sources can populate provider_id for pre-auth claims.

### Query Results Summary

**Query 6 - Procedure Existence Verification**:
- 3,282 distinct procedures not in `procedurelog`
- 4,627 total records not in `procedurelog`
- 1 procedure with `procedure_id = 0` (placeholder)
- 3,281 procedures with non-zero `procedure_id` (planned but not completed)
- ✅ **Confirms root cause**: Procedures haven't been completed yet

**Query 7 - Treatment Plans Coverage**:
- 0 procedures found in treatment plans (`proctp`)
- 0 records with treatment plan provider information
- ❌ **Treatment plans don't help**: Pre-auth procedures aren't linked to treatment plans

**Query 8 - Treatment Plan Sample**:
- All 20 sample records show NULL for `treatplan_provider_id`
- Confirms no matches between pre-auth claims and treatment plans
- ❌ **No provider information available from treatment plans**

**Query 9 - Source Claimproc Analysis**:
- 3,282 distinct procedures in `claimproc` that don't exist in `procedurelog`
- 4,627 total `claimproc` records with `claim_id = 0` and no matching `procedurelog` entry
- 1 procedure with `procedure_id = 0` (placeholder)
- 3,281 procedures with non-zero `procedure_id`
- ✅ **Confirms**: Source data shows these are pre-auth procedures not yet completed

**Query 10 - Appointments Coverage**:
- 592 distinct procedures with appointments
- 949 total records with appointments (20.5% of NULL provider records)
- 5 distinct providers identified from appointments
- ✅ **Appointments provide provider information**: ~20% of NULL provider records have matching appointments

**Query 11 - Summary Analysis**:
- Total NULL provider records: 4,375 (distinct claim/procedure combinations)
- Procedures in procedurelog: 0 (confirms root cause)
- Procedures in treatplan: 0 (confirms treatment plans don't help)
- Procedures with appointments: 592 (13.5% of distinct procedures)
- Procedures with procedure_id = 0: 1 (placeholder procedure)

**Key Findings**:
1. **Treatment plans (`proctp`) do NOT provide provider information** for pre-auth claims. This suggests these pre-auth claims are created independently of formal treatment plans, or the procedures are too early in the planning process to have assigned providers.

2. **Appointments DO provide provider information** for approximately 20% of NULL provider records:
   - 592 distinct procedures (13.5% of distinct procedures) have appointments
   - 949 total records (20.5% of NULL provider records) have appointments
   - 5 distinct providers can be identified from appointments
   - This represents a viable option for populating provider_id for some pre-auth claims

---

## Conclusion

The NULL `provider_id` values in `fact_claim` are **expected and documented** behavior for pre-authorization claims. These represent procedures that haven't been completed yet, so they don't exist in `procedurelog` where `provider_id` is sourced.

### Investigation Summary

**Alternative Data Sources Evaluated**:
1. ❌ **Treatment Plans (`proctp`)**: 0 procedures found - Not viable
2. ✅ **Appointments**: 592 procedures (13.5%) / 949 records (20.5%) - Partially viable
3. ❌ **Source Claimproc**: No provider information available - Not viable

**Recommendation**: 
- ✅ **Accept NULL values** for pre-auth claims as accurate representation (recommended default)
- ✅ **Document** in analysis that provider-level metrics exclude pre-auth claims
- ⚠️ **Optional Enhancement**: If `provider_id` is critical for pre-auth analysis, implement Option 3 (Appointments) as shown in the **Implementation Guide** above
  - This will populate `provider_id` for approximately **20% of pre-auth claims** (949 records)
  - ⚠️ **Critical Note**: Appointment provider may differ from actual performing provider
    - Appointment provider represents the **scheduled provider** for the appointment
    - The **actual performing provider** may be different if appointments are rescheduled, reassigned, or if multiple providers work on the same appointment
    - Use with caution in provider performance analysis
  - Consider adding a `provider_source` field to track data lineage

---

**Analysis Date**: 2026-01-23  
**Analyst**: Auto (AI Assistant)  
**Status**: ✅ Root cause identified and documented
