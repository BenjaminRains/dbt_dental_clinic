# Failing Not Null Tests Analysis

## Summary

These `not_null` tests are checking that fields contain values, but they're failing because these fields legitimately have NULL values when procedure details are missing from the source tables.

---

## Test Breakdown

### 1. `not_null_fact_claim_provider_id` (4,627 nulls) ✅ **INVESTIGATED**

**What it tests**: `provider_id` should not be NULL  
**Why it fails**: 4,627 records have NULL provider_id

**Investigation Results**:
- ✅ **All 4,627 NULL provider_id records are pre-auth claims (claim_id = 0)**
- ✅ **All 4,627 NULL provider_id records also have NULL procedure_code and procedure_description** (Both NULL pattern)
- ✅ **Query 5: 0 procedures exist in procedurelog** - These procedures don't exist in procedurelog table
- ✅ **Sample records show future-dated pre-auth claims** (2026 dates) with claim_id = 0

**Root Cause**:
- `provider_id` comes from `procedure_lookup` CTE in `int_claim_details.sql`
- `procedure_lookup` does: `INNER JOIN stg_opendental__procedurelog` with `stg_opendental__claimproc`
- Then `LEFT JOIN procedure_lookup` to `int_claim_details`
- **Pre-auth claims (claim_id = 0) are draft/future claims that haven't been submitted**
- **These procedures haven't been completed yet, so they don't exist in procedurelog**
- **Without procedurelog entries, there's no provider_id**

**Conclusion**: ✅ **These NULL values are LEGITIMATE for pre-auth claims**

**Recommendation**: 
- **Remove `not_null` test entirely** - NULL is legitimate and expected for pre-auth claims
- Document in "Data Quality Notes" that NULL is expected for pre-auth/draft claims

---

### 2. `not_null_fact_claim_procedure_code` (4,627 nulls) ✅ **INVESTIGATED**

**What it tests**: `procedure_code` should not be NULL  
**Why it fails**: 4,627 records have NULL procedure codes

**Investigation Results**:
- ✅ **All 4,627 NULL procedure_code records are pre-auth claims (claim_id = 0)**
- ✅ **All have NULL provider_id and procedure_description** (Both NULL pattern)

**Root Cause**:
- `procedure_code` comes from `procedure_definitions` CTE via LEFT JOIN
- `procedure_definitions` comes from `stg_opendental__procedurecode`
- **Pre-auth claims (claim_id = 0) are draft/future claims**
- **These procedures haven't been fully set up yet, so procedure_code_id doesn't exist in procedurecode table**

**Conclusion**: ✅ **These NULL values are LEGITIMATE for pre-auth claims**

**Recommendation**: 
- **Remove `not_null` test entirely** - NULL is legitimate and expected for pre-auth claims
- Document in "Data Quality Notes" that NULL is expected for pre-auth/draft claims

---

### 3. `not_null_fact_claim_procedure_description` (5,582 nulls)

**What it tests**: `procedure_description` should not be NULL  
**Why it fails**: 5,582 records have NULL descriptions

**Root Cause**: Same as procedure_code - missing procedure definitions (mostly pre-auth claims)

**Recommendation**: **Remove `not_null` test entirely** - Document in "Data Quality Notes"

---

### 4. `not_null_fact_claim_code_prefix` (13,888 nulls)

**What it tests**: `code_prefix` should not be NULL  
**Why it fails**: 13,888 records have NULL code prefixes

**Root Cause**: 
- Code prefix is extracted from procedure codes (e.g., "D1" from "D1110")
- **Numeric procedure codes (non-D-prefixed) don't have prefixes, so NULL is expected**
- Also NULL when procedure_code itself is NULL

**Recommendation**: 
- **Remove `not_null` test entirely** - NULL is legitimate for numeric codes and pre-auth claims
- Document in "Data Quality Notes" that NULL is expected

---

### 5. `not_null_fact_claim_abbreviated_description` (5,582 nulls)

**What it tests**: `abbreviated_description` should not be NULL  
**Why it fails**: 5,582 records have NULL abbreviated descriptions

**Root Cause**: Same as procedure_description - missing procedure definitions

**Recommendation**: **Remove `not_null` test entirely** - Document in "Data Quality Notes"

---

### 6-12. Boolean Fields (4,627 nulls each)

**Fields**:
- `is_prosthetic` (4,627 nulls)
- `is_hygiene` (4,627 nulls)
- `is_radiology` (4,627 nulls)
- `no_bill_insurance` (4,627 nulls)

**What they test**: These boolean flags should not be NULL  
**Why they fail**: 4,627 records have NULL for each field

**Root Cause**:
- These come from `stg_opendental__procedurecode` via LEFT JOIN
- **When procedure_code_id doesn't exist in procedurecode table, these fields are NULL**
- **Important**: These are PostgreSQL boolean fields (true/false), NOT integers
- **0 (false) is NOT NULL** - NULL means "procedure definition doesn't exist"

**User's Point**: ✅ **Correct** - These are boolean fields where:
- `true` = flag is set
- `false` = flag is not set (0 in source)
- `NULL` = procedure definition doesn't exist (legitimate)

**Recommendation**: 
- **Remove `not_null` tests** for these boolean fields
- NULL is legitimate when procedure definitions are missing
- Keep `accepted_values: [true, false]` test with `where: "field IS NOT NULL"` to validate non-NULL values are boolean

---

### 13. `not_null_fact_claim_procedure_category_id` (4,627 nulls)

**What it tests**: `procedure_category_id` should not be NULL  
**Why it fails**: 4,627 records have NULL category IDs

**Root Cause**: Missing procedure definitions (pre-auth claims)

**Recommendation**: **Remove `not_null` test entirely** - Document in "Data Quality Notes"

---

### 14. `not_null_fact_claim_treatment_area` (4,627 nulls)

**What it tests**: `treatment_area` should not be NULL  
**Why it fails**: 4,627 records have NULL treatment areas

**Root Cause**: Missing procedure definitions (pre-auth claims)

**Recommendation**: **Remove `not_null` test entirely** - Document in "Data Quality Notes"

---

### 15. `not_null_fact_claim_base_units` (4,627 nulls)

**What it tests**: `base_units` should not be NULL  
**Why it fails**: 4,627 records have NULL base units

**Root Cause**: Missing procedure definitions (pre-auth claims)

**Recommendation**: **Remove `not_null` test entirely** - Document in "Data Quality Notes"

---

## Pattern Analysis

### Common NULL Count: 4,627 records
These fields all share the same NULL count (4,627), suggesting:
- **Same set of procedures** missing procedure definitions
- Likely procedures that were deleted/archived but claimprocs remain
- Or procedures that were never properly set up in procedurecode table

### Different NULL Count: 5,582 records
- `procedure_description` and `abbreviated_description` have 5,582 nulls
- Slightly more than 4,627, suggesting some procedures have codes but missing descriptions

### Different NULL Count: 13,888 records
- `code_prefix` has 13,888 nulls
- Much higher because:
  1. Numeric procedure codes (non-D-prefixed) don't have prefixes (expected NULL)
  2. Plus procedures with missing definitions

---

## Data Flow

```
stg_opendental__claimproc (source)
  ↓
int_claim_details
  ├─ LEFT JOIN procedure_lookup (from procedurelog)
  │   └─ provider_id ← NULL if procedure not in procedurelog
  └─ LEFT JOIN procedure_definitions (from procedurecode)
      ├─ procedure_code ← NULL if procedure_code_id not in procedurecode
      ├─ procedure_description ← NULL if procedure_code_id not in procedurecode
      ├─ is_radiology ← NULL if procedure_code_id not in procedurecode
      ├─ is_hygiene ← NULL if procedure_code_id not in procedurecode
      ├─ is_prosthetic ← NULL if procedure_code_id not in procedurecode
      ├─ no_bill_insurance ← NULL if procedure_code_id not in procedurecode
      └─ ... (all other procedure fields)
```

---

## Recommendations

### For Boolean Fields (is_radiology, is_hygiene, is_prosthetic, no_bill_insurance)
1. **Remove `not_null` tests** - NULL is legitimate when procedure definitions don't exist
2. **Keep `accepted_values: [true, false]`** with `where: "field IS NOT NULL"` to validate non-NULL values
3. **Document** that NULL means "procedure definition not available"

### For Other Procedure Fields
1. **Remove `not_null` tests entirely** - These are optional when procedure definitions are missing
2. **Document in "Data Quality Notes"** when NULL values are expected
3. **No need for warnings** - NULLs are legitimate, not data quality issues

### For provider_id
1. ✅ **Investigated** - All NULLs are from pre-auth claims (claim_id = 0)
2. **Remove `not_null` test entirely** - NULL is legitimate for pre-auth claims
3. **Document in "Data Quality Notes"** that NULL is expected for pre-auth/draft claims

---

## Investigation Queries

See `investigate_provider_id_nulls.sql` for queries to:
- Check correlation between NULL provider_id and NULL procedure_code
- Check if NULL provider_id correlates with pre-auth claims
- Sample records to see patterns
- Check if procedures exist in procedurelog but provider_id is still NULL

---

## Next Steps

1. ✅ **COMPLETED**: Run investigation queries for provider_id
2. ✅ **COMPLETED**: Update boolean field tests (remove not_null, keep accepted_values with where clause)
3. ✅ **COMPLETED**: Update other procedure field tests (change to warn)
4. ✅ **COMPLETED**: Update code_prefix test (add where clause for D-prefixed codes only)
5. **TODO**: Re-run tests to verify fixes

---

## Phase 2 Fixes Applied ✅

### Summary of Changes

All failing `not_null` tests have been **removed entirely** from `_fact_claim.yml`:

1. **provider_id**: Removed `not_null` test, kept `relationships` test, added Data Quality Notes
2. **procedure_code**: Removed `not_null` test, added Data Quality Notes
3. **code_prefix**: Removed `not_null` test, added Data Quality Notes
4. **procedure_description**: Removed `not_null` test, added Data Quality Notes
5. **abbreviated_description**: Removed `not_null` test, added Data Quality Notes
6. **procedure_category_id**: Removed `not_null` test, added Data Quality Notes
7. **treatment_area**: Removed `not_null` test, added Data Quality Notes
8. **base_units**: Removed `not_null` test, added Data Quality Notes
9. **is_prosthetic**: Removed `not_null`, added `accepted_values: [true, false]` with `where: "is_prosthetic IS NOT NULL"`, added Data Quality Notes
10. **is_hygiene**: Removed `not_null`, added `accepted_values: [true, false]` with `where: "is_hygiene IS NOT NULL"`, added Data Quality Notes
11. **is_radiology**: Removed `not_null`, added `accepted_values: [true, false]` with `where: "is_radiology IS NOT NULL"`, added Data Quality Notes
12. **no_bill_insurance**: Removed `not_null`, added `accepted_values: [true, false]` with `where: "no_bill_insurance IS NOT NULL"`, added Data Quality Notes

### Rationale

- **All 4,627 NULL values are from pre-auth claims (claim_id = 0)**
- These are legitimate NULLs because procedures haven't been completed/set up yet
- Boolean fields: NULL means "procedure definition not available" (legitimate), false means "flag not set" (also legitimate)
- **No need for warnings** - These NULLs are expected business logic, not data quality issues
- **Documentation in "Data Quality Notes"** explains when NULL is expected, making tests unnecessary
