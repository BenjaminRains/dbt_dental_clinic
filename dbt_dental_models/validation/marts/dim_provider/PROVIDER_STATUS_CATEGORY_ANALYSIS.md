# Provider Status Category Investigation

**Date**: 2026-01-23  
**Status**: 🔍 **IN PROGRESS**  
**Investigation File**: `investigate_provider_status_category.sql`  
**Related Documentation**: `validation/VALIDATION_TEMPLATE.md` - Validation framework and template

---

## Executive Summary

**Issue**: Need to verify that `provider_status_category` correctly maps all `provider_status` values from the source system.

**Current Logic** (from `dim_provider.sql` lines 135-140):
```sql
case
    when p.termination_date is not null then 'Terminated'
    when p.provider_status = 0 then 'Active'
    when p.provider_status = 1 then 'Inactive'
    else 'Unknown'
end as provider_status_category
```

**Documented Valid Values** (from `_dim_provider.yml`):
- 'Active': Provider is currently active
- 'Inactive': Provider is temporarily inactive
- 'Terminated': Provider has been terminated
- 'Unknown': Status cannot be determined

**Investigation Goals**:
1. Verify what `provider_status` values exist in source data
2. Check if any records have `provider_status_category = 'Unknown'` that could be mapped
3. Verify if `provider_status` can be NULL and how that's handled
4. Confirm business rules for status categorization

---

## Investigation Queries

Run the following investigation queries to understand the data:

**Location**: `validation/marts/dim_provider/investigate_provider_status_category.sql`

**Key Queries**:
1. **Query 1**: Provider Status Distribution in Source
   - Distribution of `provider_status` values in staging
   - NULL value check
   
2. **Query 2**: Provider Status Category Distribution
   - Summary of `provider_status_category` values
   - Breakdown by `provider_status` and `termination_date`
   - Sample records for each category

3. **Query 3**: Unknown Status Category Analysis
   - Detailed analysis of Unknown records
   - Breakdown by `provider_status` values
   - Sample records

4. **Query 4**: Provider Status vs Status Category Comparison
   - Cross-tabulation of status and category
   - Check for unexpected mappings

5. **Query 5**: Provider Status Description Analysis
   - Check `provider_status_description` for Unknown records
   - All provider_status values and their descriptions

6. **Query 6**: Summary Statistics
   - Overall counts and percentages

---

## Expected Findings

Based on the current logic, we expect:
- **Active**: `provider_status = 0` AND `termination_date IS NULL`
- **Inactive**: `provider_status = 1` AND `termination_date IS NULL`
- **Terminated**: `termination_date IS NOT NULL` (regardless of provider_status)
- **Unknown**: Any other combination (e.g., `provider_status` values other than 0 or 1, or NULL provider_status)

**Questions to Answer**:
1. Are there `provider_status` values other than 0 and 1 in the source?
2. Can `provider_status` be NULL, and if so, what should it map to?
3. Are there any Unknown records that could be properly categorized?
4. Does the business logic correctly handle all edge cases?

---

## Investigation Results

*Results will be documented here after running investigation queries*

---

## Root Cause Analysis

*To be completed after investigation*

---

## Recommended Actions

*To be completed after investigation*

---

## Related Documentation

- **Investigation SQL**: `validation/marts/dim_provider/investigate_provider_status_category.sql`
- **Model Logic**: `models/marts/dim_provider.sql` (lines 135-140)
- **Model Documentation**: `models/marts/_dim_provider.yml` (lines 218-236)
- **Validation Framework**: `validation/README.md` - How validation works
