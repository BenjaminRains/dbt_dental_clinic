# Patient Category Mapping Investigation

**Date**: 2026-01-23  
**Status**: 🔍 **IN PROGRESS**  
**Investigation File**: `investigate_patient_category_mappings.sql`  
**Related Documentation**: `validation/VALIDATION_TEMPLATE.md` - Validation framework and template

---

## Executive Summary

**Issue**: Need to verify that `age_category`, `preferred_confirmation_method`, and `preferred_contact_method` correctly map all source values.

**Current Logic** (from `dim_patient.sql`):

### Age Category (lines 77-82):
```sql
case 
    when s.age < 18 then 'Minor'
    when s.age between 18 and 64 then 'Adult'
    when s.age >= 65 then 'Senior'
    else 'Unknown'
end as age_category
```

### Preferred Confirmation Method (lines 92-98):
```sql
case s.preferred_confirmation_method
    when 0 then 'None'
    when 2 then 'Email'
    when 4 then 'Text'
    when 8 then 'Phone'
    else 'Unknown'
end as preferred_confirmation_method
```

### Preferred Contact Method (lines 99-108):
```sql
case s.preferred_contact_method
    when 0 then 'None'
    when 2 then 'Email'
    when 3 then 'Mail'
    when 4 then 'Phone'
    when 5 then 'Text'
    when 6 then 'Other'
    when 8 then 'Portal'
    else 'Unknown'
end as preferred_contact_method
```

**Investigation Goals**:
1. Verify if `age` can be NULL and how that affects `age_category`
2. Check distribution of `preferred_confirmation_method` values in source
3. Check distribution of `preferred_contact_method` values in source
4. Identify any unmapped values that result in 'Unknown' categorizations
5. Determine if additional mappings are needed

---

## Investigation Queries

Run the following investigation queries to understand the data:

**Location**: `validation/marts/dim_patient/investigate_patient_category_mappings.sql`

**Key Queries**:

### Age Category (Query Set 1):
1. **Query 1.1**: Age distribution and NULL check
2. **Query 1.2**: Age Category distribution
3. **Query 1.3**: Age Category breakdown by age ranges
4. **Query 1.4**: Sample records for Unknown age_category

### Preferred Confirmation Method (Query Set 2):
1. **Query 2.1**: Distribution in source (staging)
2. **Query 2.2**: Distribution in mart
3. **Query 2.3**: Breakdown by source value and mapped category
4. **Query 2.4**: Unknown analysis
5. **Query 2.5**: Breakdown of Unknown by source values
6. **Query 2.6**: Sample records for Unknown

### Preferred Contact Method (Query Set 3):
1. **Query 3.1**: Distribution in source (staging)
2. **Query 3.2**: Distribution in mart
3. **Query 3.3**: Breakdown by source value and mapped category
4. **Query 3.4**: Unknown analysis
5. **Query 3.5**: Breakdown of Unknown by source values
6. **Query 3.6**: Sample records for Unknown

### Combined Analysis (Query Set 4):
1. **Query 4.1**: Patients with multiple Unknown categories
2. **Query 4.2**: Cross-tabulation of Unknown categories

### Summary Statistics (Query Set 5):
1. **Query 5.1**: Overall summary for all three fields
2. **Query 5.2**: Distinct source values summary

---

## Expected Findings

### Age Category:
- **Expected Mappings**:
  - `age < 18` → 'Minor'
  - `age BETWEEN 18 AND 64` → 'Adult'
  - `age >= 65` → 'Senior'
  - `age IS NULL` or other edge cases → 'Unknown'
- **Questions to Answer**:
  - Can `age` be NULL? If so, is this expected or a data quality issue?
  - Are there any negative ages or other edge cases?
  - Should NULL age map to 'Unknown' or be handled differently?

### Preferred Confirmation Method:
- **Currently Mapped**: `[0, 2, 4, 8]`
  - `0` = 'None'
  - `2` = 'Email'
  - `4` = 'Text'
  - `8` = 'Phone'
- **Questions to Answer**:
  - Are there other values in source (e.g., 1, 3, 5, 6, 7, 9+)?
  - What should unmapped values map to?
  - Are there any NULL values and how should they be handled?

### Preferred Contact Method:
- **Currently Mapped**: `[0, 2, 3, 4, 5, 6, 8]`
  - `0` = 'None'
  - `2` = 'Email'
  - `3` = 'Mail'
  - `4` = 'Phone'
  - `5` = 'Text'
  - `6` = 'Other'
  - `8` = 'Portal'
- **Questions to Answer**:
  - Are there other values in source (e.g., 1, 7, 9+)?
  - What should unmapped values map to?
  - Are there any NULL values and how should they be handled?

---

## Investigation Results

*Results will be documented here after running investigation queries*

---

## Root Cause Analysis

*To be completed after investigation*

---

## Recommended Actions

*To be completed after investigation*

**Potential Actions**:
1. **Age Category**:
   - If NULL ages are data quality issues, add data quality checks
   - If NULL ages are expected (e.g., for deceased patients), document business rules
   - Handle negative ages or other edge cases

2. **Preferred Confirmation Method**:
   - Map additional values if found in source
   - Document business rules for NULL values
   - Update CASE statement if needed

3. **Preferred Contact Method**:
   - Map additional values if found in source
   - Document business rules for NULL values
   - Update CASE statement if needed

4. **Documentation**:
   - Update `_dim_patient.yml` with complete mappings
   - Document business rules for edge cases
   - Add data quality notes

---

## Related Documentation

- **Investigation SQL**: `validation/marts/dim_patient/investigate_patient_category_mappings.sql`
- **Model Logic**: `models/marts/dim_patient.sql` (lines 77-108)
- **Model Documentation**: `models/marts/_dim_patient.yml` (need to verify)
- **Validation Framework**: `validation/README.md` - How validation works
- **Staging Model**: `models/staging/opendental/stg_opendental__patient.sql`
- **Intermediate Model**: `models/intermediate/foundation/int_patient_profile.sql`
