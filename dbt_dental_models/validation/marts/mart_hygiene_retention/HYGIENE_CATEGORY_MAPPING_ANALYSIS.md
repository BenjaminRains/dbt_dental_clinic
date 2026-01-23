# Hygiene Retention Category Mapping Investigation

**Date**: 2026-01-23  
**Status**: 🔍 **IN PROGRESS**  
**Investigation File**: `investigate_hygiene_category_mappings.sql`  
**Related Documentation**: `validation/VALIDATION_TEMPLATE.md` - Validation framework and template

---

## Executive Summary

**Issue**: Need to verify that `hygiene_status`, `retention_category`, and `patient_risk_category` correctly handle all scenarios and edge cases.

**Current Logic** (from `mart_hygiene_retention.sql`):

### Hygiene Status (lines 170-175):
```sql
case 
    when he.last_hygiene_date >= current_date - interval '6 months' then 'Current'
    when he.last_hygiene_date >= current_date - interval '9 months' then 'Due'
    when he.last_hygiene_date >= current_date - interval '12 months' then 'Overdue'
    else 'Lapsed'
end as hygiene_status
```

### Retention Category (lines 177-182):
```sql
case 
    when he.hygiene_visits_last_year >= 2 and he.regular_interval_percentage >= 80 then 'Excellent'
    when he.hygiene_visits_last_year >= 2 and he.regular_interval_percentage >= 60 then 'Good'
    when he.hygiene_visits_last_year >= 1 then 'Fair'
    else 'Poor'
end as retention_category
```

### Patient Risk Category (lines 184-189):
```sql
case 
    when he.last_hygiene_date < current_date - interval '12 months' then 'High Risk'
    when he.hygiene_no_show_rate > 20 then 'Medium Risk'
    when he.avg_hygiene_interval_days > 240 then 'Medium Risk'
    else 'Low Risk'
end as patient_risk_category
```

**Investigation Goals**:
1. Verify that 'Lapsed' is an appropriate catch-all for `hygiene_status`
2. Check if `retention_category` logic covers all scenarios correctly
3. Verify `patient_risk_category` logic handles all edge cases
4. Identify any NULL value handling issues
5. Check for unexpected categorizations

---

## Investigation Queries

Run the following investigation queries to understand the data:

**Location**: `validation/marts/mart_hygiene_retention/investigate_hygiene_category_mappings.sql`

**Key Queries**:

### Hygiene Status (Query Set 1):
1. **Query 1.1**: Hygiene Status distribution
2. **Query 1.2**: Breakdown by last_hygiene_date and days_since_last_hygiene
3. **Query 1.3**: 'Lapsed' category detailed analysis
4. **Query 1.4**: Sample records for 'Lapsed' category
5. **Query 1.5**: Edge cases - potential misclassifications

### Retention Category (Query Set 2):
1. **Query 2.1**: Retention Category distribution
2. **Query 2.2**: Breakdown by key factors
3. **Query 2.3**: Edge cases analysis
4. **Query 2.4**: Sample records for each category

### Patient Risk Category (Query Set 3):
1. **Query 3.1**: Patient Risk Category distribution
2. **Query 3.2**: Breakdown by key factors
3. **Query 3.3**: Edge cases analysis
4. **Query 3.4**: Sample records for each category

### Combined Analysis (Query Set 4):
1. **Query 4.1**: Cross-tabulation of hygiene_status and retention_category
2. **Query 4.2**: Cross-tabulation of hygiene_status and patient_risk_category
3. **Query 4.3**: Patients with unexpected combinations

### Summary Statistics (Query Set 5):
1. **Query 5.1**: Overall summary for all three fields
2. **Query 5.2**: NULL value check for key fields

---

## Expected Findings

### Hygiene Status:
- **Expected Mappings**:
  - `last_hygiene_date >= current_date - interval '6 months'` → 'Current'
  - `last_hygiene_date >= current_date - interval '9 months'` → 'Due'
  - `last_hygiene_date >= current_date - interval '12 months'` → 'Overdue'
  - All other cases (including NULL) → 'Lapsed'
- **Questions to Answer**:
  - Is 'Lapsed' appropriate for all cases that don't match other conditions?
  - How should NULL `last_hygiene_date` be handled?
  - Are there patients with recent hygiene dates still classified as 'Lapsed'?

### Retention Category:
- **Expected Mappings**:
  - `hygiene_visits_last_year >= 2 AND regular_interval_percentage >= 80` → 'Excellent'
  - `hygiene_visits_last_year >= 2 AND regular_interval_percentage >= 60` → 'Good'
  - `hygiene_visits_last_year >= 1` → 'Fair'
  - All other cases → 'Poor'
- **Questions to Answer**:
  - Does the logic cover all scenarios correctly?
  - How should NULL `regular_interval_percentage` be handled?
  - Are there edge cases where retention is misclassified?

### Patient Risk Category:
- **Expected Mappings**:
  - `last_hygiene_date < current_date - interval '12 months'` → 'High Risk'
  - `hygiene_no_show_rate > 20` → 'Medium Risk'
  - `avg_hygiene_interval_days > 240` → 'Medium Risk'
  - All other cases → 'Low Risk'
- **Questions to Answer**:
  - Does the logic cover all scenarios correctly?
  - How should NULL values be handled (last_hygiene_date, hygiene_no_show_rate, avg_hygiene_interval_days)?
  - Are there edge cases where risk is misclassified?

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
1. **Hygiene Status**:
   - Verify 'Lapsed' is appropriate catch-all
   - Handle NULL `last_hygiene_date` cases explicitly
   - Check for patients with recent hygiene dates still classified as 'Lapsed'

2. **Retention Category**:
   - Verify all logic branches are covered
   - Handle NULL `regular_interval_percentage` cases
   - Check for misclassifications

3. **Patient Risk Category**:
   - Verify all logic branches are covered
   - Handle NULL values appropriately (last_hygiene_date, hygiene_no_show_rate, avg_hygiene_interval_days)
   - Check for misclassifications

4. **Documentation**:
   - Update `_mart_hygiene_retention.yml` with complete logic documentation
   - Document edge cases and business rules
   - Add data quality notes

---

## Related Documentation

- **Investigation SQL**: `validation/marts/mart_hygiene_retention/investigate_hygiene_category_mappings.sql`
- **Model Logic**: `models/marts/mart_hygiene_retention.sql` (lines 170-189)
- **Model Documentation**: `models/marts/_mart_hygiene_retention.yml` (lines 754-810)
- **Validation Framework**: `validation/README.md` - How validation works
