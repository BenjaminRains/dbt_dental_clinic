# Patient Retention Category Mapping Investigation

**Date**: 2026-01-23  
**Status**: 🔍 **IN PROGRESS**  
**Investigation File**: `investigate_retention_category_mappings.sql`  
**Related Documentation**: `validation/VALIDATION_TEMPLATE.md` - Validation framework and template

---

## Executive Summary

**Issue**: Need to verify that `retention_status`, `churn_risk_category`, and `patient_value_category` correctly handle all scenarios and edge cases.

**Current Logic** (from `mart_patient_retention.sql`):

### Retention Status (lines 287-295):
```sql
case 
    when pa.appointments_last_30_days > 0 then 'Active'
    when pa.appointments_last_90_days > 0 then 'Recent'
    when pa.appointments_last_6_months > 0 then 'Moderate'
    when pa.appointments_last_year > 0 then 'Dormant'
    when pa.appointments_last_2_years > 0 then 'Inactive'
    when pa.last_appointment_date > current_date then 'Scheduled'
    else 'Lost'
end as retention_status
```

### Churn Risk Category (lines 298-306):
```sql
case 
    when pg.days_since_last_visit is null and pa.last_appointment_date > current_date then 'Low Risk'
    when pg.days_since_last_visit > 365 then 'High Risk'
    when pg.days_since_last_visit > 180 and pa.appointments_last_year = 0 then 'High Risk'
    when pg.days_since_last_visit > 120 and pa.no_show_appointments::numeric / nullif(pa.total_appointments, 0) > 0.3 then 'Medium Risk'
    when pg.days_since_last_visit > 90 then 'Medium Risk'
    when pa.appointments_last_year < 2 and pg.avg_days_between_appointments > 180 then 'Medium Risk'
    else 'Low Risk'
end as churn_risk_category
```

### Patient Value Category (lines 309-315):
```sql
case 
    when coalesce(pp.lifetime_production, 0) = 0 then 'No Production'
    when coalesce(pp.lifetime_production, 0) < 500 then 'Low Value'
    when coalesce(pp.lifetime_production, 0) < 2000 then 'Medium Value'
    when coalesce(pp.lifetime_production, 0) < 5000 then 'High Value'
    else 'VIP'
end as patient_value_category
```

**Investigation Goals**:
1. Verify that 'Lost' is an appropriate catch-all for `retention_status`
2. Check if `churn_risk_category` logic covers all scenarios correctly
3. Verify `patient_value_category` ranges are complete and correct
4. Identify any edge cases or unexpected categorizations
5. Check for NULL value handling issues

---

## Investigation Queries

Run the following investigation queries to understand the data:

**Location**: `validation/marts/mart_patient_retention/investigate_retention_category_mappings.sql`

**Key Queries**:

### Retention Status (Query Set 1):
1. **Query 1.1**: Retention Status distribution
2. **Query 1.2**: Breakdown by appointment activity fields
3. **Query 1.3**: 'Lost' category detailed analysis
4. **Query 1.4**: Sample records for 'Lost' category
5. **Query 1.5**: Edge cases - potential misclassifications

### Churn Risk Category (Query Set 2):
1. **Query 2.1**: Churn Risk Category distribution
2. **Query 2.2**: Breakdown by key factors
3. **Query 2.3**: Edge cases analysis
4. **Query 2.4**: Sample records for each category

### Patient Value Category (Query Set 3):
1. **Query 3.1**: Patient Value Category distribution
2. **Query 3.2**: Breakdown by lifetime_production ranges
3. **Query 3.3**: Edge cases analysis
4. **Query 3.4**: Sample records for each category

### Combined Analysis (Query Set 4):
1. **Query 4.1**: Cross-tabulation of retention_status and churn_risk_category
2. **Query 4.2**: Cross-tabulation of retention_status and patient_value_category
3. **Query 4.3**: Patients with unexpected combinations

### Summary Statistics (Query Set 5):
1. **Query 5.1**: Overall summary for all three fields
2. **Query 5.2**: NULL value check for key fields

---

## Expected Findings

### Retention Status:
- **Expected Mappings**:
  - `appointments_last_30_days > 0` → 'Active'
  - `appointments_last_90_days > 0` → 'Recent'
  - `appointments_last_6_months > 0` → 'Moderate'
  - `appointments_last_year > 0` → 'Dormant'
  - `appointments_last_2_years > 0` → 'Inactive'
  - `last_appointment_date > current_date` → 'Scheduled'
  - All other cases → 'Lost'
- **Questions to Answer**:
  - Is 'Lost' appropriate for all cases that don't match other conditions?
  - Are there patients with activity that still get classified as 'Lost'?
  - How should NULL `last_appointment_date` be handled?

### Churn Risk Category:
- **Expected Mappings**:
  - `days_since_last_visit IS NULL AND last_appointment_date > current_date` → 'Low Risk'
  - `days_since_last_visit > 365` → 'High Risk'
  - `days_since_last_visit > 180 AND appointments_last_year = 0` → 'High Risk'
  - `days_since_last_visit > 120 AND no_show_rate > 0.3` → 'Medium Risk'
  - `days_since_last_visit > 90` → 'Medium Risk'
  - `appointments_last_year < 2 AND avg_days_between_appointments > 180` → 'Medium Risk'
  - All other cases → 'Low Risk'
- **Questions to Answer**:
  - Does the logic cover all scenarios correctly?
  - Are there edge cases where risk is misclassified?
  - How should NULL `days_since_last_visit` be handled when there's no future appointment?

### Patient Value Category:
- **Expected Mappings**:
  - `lifetime_production = 0 OR NULL` → 'No Production'
  - `lifetime_production < 500` → 'Low Value'
  - `lifetime_production < 2000` → 'Medium Value'
  - `lifetime_production < 5000` → 'High Value'
  - `lifetime_production >= 5000` → 'VIP'
- **Questions to Answer**:
  - Are the ranges appropriate for the business?
  - Are there any negative production values that need handling?
  - Is NULL production correctly mapped to 'No Production'?

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
1. **Retention Status**:
   - Verify 'Lost' is appropriate catch-all
   - Handle NULL `last_appointment_date` cases
   - Check for patients with activity still classified as 'Lost'

2. **Churn Risk Category**:
   - Verify all logic branches are covered
   - Handle edge cases (e.g., NULL `days_since_last_visit` without future appointment)
   - Check for misclassifications

3. **Patient Value Category**:
   - Verify ranges are appropriate
   - Handle negative production values if they exist
   - Document business rules for NULL production

4. **Documentation**:
   - Update `_mart_patient_retention.yml` with complete logic documentation
   - Document edge cases and business rules
   - Add data quality notes

---

## Related Documentation

- **Investigation SQL**: `validation/marts/mart_patient_retention/investigate_retention_category_mappings.sql`
- **Model Logic**: `models/marts/mart_patient_retention.sql` (lines 287-315)
- **Model Documentation**: `models/marts/_mart_patient_retention.yml` (lines 1016-1059)
- **Validation Framework**: `validation/README.md` - How validation works
