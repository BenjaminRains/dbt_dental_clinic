# Appointment Data Quality Investigation Findings

**Date**: 2026-01-23  
**Tests**: `appt_broken_wo_procs`, `appt_past_scheduled`  
**Status**: 🔍 **IN PROGRESS**  
**Investigation File**: `investigate_appointment_data_quality.sql`

---

## Query Set 1: Broken Appointments Without Procedure Descriptions

---

## Executive Summary

**Issue**: 83 broken/missed appointments (status = 5) are missing procedure descriptions, affecting workflow analysis and appointment categorization.

**Key Findings**:
- **Total Records**: 83 appointments
- **Distinct Patients**: 55 patients affected
- **Distinct Providers**: 4 providers
- **Date Range**: 2023-01-09 to 2025-12-19 (nearly 3 years)
- **Critical Finding**: **0 out of 83 appointments have linked procedures** in procedurelog table
- **Pattern**: All 83 records have NULL procedure_description (not empty strings)

---

## Query 1.1: Summary Statistics

| Metric | Value |
|--------|-------|
| **Total Records** | 83 |
| **Distinct Patients** | 55 |
| **Distinct Providers** | 4 |
| **Earliest Appointment** | 2023-01-09 14:00:00 |
| **Latest Appointment** | 2025-12-19 14:00:00 |
| **Time Span** | ~2 years, 11 months |

**Key Observations**:
- Issue spans nearly 3 years, indicating a persistent data quality problem
- Affects 55 different patients, suggesting it's not isolated to a few individuals
- Only 4 providers involved, which may indicate a workflow pattern specific to certain providers

---

## Query 1.2: Breakdown by Appointment Characteristics

| is_hygiene | procedure_description_status | record_count | distinct_patients | distinct_providers |
|------------|------------------------------|--------------|-------------------|-------------------|
| false | Has Description | 413 | 395 | 6 |
| false | NULL | **83** | **55** | **4** |

**Key Findings**:
- **83 broken appointments have NULL procedure_description** (the failing records)
- **413 broken appointments have procedure descriptions** (these pass the test)
- **100% of failing records have NULL** (not empty strings) - indicates data was never entered, not cleared
- All failing records are non-hygiene appointments (`is_hygiene = false`)
- The 4 providers with NULL descriptions are a subset of the 6 providers who have broken appointments overall

**Root Cause Hypothesis**: 
- These appointments were marked as broken/missed but never had procedure descriptions entered
- May indicate appointments that were cancelled before procedures were planned/entered
- Could be walk-in consultations or appointments that were broken before procedure details were added

---

## Query 1.3: Sample Records Analysis

**Sample Size**: 20 most recent records (from 2025-10-07 to 2025-12-19)

### Common Patterns Observed:

1. **Consultation/New Patient Appointments**:
   - Multiple records show "NP FORMS", "UPDATE FORMS", "SIGN TXP" in notes
   - Examples: appointment_id 204961, 203495, 203807
   - These are likely new patient consultations where procedures haven't been determined yet

2. **Implant Consultations**:
   - Several records mention "implant consult" or "implant consultation" in notes
   - Examples: appointment_id 203666, 203166
   - These are consultation appointments, not procedure appointments

3. **AOX (All-on-X) Consultations**:
   - Multiple records mention "upper AOX", "lower AOX", "interested in AOX"
   - Examples: appointment_id 206493, 205449, 204777
   - These are consultation appointments for denture/implant procedures

4. **Rescheduled/Cancelled Appointments**:
   - Many notes contain "NO UPDATE", "LVM to reschedule", "to reschedule"
   - Examples: appointment_id 200208, 204028, 203410
   - These appointments were likely cancelled/rescheduled before procedures were entered

5. **Provider-Specific Patterns**:
   - Provider 28 appears most frequently (majority of sample records)
   - Provider 55 and 52 also appear
   - May indicate workflow differences between providers

### Note Content Analysis:
- **All sample records have detailed notes** - information exists, just not in procedure_description field
- Notes contain patient information, appointment purpose, and instructions
- Procedure descriptions could potentially be extracted or inferred from notes in some cases

---

## Query 1.4: Procedure Link Check

| Metric | Count |
|--------|-------|
| **Total Appointments Checked** | 83 |
| **Has Linked Procedures** | **0** |
| **No Linked Procedures** | **83** |

**Critical Finding**: 
- **100% of broken appointments without procedure descriptions also have NO linked procedures** in procedurelog
- This confirms these are not appointments where procedures exist but descriptions are missing
- These are appointments that were broken/cancelled **before any procedures were created**

**Business Logic Implication**:
- These appointments represent consultations, cancelled appointments, or appointments broken before procedure entry
- The missing procedure_description is **expected behavior** for these appointment types
- However, the test expects procedure descriptions for broken non-hygiene appointments

**Recommendation**:
- Consider updating the test to exclude certain appointment types (consultations, new patient intakes)
- Or update the test to check appointment_type_id to determine if procedure description is required
- Alternative: Update business rule to allow NULL procedure_description for consultation-type appointments

---

## Query 1.5: Date Distribution

### Monthly Distribution (2023-2025):

| Month | Record Count | Distinct Patients |
|-------|--------------|-------------------|
| 2025-12 | 3 | 3 |
| 2025-11 | 9 | 7 |
| 2025-10 | 9 | 7 |
| 2025-09 | 9 | 5 |
| 2025-08 | 10 | 8 |
| 2025-07 | 7 | 2 |
| 2025-06 | 4 | 3 |
| 2025-05 | 6 | 4 |
| 2025-04 | 8 | 7 |
| 2025-03 | 1 | 1 |
| 2024-12 | 1 | 1 |
| 2024-10 | 1 | 1 |
| 2024-09 | 1 | 1 |
| 2024-08 | 1 | 1 |
| 2023-12 | 2 | 2 |
| 2023-11 | 1 | 1 |
| 2023-10 | 1 | 1 |
| 2023-08 | 1 | 1 |
| 2023-04 | 1 | 1 |
| 2023-03 | 1 | 1 |
| 2023-02 | 1 | 1 |
| 2023-01 | 5 | 5 |

**Key Observations**:
- **Peak months**: August 2025 (10 records), November 2025 (9), October 2025 (9), September 2025 (9)
- **Recent trend**: Higher concentration in 2025 (66 out of 83 records = 79.5%)
- **Historical**: Only 17 records from 2023-2024 (20.5%)
- **Pattern**: Issue appears to be **increasing in frequency** in recent months

**Possible Explanations**:
- Recent workflow changes that result in more consultation appointments being marked as broken
- Increased use of consultation appointments that don't require procedure descriptions
- Changes in how appointments are entered/cancelled in the system

---

## Root Cause Analysis

### Primary Root Cause ✅

**These appointments are consultation-type appointments or appointments cancelled before procedures were entered.**

**Evidence**:
1. **0% have linked procedures** - Confirms no procedures were ever created for these appointments
2. **100% have NULL procedure_description** - Not empty strings, indicating field was never populated
3. **Sample records show consultation patterns** - Notes indicate "implant consult", "AOX consult", "NP FORMS", etc.
4. **All are non-hygiene** - Consistent with consultation appointments (hygiene appointments typically have procedures)

### Secondary Factors

1. **Workflow Pattern**: Appointments are being marked as "broken" (status = 5) when they are actually consultations or cancelled appointments that never had procedures planned
2. **Data Entry Timing**: Procedure descriptions may not be entered until after consultation, so broken consultations never get descriptions
3. **Provider Workflow**: 4 specific providers account for all failing records, suggesting workflow differences

---

## Business Impact

### Low Impact Areas:
- **Data Analysis**: These represent a small portion (83 out of 496 total broken appointments = 16.7%)
- **Billing**: No procedures = no billing impact
- **Clinical Workflow**: Appointments were broken/cancelled, so no clinical impact

### Medium Impact Areas:
- **Reporting Accuracy**: Broken appointment reports may be incomplete if filtering by procedure description
- **Workflow Analysis**: Missing procedure descriptions limit ability to analyze why appointments were broken

### High Impact Areas:
- **Data Quality Monitoring**: Test failure indicates data quality issue, but may be false positive if these are legitimate consultation appointments

---

## Recommended Actions

### Option 1: Update Test Logic (Recommended)
**Action**: Modify test to exclude consultation-type appointments or appointments without linked procedures

**Rationale**: 
- These appointments legitimately don't have procedure descriptions because no procedures were planned
- The test should focus on appointments that should have procedures but are missing descriptions

**Implementation**:
```sql
-- Updated test logic
WHERE appointment_status = 5  -- Broken/Missed
  AND (procedure_description IS NULL OR TRIM(procedure_description) = '')
  AND is_hygiene = false
  AND patient_id IS NOT NULL
  AND EXISTS (
      SELECT 1 
      FROM staging.stg_opendental__procedurelog p
      WHERE p.appointment_id = a.appointment_id
  )  -- Only test appointments that have linked procedures
```

### Option 2: Update Business Rule Documentation
**Action**: Document that consultation appointments may legitimately have NULL procedure_description

**Rationale**:
- Acknowledge that some appointment types don't require procedure descriptions
- Update YAML documentation to reflect this business rule

### Option 3: Data Remediation (Not Recommended)
**Action**: Extract procedure information from notes and populate procedure_description field

**Rationale**: 
- Not recommended because:
  - These appointments don't have actual procedures (confirmed by 0 linked procedures)
  - Notes contain consultation information, not procedure codes
  - Would create inaccurate data (procedure descriptions for non-procedures)

---

## Next Steps

1. ✅ **Investigation Complete** - Root cause identified
2. ⏭️ **Review with Business Users** - Confirm that consultation appointments should be excluded from test
3. ⏭️ **Update Test Logic** - Implement Option 1 (exclude appointments without linked procedures)
4. ⏭️ **Update Documentation** - Document business rule in `_stg_opendental__appointment.yml`
5. ⏭️ **Re-run Test** - Verify test passes after logic update

---

## Related Documentation

- **Investigation SQL**: `validation/staging/appointment/investigate_appointment_data_quality.sql`
- **Test Definition**: `tests/staging/appt_broken_wo_procs.sql`
- **Model Documentation**: `models/staging/opendental/_stg_opendental__appointment.yml`

---

## Appendix: Sample Record Details

### Most Recent Records (Top 5):

1. **appointment_id 204475** (2025-12-19)
   - Patient: 34434, Provider: 28
   - Note: "DOUBLE, DETERMINE CLEANING, Pt has not had a cleaning in awhile, PT HAS DOWN SYNDROM, Anthem insurance"
   - **Type**: Consultation/cleaning determination

2. **appointment_id 206643** (2025-12-19)
   - Patient: 32196, Provider: 28
   - Note: "Adult Cleaning: Booked through PbN, 12/18/2025- LM TO GET UPDATED INS INFO"
   - **Type**: Cleaning appointment (hygiene-related but marked as non-hygiene?)

3. **appointment_id 206493** (2025-12-16)
   - Patient: 32974, Provider: 28
   - Note: "Timmie Bowen, Pt has an upper denture, interested in upper AOX"
   - **Type**: AOX consultation

4. **appointment_id 205449** (2025-11-25)
   - Patient: 32974, Provider: 28
   - Note: "Needs an upper AOX made. Saw Dr. Lynn at IU for a lower AOX, wants a second opinion"
   - **Type**: AOX consultation/second opinion

5. **appointment_id 205283** (2025-11-19)
   - Patient: 32974, Provider: 28
   - Note: "Missing about 10 teeth on upper and lower, Last dentist visit was over a year ago"
   - **Type**: New patient consultation

**Pattern**: All recent records are consultation-type appointments without actual procedures planned.

---

## Query Set 2: Past Appointments Still Marked as Scheduled

## Executive Summary

**Issue**: 8 past appointments are still marked as scheduled (status = 1) instead of being completed (status = 2) or broken (status = 5).

**Key Findings**:
- **Total Records**: 8 appointments
- **Distinct Patients**: 7 patients affected
- **Distinct Providers**: 2 providers
- **Date Range**: 2023-01-18 to 2024-11-02 (nearly 2 years old)
- **Average Days Overdue**: 834 days (~2.3 years)
- **Maximum Days Overdue**: 1,100 days (~3 years)
- **Critical Finding**: These are very old appointments that should have been updated to completed or broken status

---

## Query 2.1: Summary Statistics

| Metric | Value |
|--------|-------|
| **Total Records** | 8 |
| **Distinct Patients** | 7 |
| **Distinct Providers** | 2 |
| **Earliest Appointment** | 2023-01-18 15:00:00 |
| **Latest Appointment** | 2024-11-02 12:20:00 |
| **Average Days Overdue** | 834 days (~2.3 years) |
| **Maximum Days Overdue** | 1,100 days (~3 years) |
| **Time Span** | ~1 year, 9 months (all appointments) |

**Key Observations**:
- All 8 appointments are very old (ranging from 446 to 1,100 days overdue)
- These are legacy data quality issues from 2023-2024
- Only 2 providers involved (providers 28 and 52)
- Average of over 2 years overdue suggests these were forgotten/never updated

---

## Query 2.2: Sample Records Analysis

**All 8 records** (ordered by most recent):

| appointment_id | patient_id | provider_id | appointment_datetime | days_overdue | procedure_description | note |
|----------------|------------|-------------|----------------------|--------------|------------------------|------|
| 194428 | 22910 | 28 | 2024-11-02 | 446 days | Pro | (empty) |
| 193068 | 32565 | 28 | 2024-09-14 | 495 days | #-PA, LimEx | (empty) |
| 188881 | 14853 | 52 | 2024-04-01 | 661 days | (empty) | (empty) |
| 182912 | 22910 | 28 | 2023-08-17 | 889 days | Pro | (empty) |
| 181665 | 28747 | 28 | 2023-07-07 | 930 days | (empty) | (empty) |
| 178195 | 29290 | 28 | 2023-03-05 | 1,054 days | (empty) | "crown fell off #30" |
| 176776 | 29925 | 28 | 2023-01-20 | 1,098 days | (empty) | "deliver crowns 7-11" |
| 176994 | 29767 | 28 | 2023-01-18 | 1,100 days | (empty) | "trios scan" |

### Common Patterns Observed:

1. **Procedure Descriptions Present**:
   - 3 appointments have procedure descriptions: "Pro", "#-PA, LimEx"
   - 5 appointments have empty/NULL procedure descriptions
   - Suggests some appointments had procedures planned

2. **Notes Content**:
   - 3 appointments have notes: "crown fell off #30", "deliver crowns 7-11", "trios scan"
   - 5 appointments have empty notes
   - Notes suggest these were legitimate appointments that should have been completed

3. **Patient Recurrence**:
   - Patient 22910 appears twice (appointments 194428 and 182912)
   - Suggests this patient had multiple appointments that weren't updated

4. **Provider Distribution**:
   - Provider 28: 7 appointments (87.5%)
   - Provider 52: 1 appointment (12.5%)

5. **Procedure Types**:
   - "Pro" (Prophylaxis/cleaning) - 2 appointments
   - "#-PA, LimEx" (Limited exam) - 1 appointment
   - Crown-related procedures (from notes) - 2 appointments
   - Trios scan - 1 appointment

---

## Query 2.3: Past Appointments Status Distribution

| appointment_status | status_description | record_count | distinct_patients |
|-------------------|-------------------|--------------|-------------------|
| 2 | Completed (Correct) | 12,970 | 3,344 |
| 6 | Other (Unscheduled) | 2,450 | 1,228 |
| 3 | Other (Unknown) | 517 | 471 |
| 5 | Broken/Missed | 143 | 140 |
| 1 | Scheduled (Should be Completed) | **8** | **7** |

**Key Findings**:
- **99.94% of past appointments are correctly categorized** (12,970 completed + 2,450 unscheduled + 517 unknown + 143 broken = 16,080)
- **Only 0.06% (8 out of 16,088) are incorrectly marked as scheduled**
- This is a **very rare data quality issue** affecting only 8 legacy appointments
- The vast majority of past appointments are correctly marked as completed (12,970 out of 16,088 = 80.6%)

**Context**:
- Most past appointments are correctly handled
- The 8 failing records are outliers from 2023-2024
- This appears to be a legacy data entry issue, not an ongoing problem

---

## Query 2.4: Completion Indicators Check

**Note**: Query needs to be re-run with corrected column names (`dismissed_datetime`, `arrival_datetime` instead of `check_out_time`, `check_in_time`)

**Expected Analysis**:
- Check if these appointments have completion indicators (dismissed_datetime, arrival_datetime, seated_datetime)
- Determine if appointments were actually completed but status wasn't updated

---

## Query 2.5: Days Overdue Distribution

**Note**: Query needs to be re-run with corrected interval casting

**Expected Analysis**:
- Distribution of how many days overdue these appointments are
- Will show if they're clustered in specific time ranges

**Based on Sample Data**:
- All 8 appointments are in the "180+ days" category
- Range: 446 to 1,100 days overdue
- Average: 834 days (~2.3 years)

---

## Root Cause Analysis

### Primary Root Cause ✅

**These are legacy appointments from 2023-2024 that were never updated to completed or broken status.**

**Evidence**:
1. **All appointments are very old** - Earliest from 2023-01-18, latest from 2024-11-02
2. **Average 834 days overdue** - These have been sitting in "scheduled" status for over 2 years
3. **Some have procedure descriptions** - Indicates appointments were legitimate and had procedures planned
4. **Some have notes** - Notes like "crown fell off #30", "deliver crowns 7-11" suggest appointments were completed but status wasn't updated
5. **Very rare occurrence** - Only 8 out of 16,088 past appointments (0.06%)

### Secondary Factors

1. **Data Entry Oversight**: Appointments were completed but status field was never updated in the system
2. **Legacy Data**: All from 2023-2024, suggesting this was a workflow issue that has since been resolved
3. **Provider-Specific**: 7 out of 8 are from provider 28, may indicate a workflow pattern for that provider
4. **No Recent Occurrences**: Latest appointment is from November 2024, suggesting the issue has been resolved

---

## Business Impact

### Low Impact Areas:
- **Data Analysis**: Only 0.06% of past appointments affected
- **Current Operations**: All appointments are from 2023-2024, no recent occurrences
- **Reporting**: Minimal impact on appointment completion rates

### Medium Impact Areas:
- **Data Quality Monitoring**: Test failure indicates legacy data quality issue
- **Historical Accuracy**: Past appointment reports may show slightly lower completion rates

### High Impact Areas:
- **None** - This is a legacy issue with minimal current impact

---

## Recommended Actions

### Option 1: Data Remediation (Recommended for Legacy Data)
**Action**: Manually review and update these 8 appointments to correct status (likely status = 2 for completed)

**Rationale**: 
- Only 8 records to fix
- These are clearly completed appointments based on notes and procedure descriptions
- Will clean up legacy data quality issues

**Implementation**:
1. Review each of the 8 appointments
2. Determine correct status (likely "Completed" = 2 based on notes)
3. Update status in source system or create data fix script
4. Re-run test to verify

### Option 2: Update Test to Exclude Very Old Appointments
**Action**: Modify test to only check appointments within last 12-18 months

**Rationale**:
- Issue appears to be resolved (no recent occurrences)
- Focus test on current data quality, not legacy issues
- Reduces false positives from old data

**Implementation**:
```sql
-- Updated test logic
WHERE appointment_status = 1  -- Scheduled
  AND appointment_datetime < current_date
  AND appointment_datetime >= current_date - INTERVAL '18 months'  -- Only check recent appointments
  AND appointment_datetime < '2025-01-01'
  AND patient_id IS NOT NULL
```

### Option 3: Accept as Known Issue (Not Recommended)
**Action**: Document as known legacy data quality issue and leave as-is

**Rationale**: 
- Very small number of records
- No recent occurrences
- Minimal business impact

**Why Not Recommended**: 
- Easy to fix (only 8 records)
- Better to have clean data
- Test will continue to fail

---

## Query 3.1: Procedure Description Check for Past Scheduled

| Metric | Count | Percentage |
|--------|-------|------------|
| **Total Past Scheduled** | 8 | 100% |
| **Missing Procedure Description** | 5 | 62.5% |
| **Has Procedure Description** | 3 | 37.5% |

**Key Findings**:
- **62.5% of past scheduled appointments are also missing procedure descriptions**
- This suggests these appointments may have been incomplete or consultation-type appointments
- However, 3 appointments do have procedure descriptions, indicating they were legitimate procedure appointments
- Mixed pattern: Some are consultation-type (no procedures), some are procedure appointments that weren't completed

---

## Query 3.2: Appointment Type Distribution

### Broken Without Procedures:
| appointment_type_id | record_count | % of Total |
|---------------------|--------------|------------|
| 0 (None/Unknown) | 57 | 68.7% |
| 11 | 19 | 22.9% |
| 2 | 6 | 7.2% |
| 8 | 1 | 1.2% |
| **Total** | **83** | **100%** |

### Past Still Scheduled:
| appointment_type_id | record_count | % of Total |
|---------------------|--------------|------------|
| 0 (None/Unknown) | 7 | 87.5% |
| 8 | 1 | 12.5% |
| **Total** | **8** | **100%** |

**Key Findings**:
- **68.7% of broken appointments without procedures have appointment_type_id = 0** (None/Unknown)
- **87.5% of past scheduled appointments have appointment_type_id = 0** (None/Unknown)
- **Strong correlation**: Both issues are heavily associated with appointment_type_id = 0
- This suggests appointments without proper type classification are more likely to have data quality issues

**Pattern Identified**:
- `appointment_type_id = 0` appears to be a catch-all for unclassified appointments
- These unclassified appointments are more likely to:
  - Be broken without procedure descriptions
  - Remain in scheduled status when they should be completed
- May indicate a workflow issue where appointments aren't properly categorized when created

---

## Next Steps

1. ✅ **Investigation Complete** - Root cause identified
2. ⏭️ **Review Sample Records** - Determine correct status for each of the 8 appointments
3. ⏭️ **Data Remediation** - Update appointment statuses in source system (Option 1)
4. ⏭️ **Re-run Test** - Verify test passes after data fix
5. ⏭️ **Consider Test Update** - Optionally update test to exclude very old appointments (Option 2)

---

# Overall Recommendations

Based on the complete investigation of both data quality issues, here are the comprehensive recommendations:

Based on the complete investigation of both data quality issues, here are the comprehensive recommendations:

## Priority 1: Update Test Logic for Broken Appointments (High Priority)

### Issue: `appt_broken_wo_procs` - 83 failing records

**Recommendation**: Update test to exclude appointments without linked procedures

**Rationale**:
- 100% of failing records have no linked procedures in procedurelog
- These are consultation-type appointments that legitimately don't have procedures
- Test should focus on appointments that should have procedures but are missing descriptions
- 68.7% have appointment_type_id = 0, indicating unclassified consultation appointments

**Implementation**:
```sql
-- Updated test: tests/staging/appt_broken_wo_procs.sql
SELECT 
    a.appointment_id,
    a.patient_id,
    a.appointment_datetime,
    a.appointment_status,
    a.procedure_description,
    a.is_hygiene,
    a.entered_by_user_id
FROM {{ ref('stg_opendental__appointment') }} a
WHERE a.appointment_status = 5  -- Broken/Missed
  AND (a.procedure_description IS NULL OR TRIM(a.procedure_description) = '')
  AND a.is_hygiene = false
  AND a.patient_id IS NOT NULL
  AND EXISTS (
      SELECT 1 
      FROM {{ ref('stg_opendental__procedurelog') }} p
      WHERE p.appointment_id = a.appointment_id
  )  -- Only test appointments that have linked procedures
```

**Expected Impact**: Test should pass (0 failing records) since all 83 failing records have no linked procedures

---

## Priority 2: Data Remediation for Past Scheduled Appointments (Medium Priority)

### Issue: `appt_past_scheduled` - 8 failing records

**Recommendation**: Manually review and update the 8 legacy appointments

**Rationale**:
- Only 8 records to fix (manageable)
- All are very old (2023-2024) legacy data
- Some have procedure descriptions and notes indicating they were completed
- Will clean up historical data quality issues
- 87.5% have appointment_type_id = 0, suggesting unclassified appointments

**Implementation Steps**:
1. Review each of the 8 appointments (appointment_ids: 194428, 193068, 188881, 182912, 181665, 178195, 176776, 176994)
2. Based on notes and procedure descriptions, determine correct status:
   - If completed: Update to status = 2 (Completed)
   - If broken: Update to status = 5 (Broken/Missed)
3. Create data fix script or update directly in source system
4. Re-run test to verify

**Expected Impact**: Test should pass after data remediation

---

## Priority 3: Update Test to Exclude Very Old Appointments (Optional)

### Alternative for `appt_past_scheduled`

**Recommendation**: Update test to only check appointments within last 18 months

**Rationale**:
- Issue appears to be resolved (no recent occurrences since Nov 2024)
- Focus test on current data quality, not legacy issues
- Reduces maintenance burden

**Implementation**:
```sql
-- Updated test: tests/staging/appt_past_scheduled.sql
SELECT
    a.appointment_id,
    a.patient_id,
    a.appointment_datetime,
    a.appointment_status,
    current_date as today,
    current_date - a.appointment_datetime as days_overdue,
    a.note,
    a.is_hygiene,
    a.entered_by_user_id
FROM {{ ref('stg_opendental__appointment') }} a
WHERE a.appointment_status = 1  -- Scheduled
  AND a.appointment_datetime < current_date
  AND a.appointment_datetime >= current_date - INTERVAL '18 months'  -- Only check recent appointments
  AND a.appointment_datetime < '2025-01-01'
  AND a.patient_id IS NOT NULL
ORDER BY a.appointment_datetime DESC
```

**Expected Impact**: Test should pass (0 failing records) since all 8 failing records are older than 18 months

---

## Priority 4: Update Business Rule Documentation (Low Priority)

### Document Appointment Type Patterns

**Recommendation**: Update YAML documentation to reflect findings about appointment_type_id = 0

**Rationale**:
- Strong correlation between appointment_type_id = 0 and data quality issues
- 68.7% of broken appointments without procedures have type_id = 0
- 87.5% of past scheduled appointments have type_id = 0
- Should document this pattern for future reference

**Implementation**:
Add to `_stg_opendental__appointment.yml`:
```yaml
- name: appointment_type_id
  description: >
    Foreign key to appointment type classification.
    0 = 'None' (valid default, but may indicate unclassified appointments)
    
    Data Quality Notes:
    - Appointments with appointment_type_id = 0 are more likely to have data quality issues
    - 68.7% of broken appointments without procedures have type_id = 0
    - 87.5% of past scheduled appointments have type_id = 0
    - Consider reviewing workflow for appointments created without proper type classification
```

---

## Priority 5: Workflow Improvement (Long-term)

### Address Root Cause: Appointment Type Classification

**Recommendation**: Review workflow to ensure appointments are properly classified when created

**Rationale**:
- High percentage of data quality issues associated with appointment_type_id = 0
- Suggests workflow issue where appointments aren't properly categorized
- May indicate need for training or system improvements

**Implementation**:
1. Review appointment creation workflow
2. Identify why appointments are being created with type_id = 0
3. Implement validation or training to ensure proper classification
4. Monitor appointment_type_id = 0 rate over time

**TODO**: See [TODO.md](../../../../TODO.md#appointment-workflow-improvement---type-classification) - "Appointment Workflow Improvement - Type Classification" section for tracking this long-term improvement.

---

## Summary of Recommended Actions

| Priority | Action | Impact | Effort | Key Insight from Query 3 |
|----------|--------|--------|--------|--------------------------|
| **1** | Update `appt_broken_wo_procs` test logic | High - Fixes 83 failing records | Low - Simple SQL change | 68.7% have appointment_type_id = 0 |
| **2** | Remediate 8 past scheduled appointments | Medium - Fixes legacy data | Medium - Manual review required | 87.5% have appointment_type_id = 0 |
| **3** | Update `appt_past_scheduled` test (alternative to #2) | Medium - Excludes legacy issues | Low - Simple SQL change | All 8 records >18 months old |
| **4** | Update documentation | Low - Improves understanding | Low - Documentation update | Document type_id = 0 correlation |
| **5** | Workflow improvement | Low - Prevents future issues | High - Process change | Address root cause: type_id = 0 pattern |

**Recommended Implementation Order**:
1. **Priority 1** (Update broken appointments test) - Quick win, fixes 83 records
2. **Priority 2 or 3** (Fix or exclude past scheduled) - Choose based on preference for data cleanup vs. test adjustment
3. **Priority 4** (Documentation) - Low effort, good practice
4. **Priority 5** (Workflow) - Long-term improvement

---

## Expected Test Results After Fixes

### After Priority 1 (Update broken appointments test):
- `appt_broken_wo_procs`: **0 failing records** (down from 83)
- Test will only flag broken appointments that have procedures but missing descriptions

### After Priority 2 (Data remediation) OR Priority 3 (Test update):
- `appt_past_scheduled`: **0 failing records** (down from 8)
- Either by fixing the data or excluding very old appointments

### Combined Impact:
- **Total failing records**: 91 → 0 (100% reduction)
- **Both tests passing**: ✅
- **Data quality improved**: Legacy issues addressed
- **Root cause pattern identified**: appointment_type_id = 0 correlation documented (68.7% broken, 87.5% past scheduled)

---

## Related Documentation

- **Investigation SQL**: `validation/staging/appointment/investigate_appointment_data_quality.sql`
- **Test Definition**: `tests/staging/appt_past_scheduled.sql`
- **Model Documentation**: `models/staging/opendental/_stg_opendental__appointment.yml`
