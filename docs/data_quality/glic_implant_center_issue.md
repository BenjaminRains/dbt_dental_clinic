# Data Quality Issue: GLIC Implant Center

## Issue Summary
Patient ID 32974 represents "GLIC" (the new implant center), which is being incorrectly treated as a patient in the system, causing multiple data quality test failures.

## Root Cause Analysis
- **Source System Issue**: OpenDental is being used to track a clinic/facility as if it were a patient
- **Data Entry Problem**: Staff are creating "patient" records for non-patient entities
- **System Design Mismatch**: The system wasn't designed to handle facility/clinic entities

## Data Quality Issues Identified

### Patient ID 32974 (GLIC) Characteristics:
- **Birth Date**: `0001-01-01` (placeholder/default value)
- **Age**: `2024` years (impossible age calculated from invalid birth date)
- **Position Code**: `House` (indicating it's a facility, not a person)
- **Preferred Name**: Empty (because it's not a person)
- **Appointment Dates**: All future dates (2025-2026)
- **Patient Status**: `Patient` (incorrect classification)

### Test Failures Caused:
1. **Age Range Test**: Age = 2024 (exceeds 120 limit)
2. **Appointments Last 30 Days**: 51 appointments (counting future appointments)
3. **Date Range Tests**: Future appointment dates outside expected ranges
4. **Annual Patient Value**: Potentially inflated due to future appointments

## Business Impact
- **Analytics Accuracy**: Patient retention metrics are skewed
- **Reporting Issues**: GLIC appears as an outlier in patient reports
- **Data Trust**: Users may question data quality when seeing impossible values

## Recommended Solutions

### Short-term (Immediate):
1. **Filter GLIC from patient analysis** - Exclude patient_id 32974 from patient retention marts
2. **Set failing tests to warn** - Allow pipeline to continue while monitoring
3. **Document the issue** - Create clear documentation for data users

### Medium-term (Data Model):
1. **Create facility/clinic entity type** - Separate from patient entities
2. **Implement proper filtering logic** - Distinguish between patients and facilities
3. **Add data validation rules** - Prevent similar issues in the future

### Long-term (Process):
1. **Staff training** - Educate dental staff on proper data entry
2. **System configuration** - Configure OpenDental to handle facilities properly
3. **Data governance** - Establish clear rules for entity types

## Monitoring and Prevention

### Data Quality Checks to Implement:
```sql
-- Check for other non-patient entities
SELECT 
    patient_id,
    preferred_name,
    birth_date,
    age,
    position_code,
    patient_status
FROM dim_patient 
WHERE birth_date = '0001-01-01' 
   OR age > 120 
   OR position_code = 'House'
   OR preferred_name IS NULL;
```

### Alerts to Set Up:
- Age > 120 years
- Birth date = 0001-01-01
- Position code = 'House'
- Future appointment dates
- Missing patient names

## Action Items
- [ ] Filter GLIC from patient retention analysis
- [ ] Set data quality tests to warn instead of fail
- [ ] Investigate other potential non-patient entities
- [ ] Create staff training materials
- [ ] Implement monitoring queries
- [ ] Document proper data entry procedures

## Contact Information
- **Data Team**: [Your contact info]
- **Dental Staff**: [Staff contact info]
- **OpenDental Support**: [Support contact info]

---
**Created**: [Current Date]
**Last Updated**: [Current Date]
**Status**: Active
**Priority**: High
