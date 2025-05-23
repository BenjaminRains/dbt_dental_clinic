# Insurance Verification Timestamp and Date Issues

## Overview
This document outlines data quality issues identified in the insurance verification history records (`stg_opendental__insverifyhist`) related to timestamps and dates. These issues affect data completeness and tracking of verification activities.

## Critical Issues

### 1. Missing Assignment Dates
**Severity**: High  
**Impact**: Critical for tracking verification assignments and workflow

#### Description
19,613 records have null `last_assigned_date` values. Analysis shows these are all system-generated records (`user_id = 0`) created by verify users 9282 (11,971 records) and 57 (6,970 records).

#### Current State
- Total affected records: 19,613
- Date range: 2020-04-28 to 2025-02-28
- All records are system-generated (`user_id = 0`)
- Created by verify users 9282 and 57
- Affects both verification types (1=Subscriber, 2=Plan)

#### Business Impact
1. **Workflow Tracking**: Cannot accurately determine when verifications were assigned
2. **Process Analysis**: Difficult to analyze verification workflow efficiency
3. **Compliance**: May impact regulatory reporting requirements
4. **Data Quality**: Indicates potential issues with system-generated records

### 2. Missing Creation Timestamps
**Severity**: Medium  
**Impact**: Affects audit trail and record tracking

#### Description
6,975 records have null values in both `entry_timestamp` and `_created_at`. This exact match suggests these are the same set of records where creation timestamps were not captured.

#### Current State
- Total affected records: 6,975
- Affects both `entry_timestamp` and `_created_at`
- Likely related to system-generated records
- Pattern similar to missing assignment dates

#### Business Impact
1. **Audit Trail**: Incomplete record of when verifications were created
2. **Data Lineage**: Difficult to track record creation timing
3. **Process Analysis**: Challenges in analyzing verification workflow timing

### 3. Verification Date Mismatches
**Severity**: Low  
**Impact**: Expected behavior, but needs documentation

#### Description
17 history records have `last_assigned_date` values that don't match their corresponding verification records. This is expected behavior as verification records store target dates while history records track actual verification dates.

#### Current State
- Total affected records: 17
- All mismatches are in `last_assigned_date`
- Expected behavior for date tracking differences

## Root Cause Analysis

### System-Generated Records
1. **Pattern**: All issues primarily affect system-generated records (`user_id = 0`)
2. **Users**: Created by specific verify users (9282, 57)
3. **Timing**: Records span from 2020 to 2025
4. **Types**: Affects both verification types (1=Subscriber, 2=Plan)

### Data Quality Impact
1. **Test Failures**:
   - `not_null` test failing for `last_assigned_date` (19,613 records)
   - `not_null` test failing for `entry_timestamp` and `_created_at` (6,975 records)
   - `relationships` test warning for date mismatches (17 records)

2. **Process Issues**:
   - System-generated records missing critical timestamp data
   - Inconsistent capture of creation timestamps
   - Expected mismatches between verification and history dates

## Recommendations

### Short-term Actions
1. **Documentation**:
   - Update model YAML files with data quality notes
   - Document expected behaviors and patterns
   - Create alerts for new records with missing data

2. **Testing**:
   - Maintain `not_null` test for `last_assigned_date` as failure
   - Change `not_null` tests for timestamps to warnings
   - Document expected mismatches in relationship tests

### Long-term Solutions
1. **Source System Improvements**:
   - Implement required field validation for system-generated records
   - Ensure consistent timestamp capture
   - Add validation for date ranges

2. **Process Updates**:
   - Review system-generated record creation process
   - Implement consistent timestamp capture
   - Consider adding status tracking for verification workflow

## Next Steps
1. [ ] Investigate why system-generated records have null timestamps
2. [ ] Review verification workflow for timestamp capture
3. [ ] Consider adding status tracking for verification records
4. [ ] Monitor for new records with missing data
5. [ ] Update documentation as patterns are better understood

## Related Documentation
- [Insurance Verification Orphaned Records](../docs/data_quality/insurance_verification_orphaned_records.md)
- [Insurance Verification Orphaned Verification Records](../docs/data_quality/insurance_verification_orphaned_verification_records.md)
- [Insurance Subscriber Data Quality](../docs/data_quality/insurance_subscriber_data_quality.md)

## Last Updated
[Current Date] 