# Insurance Verification Orphaned Records Analysis

## Overview
This report documents the discovery of orphaned insurance verification history records in the OpenDental system. These are records that exist in `insverifyhist` but do not have corresponding records in `insverify`.

## Key Findings

### Scale of the Issue
- Total orphaned records: 1,987
  - Type 1 (Insurance Subscriber): 55 records
  - Type 2 (Patient Eligibility): 1,932 records
- Affected entities:
  - Type 1: 27 unique entities
  - Type 2: 745 unique entities

### Record Patterns
1. **Sequential Nature**
   - Type 1: 28 out of 55 records (51%) are part of a sequence
   - Type 2: 1,187 out of 1,932 records (61%) are part of a sequence
   - This suggests these aren't random occurrences but part of a systematic process

2. **Timing Patterns**
   - Time span: January 1, 2023 to February 15, 2025
   - Average gaps between verifications:
     - Type 1: ~81 days
     - Type 2: ~115 days
   - No recent activity (no records in last 2 months)

3. **User Patterns**
   - Primary users involved:
     - User 57: 937 Type 2 verifications
     - User 9282: 924 Type 2 verifications
   - Regular weekly pattern of verification activities

## Impact
- The `relationships` test in `_stg_opendental__insverifyhist.yml` is failing
- This indicates a potential data integrity issue in the insurance verification process
- The issue affects both types of verifications but is more prevalent in Type 2

## Next Steps
1. Investigate the system's process for creating verification and history records
2. Determine if this is an expected behavior or a data quality issue
3. Consider modifying the test to account for this pattern if it's expected behavior
4. Document the findings in the model's YAML file

## Last Updated
[Current Date] 