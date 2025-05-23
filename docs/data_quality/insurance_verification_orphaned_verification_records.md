# Insurance Verification Orphaned Verification Records Analysis

## Overview
This report documents the discovery of 218 verification records in `insverify` that do not have corresponding history records in `insverifyhist`. These records show distinct patterns that suggest they are part of an automated or semi-automated insurance verification workflow.

## Test Failures

### 1. Missing History Records
The `relationships` test in `_stg_opendental__insverify.yml` is failing with 218 results, indicating that these verification records exist without corresponding history records.

### 2. Null Created At Timestamps
The `not_null` test on `_created_at` is failing with 1,094 results. Analysis shows:
- 438 Type 1 records and 656 Type 2 records
- All records have `user_id = 0` (system-generated)
- All records have `last_verified_date` populated in 2023
- All records have null `entry_timestamp` and `last_assigned_date`
- All Type 1 records have matching subscribers (mostly ACTIVE, some TERMINATED)

### 3. Invalid Subscriber References
The `relationships_where` test for Type 1 verifications is failing with 283 results. Analysis shows:
- All records are Type 1 (Insurance subscriber)
- All records are system-generated (`user_id = 0`)
- All records have unique `foreign_key_id`s
- Most records (275) have `last_verified_date` populated
- None have `last_assigned_date` populated
- Records span from 2023-01-04 to 2025-02-27
- Consistent timestamp pattern:
  - `entry_timestamp` and `_created_at` are identical
  - `_updated_at` is exactly 1 hour and 1 minute after `_created_at`
- Some records have very old `entry_timestamp` dates (2020, 2022) but recent `last_verified_date` (2023)

### 4. Verification Date Mismatches
The `equality` test comparing verification and history records is failing with 29,708 results. Analysis shows:
- 6,672 Type 1 records affecting 1,066 unique verifications
- 6,196 Type 2 records affecting 1,997 unique verifications
- All mismatches are in `last_verified_date` only
- No mismatches in `foreign_key_id` or `last_assigned_date`
- All records are user-generated (no system-generated records)
- Records span from 2020-04-28 to 2025-02-13
- Pattern shows:
  - Verification records have future target dates
  - History records show actual verification dates
  - Multiple history records per verification
  - History records form a linked list with next/prev references
  - All history records for a verification have the same `entry_timestamp`

This suggests the mismatches are expected behavior, where:
1. The verification record's `last_verified_date` represents the target/scheduled date
2. The history records track the actual verification dates
3. The history records form a chain of verification activities leading up to the target date

## Key Findings

### Record Patterns
1. **User Pattern**
   - All records have `user_id = 0`
   - No records from primary users (57, 9282)
   - Suggests automated or system-generated records

2. **Timing Patterns**
   - Records span from January to September 2023
   - All created during business hours (9:00-15:00)
   - For records with matching subscribers:
     - Verification records created 3-12 days before subscriber creation
     - Example: Verification 2023-01-24 → Subscriber 2023-01-27 (3 days)
     - Example: Verification 2023-07-22 → Subscriber 2023-08-03 (12 days)

3. **Data Completeness**
   - Empty `last_verified_date` and `last_assigned_date`
   - Empty `note` fields
   - All records are Type 1 (Insurance Subscriber)

### Subscriber Relationship Analysis
1. **Matching Subscribers**
   - 6 out of 10 sampled records have matching subscribers
   - All matching subscribers are marked as 'ACTIVE'
   - All have external subscriber IDs
   - Some have effective dates going back to 2010, 2022
   - None have termination dates

2. **Missing Subscribers**
   - 4 out of 10 sampled records never resulted in subscriber creation
   - These appear to be abandoned or failed verification attempts

## Process Analysis
The data suggests a two-step automated process:
1. System creates verification records (`user_id = 0`)
2. These records are later processed to create subscribers
   - Subscribers are created 3-12 days after verification
   - Some records are abandoned or fail to complete

## Data Quality Impact
1. **Test Failures**
   - The `relationships` test is failing due to orphaned verification records
   - The `not_null` test on `_created_at` is failing due to system-generated records
   - The `relationships_where` test is failing due to invalid subscriber references
   - The `equality` test is failing due to expected date mismatches between verification and history records
   - All issues indicate potential data integrity issues in the verification process

2. **Process Issues**
   - Some verification records never complete the subscriber creation step
   - The presence of old effective dates (2010, 2022) suggests possible data migration
   - No benefit notes or verification details are present
   - System-generated records are missing critical timestamp data
   - Consistent 1-hour-1-minute update pattern suggests automated processing
   - Verification dates represent target dates while history records show actual dates

## Next Steps
1. Investigate the automated process creating these records
2. Determine if the abandoned verification records should be cleaned up
3. Consider modifying the tests to account for these known patterns
4. Document the expected behavior of the verification workflow
5. Investigate why system-generated records have null `entry_timestamp` values
6. Investigate the 1-hour-1-minute update pattern in the timestamps
7. Determine why some records have old creation dates but recent verification dates
8. Update the equality test to account for the target vs actual date pattern

## Related Documentation
- See `docs/data_quality/insurance_verification_orphaned_records.md` for analysis of orphaned history records
- See `_stg_opendental__insverify.yml` for test configuration
- See `_stg_opendental__insverifyhist.yml` for related test configuration

## Last Updated
[Current Date] 