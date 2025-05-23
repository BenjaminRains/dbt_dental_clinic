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

This suggests these records are part of the same automated process as the orphaned verification records, but with a different pattern of data population.

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
   - Both issues indicate potential data integrity issues in the verification process

2. **Process Issues**
   - Some verification records never complete the subscriber creation step
   - The presence of old effective dates (2010, 2022) suggests possible data migration
   - No benefit notes or verification details are present
   - System-generated records are missing critical timestamp data

## Next Steps
1. Investigate the automated process creating these records
2. Determine if the abandoned verification records should be cleaned up
3. Consider modifying the tests to account for these known patterns
4. Document the expected behavior of the verification workflow
5. Investigate why system-generated records have null `entry_timestamp` values

## Related Documentation
- See `docs/data_quality/insurance_verification_orphaned_records.md` for analysis of orphaned history records
- See `_stg_opendental__insverify.yml` for test configuration
- See `_stg_opendental__insverifyhist.yml` for related test configuration

## Last Updated
[Current Date] 