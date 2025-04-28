# Insurance Carrier Mapping Issue Analysis

## Overview
Two significant data quality issues have been identified in the insurance claims data:

1. **Missing Carrier Information**: Approximately 47.41% of claims (18,992 records) have a valid 
claim ID but no associated carrier information, despite having valid plan numbers. This affects 
the ability to track and analyze insurance claims by carrier.

2. **Duplicate Claim Records**: Analysis of the source data reveals that approximately 15% of 
claims appear multiple times in the system. These duplicates show the same claim number but 
different procedure records, with some claims having twice as many records as procedures. 
This pattern exists across both 'Received' and 'Supplemental' claim statuses.

Both issues require attention as they impact:
- AR analysis and reporting
- Insurance payment tracking
- Procedure-level financial calculations
- Data integrity in downstream systems

## Data Analysis

### Current State
- **Total Records**: 40,060
- **Valid Records**: 21,068 (52.59%)
- **Invalid Records**: 18,992 (47.41%)
  - All invalid records have claims but no carrier information
  - No records have carriers without claims

### Top Affected Plans
| Plan Number | Record Count | Percentage | Date Range |
|------------|-------------|------------|------------|
| 11587 | 1,718 | 5.71% | 2023-01-10 to 2024-12-30 |
| 5937 | 1,449 | 4.81% | 2023-01-02 to 2025-02-27 |
| 12530 | 710 | 2.36% | 2023-01-11 to 2025-02-27 |
| 2700 | 643 | 2.14% | 2023-01-17 to 2025-02-17 |
| 2674 | 547 | 1.82% | 2023-01-18 to 2025-02-18 |

### Time Range Analysis
- **Earliest Claim**: 2023-01-02
- **Latest Claim**: 2025-02-28
- **Duration**: Over 2 years of affected claims
- **Pattern**: Consistent throughout the period, not isolated to specific timeframes

## Impact

### Technical Impact
1. **Data Quality Tests**: Current tests expect each claim to be associated with exactly one carrier
 (one-to-one relationship between claim and carrier). A carrier can have many claims 
 (one-to-many relationship between carrier and claims), but each claim should map to exactly one 
 carrier.
2. **Reporting**: AR analysis may be incomplete without carrier information
3. **Data Integrity**: Missing carrier information affects downstream analytics

### Business Impact
1. **AR Analysis**: Incomplete carrier information may affect:
   - Insurance aging reports
   - Carrier-specific collections analysis
   - Insurance payment tracking
2. **Revenue Cycle**: Potential impact on:
   - Insurance follow-up processes
   - Payment posting accuracy
   - Claims management

## Examples

### Example 1: Plan 11587 Analysis
```
Plan Number: 11587
Total Records: 1,696
Date Range: 2023-06-13 to 2024-12-30
Status: All claims have status 'R' (Received)
Patterns:
- Most claims have 1-4 procedures
- Some large claims with 8-26 procedures
- Regular claim activity throughout the period
- Source data contains duplicate claim records:
  * Same claim number appears multiple times
  * Some claims have 2x the number of records as procedures
  * Duplicates exist in both 'R' and 'S' status claims
- All claims have valid plan numbers but no carrier mapping
```

### Example 2: Recent Activity
```
Plan Number: 5937
Records: 1,449
Date Range: 2023-01-02 to 2025-02-27
Status: Continuous activity through present
Impact: Affects current AR and collections
```

## Potential Causes

1. **Data Entry Issues**:
   - Plan numbers may be entered incorrectly
   - Carrier records may not be created properly
   - Plan number 11587 exists and is active, but carrier mapping is missing

2. **System Configuration**:
   - Plan-to-carrier mapping may be misconfigured
   - Carrier records may be deactivated incorrectly
   - The issue appears systematic for certain plan numbers

3. **Business Process**:
   - New plans may be added without proper carrier setup
   - Carrier information may be updated incorrectly
   - The pattern suggests a process gap in carrier setup

## Recommendations

1. **Short-term**:
   - Review and document valid plan numbers without carriers
   - Update data quality tests to reflect current state
   - Add documentation for affected plans

2. **Medium-term**:
   - Investigate source system carrier setup process
   - Review plan number assignment procedures
   - Implement validation checks for new plan entries

3. **Long-term**:
   - Develop automated carrier mapping validation
   - Create monitoring for new plan-carrier relationships
   - Establish data quality metrics for carrier information

## Next Steps

1. **Data Validation**:
   - Verify plan numbers against source system
   - Confirm carrier record existence
   - Document valid vs. invalid mappings

2. **Process Review**:
   - Review plan setup procedures
   - Document carrier mapping requirements
   - Identify process gaps

3. **System Updates**:
   - Update data quality tests
   - Implement monitoring
   - Create documentation

## Questions for Stakeholders

1. Are these plan numbers valid in the source system?
2. Is the missing carrier information expected for certain plan types?
3. What is the impact on business processes?
4. Should we update our data model to handle this pattern?
5. What is the preferred approach for handling these cases in reports?

## Contact Information

For questions or additional information, please contact:
- Data Engineering Team
- Insurance Operations Team
- Revenue Cycle Management

## Data Quality Issues

### Duplicate Claims
Analysis of source data reveals that duplicate claims exist in the source system:
1. **Pattern**: Some claims appear multiple times with the same claim number
2. **Frequency**: Approximately 15% of claims show duplication
3. **Impact**: 
   - Affects procedure counts in reporting
   - May impact AR calculations
   - Requires careful handling in data models

### Missing Carrier Information
// ... existing code ... 