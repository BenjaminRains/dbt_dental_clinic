# Claims Data Quality Issues

This document tracks data quality issues identified in the claims data model. Each issue is documented with its impact, root cause, and recommended solutions.

## Issue #1: Extreme Allowed Amount Values

### Description
Some claims have allowed amounts that are orders of magnitude higher than expected, indicating data entry errors.

### Example
- **Claim ID**: 21561
- **Procedure**: D2391 (resin-based composite - one surface, posterior)
- **Billed Amount**: $252.00
- **Allowed Amount**: $252,252.00 (1000x the expected amount)
- **Date**: 2024-01-05

### Impact
- Causes test failures in `int_claim_details` model
- Distorts analytics and reporting
- Affects financial calculations and insurance analysis

### Root Cause
- Data entry error where decimal point was misplaced
- Similar procedures in the same claim show normal allowed amounts
- Typical allowed amounts for this procedure range from $126.00 to $252.00

### Recommended Solutions
1. **Immediate Fix**:
   - Correct the allowed amount to $252.00 to match the billed amount
   - Or set to $-1.00 to match the duplicate procedure in the same claim

2. **Preventive Measures**:
   - Add validation in the source system to prevent orders of magnitude differences between billed and allowed amounts
   - Implement automated checks for values that are significantly different from historical averages

3. **Test Enhancement**:
   - Modify the test to check for values that are orders of magnitude different from the billed amount
   - Add a ratio check between billed and allowed amounts

### Status
- [ ] Issue fixed
- [ ] Test modified
- [ ] Preventive measures implemented

## Issue #2: Systematic Use of -1.0 as Placeholder Value

### Description
A large number of claims use -1.0 as a placeholder value for allowed amounts, causing test failures.

### Example
- **Total Records**: 8,485
- **Value**: Consistently -1.0
- **Procedures Affected**: 123 different procedure codes
- **Patients Affected**: 1,618 different patients

### Impact
- Causes test failures in `int_claim_details` model
- Makes it difficult to distinguish between actual negative values and placeholder values
- Complicates financial analysis and reporting
- Represents 99.96% of all test failures

### Root Cause
- -1.0 appears to be used systematically as a placeholder when:
  - Insurance verification is pending
  - Claim is still being processed
  - Allowed amount hasn't been determined yet
  - Claim is in a transitional state

### Recommended Solutions
1. **Immediate Fix**:
   - Consider using NULL instead of -1.0 for unknown allowed amounts
   - Or use a separate status field to indicate pending verification

2. **Preventive Measures**:
   - Update data entry procedures to use NULL for unknown values
   - Add validation to prevent negative values unless explicitly allowed
   - Implement a status tracking system for claims

3. **Test Enhancement**:
   - Modify the test to either:
     - Exclude records with -1.0 if this is a valid placeholder
     - Or create a separate test for placeholder values
   - Add a status field to track claim verification state

### Status
- [ ] Issue fixed
- [ ] Test modified
- [ ] Preventive measures implemented

## Issue #3: Zero Billed Amount with Non-Zero Allowed Amount

### Description
A large number of claim records (1,592) have zero billed amounts but non-zero allowed amounts, creating unexpected insurance calculations.

### Example
- **Total Records**: 1,592
- **Billed Amount**: $0.00
- **Allowed Amount**: Various positive values (e.g., $78.00, $27.00, $157.00)
- **Claim IDs**: Multiple affected

### Impact
- Creates misleading insurance calculations
- Triggers decimal point error tests
- Affects financial reporting accuracy
- Represents 99.75% of decimal point error test failures

### Root Cause
- Potential data entry issues
- Possible incorrect mapping of zero-fee procedures to insurance coverage
- May represent special cases where procedures should not have been billed but were incorrectly submitted to insurance

### Recommended Solutions
1. **Immediate Fix**:
   - Create a separate test to identify and monitor these cases
   - Investigate highest-value allowed amounts on zero-billed procedures

2. **Preventive Measures**:
   - Add validation in the source system to prevent allowed amounts on zero-billed procedures
   - Create business rules for special case handling

3. **Test Enhancement**:
   - Updated the `warn_decimal_point_error` test to exclude zero billed amounts
   - Added a new test `warn_zero_billed_with_nonzero_allowed` to specifically track this issue

### Status
- [x] Issue identified
- [x] Test modified
- [ ] Preventive measures implemented

## Issue Template

### Description
[Brief description of the issue]

### Example
- **Claim ID**: [ID]
- **Procedure**: [Code and description]
- **Billed Amount**: [Amount]
- **Allowed Amount**: [Amount]
- **Date**: [Date]

### Impact
[How this issue affects the business and analytics]

### Root Cause
[Analysis of why this issue occurs]

### Recommended Solutions
1. **Immediate Fix**:
   [Steps to fix existing data]

2. **Preventive Measures**:
   [Steps to prevent future occurrences]

3. **Test Enhancement**:
   [Changes needed to tests]

### Status
- [ ] Issue fixed
- [ ] Test modified
- [ ] Preventive measures implemented 