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