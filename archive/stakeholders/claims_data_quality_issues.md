# Claims Data Quality Issues

This document tracks data quality issues identified in the claims data model. Each issue is documented with its impact, root cause, and recommended solutions.

## Issue #1: Extreme Allowed Amount Values

### Description
Some claims have allowed amounts that are orders of magnitude higher than expected, indicating data entry errors.

### Examples
1. **Claim ID**: 21561
   - **Procedure**: D2391 (resin-based composite - one surface, posterior)
   - **Billed Amount**: $252.00
   - **Allowed Amount**: $252,252.00 (1000x the expected amount)
   - **Date**: 2024-01-05

2. **Claim ID**: 25085
   - **Billed Amount**: $109.00
   - **Allowed Amount**: $10,934.00 (100x the billed amount)
   - **Paid Amount**: $109.00
   - **Pattern**: Decimal point error (should be $109.34)

3. **Claim ID**: 24286
   - **Billed Amount**: $134.00
   - **Allowed Amount**: $10,380.00 (77x the billed amount)
   - **Paid Amount**: $103.00
   - **Pattern**: Decimal point error (should be $103.80)

### Impact
- Causes test failures in `int_claim_details` model
- Distorts analytics and reporting
- Affects financial calculations and insurance analysis
- Currently failing test: `dbt_expectations_expect_column_values_to_be_between_int_claim_payments_allowed_amount__10000__0__allowed_amount_1_0`
- Failing records: 21561, 25085, 24286

### Root Cause
- Data entry errors where decimal point was misplaced
- Similar procedures in the same claim show normal allowed amounts
- Typical allowed amounts for these procedures range from $100-$300
- Patterns show consistent decimal point errors (100x, 77x, 1000x differences)

### Recommended Solutions
1. **Immediate Fix**:
   - Correct the allowed amounts to match the billed amounts or paid amounts
   - Or set to $-1.00 to match the duplicate procedure in the same claim

2. **Preventive Measures**:
   - Add validation in the source system to prevent orders of magnitude differences between billed and allowed amounts
   - Implement automated checks for values that are significantly different from historical averages
   - Add a ratio check between billed and allowed amounts in the source system

3. **Test Enhancement**:
   - Modify the test to check for values that are orders of magnitude different from the billed amount
   - Add a ratio check between billed and allowed amounts
   - Set threshold at 10x to catch decimal point errors while allowing for legitimate variations

### Status
- [ ] Issue fixed
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