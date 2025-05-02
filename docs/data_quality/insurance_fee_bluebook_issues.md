# Insurance Bluebook Log Data Quality Issues

## Overview

We have identified several data quality issues with insurance payment records (`int_insurance_payment_allocated` model) related to allowed fees, bluebook data, and override amounts. This document outlines these issues, their impact, and recommended actions.

## Issue Description

Many insurance payment records in our system do not have corresponding entries in the `insbluebooklog` table, resulting in NULL values for:
- `allowed_fee`
- `allowed_fee_description`
- `allowed_fee_updated_at`

Currently, approximately 3,146 records (as of February 2025) are affected by this issue.

## Impact

1. **Data Validation Errors**: Failing tests for `not_null_int_insurance_payment_allocated_allowed_fee_updated_at`
2. **Incomplete Fee History**: Unable to track allowed fee changes and adjustments for affected claims
3. **Audit Limitations**: Missing history for fee negotiations and adjustments
4. **Reporting Gaps**: Fee analysis and variance reporting may be incomplete

## Example Data

Below is a sample of affected records from recent payments:

| payment_id | patient_id | carrier_name | procedure_code | split_amount | bluebook_payment_amount | allowed_fee | allowed_fee_updated_at |
|------------|------------|--------------|---------------|--------------|-------------------------|-------------|------------------------|
| 19010      | 27181      | Anthem BCBS  | D0120         | 12.00        | 12.00                   | NULL        | NULL                   |
| 18995      | 31264      | TeamCare     | D0220         | 34.00        | NULL                    | NULL        | NULL                   |
| 18957      | 3784       | Liberty Dental| D1110        | 24.00        | 27.00                   | NULL        | NULL                   |
| 18813      | 32786      | BCBS Illinois| D0150         | 43.00        | 43.00                   | NULL        | NULL                   |
| 18810      | 24931      | Cigna Dental | D2740         | 631.50       | 631.50                  | NULL        | NULL                   |

## Patterns Observed

1. **Mixed Data Completeness**: Some records have bluebook payment amounts but no allowed fee history
2. **Carrier Diversity**: The issue spans multiple insurance carriers
3. **Procedure Variety**: Affects various procedure types (exams, cleanings, crowns, etc.)
4. **Recent Data**: Most affected records are from recent claims/payments

## Root Cause Analysis

Potential root causes include:

1. **Process Gap**: The process to update the `insbluebooklog` table may not be consistently followed
2. **System Configuration**: Certain claim types or carriers may not trigger bluebook log entries
3. **Integration Issue**: Data synchronization between systems may be incomplete
4. **Staff Training**: Staff may not be completing all required steps for claim processing

## Recommended Actions

### For IT/Data Team

1. **Temporary Fix**: The validation test has been modified to only require `allowed_fee_updated_at` when `allowed_fee` is present
2. **Investigation**: Review the process that updates the `insbluebooklog` table to identify gaps
3. **Backfill Strategy**: Develop a plan to backfill missing history data where appropriate

### For Clinical Staff

1. **Process Review**: Confirm whether proper procedures are being followed for updating allowed fee information
2. **Documentation**: Ensure that fee overrides and adjustments are properly documented in the system
3. **Carriers Review**: Note whether specific insurance carriers show this pattern more frequently

### For Billing/Finance Staff

1. **Audit Sample Claims**: Review a sample of claims without fee history to determine if this impacts reimbursement
2. **Monitor Future Claims**: Pay special attention to claims processing to ensure complete data capture
3. **Reporting Adjustment**: Be aware that fee analysis reports may have incomplete data

## Next Steps

1. Schedule a review meeting with IT, clinical, and billing staff
2. Develop a data quality improvement plan
3. Monitor the percentage of records missing fee history over time
4. Update procedures as needed to ensure consistent data capture

## Questions for Discussion

1. Is there a legitimate business case where some insurance payments don't require fee history tracking?
2. Are certain carriers or claim types systematically missing this data?
3. Has there been a recent change in procedures or systems that might explain this pattern?
4. What specific information is most critical to capture in the bluebook logs?

## Extreme Allowed Override Amount Values

### Issue Description

In addition to missing bluebook data, we have identified several records with extremely high `allowed_override_amount` values in the bluebook table. This appears to be a data entry issue.

### Examples

| Insurance Carrier | allowed_override_amount | Expected Range | Likely Issue |
|-------------------|-------------------------|----------------|--------------|
| Delta Dental Of Washington | 252,252.0 | ~2,500 | Decimal or duplicate entry (252 entered twice) |
| Aflac | 10,934.0 | ~1,093 | Decimal place error |
| Delta Dental IL | 10,380.0 | ~1,038 | Decimal place error |

### Impact

1. **Data Validation Failures**: These extreme values cause the range validation test to fail
2. **Reporting Distortion**: Skews aggregate calculations for allowed fee metrics
3. **Consistency Issues**: Creates inconsistency between allowed amounts and payment amounts

### Patterns Observed

1. Most extreme values appear to be either:
   - Decimal place errors (missing decimal point)
   - Duplicate digit entries
   - Multiple entries combined into one field
2. Several affected carriers are Delta Dental affiliated plans
3. Pattern is similar to fee entry errors documented in the fee model quality report

### Recommended Actions

1. Implement data entry validation to prevent extreme values
2. Audit existing extreme values and correct where appropriate
3. Consider creating alerts for values significantly higher than procedure averages
4. Review carrier-specific data entry protocols, especially for Delta Dental

---

*Document prepared: March 2025*  
*Updated: May 2025 with allowed override amount issues*  
*Contact: Data Quality Team*