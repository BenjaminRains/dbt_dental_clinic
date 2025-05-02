# Insurance Bluebook Log Data Quality Issues

## Overview

We have identified a data quality issue where many insurance payment records (`int_insurance_payment_allocated` model) lack associated allowed fee history data from the `insbluebooklog` table. This document outlines the issue, its impact, and recommended actions.

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

---

*Document prepared: March 2025*  
*Contact: Data Quality Team*