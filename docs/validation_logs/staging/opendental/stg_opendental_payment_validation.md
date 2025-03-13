# Payment Staging Model Validation

## Latest Validation Results
**Date:** 2024-03-14
**Model:** stg_opendental__payment

### Data Overview
- Total Records: 11,766
- Date Range: 2023-01-02 to 2025-02-28
- Source Table: opendental.payment

### Test Results
✅ All tests passing (11/11):
1. Unique payment_ids
2. Not null checks (payment_id, patient_id, amount, date, type)
3. Valid payment types
4. Date range validations
5. Payment type business rules

### Payment Type Distribution
```sql
select 
    payment_type_id,
    count(*) as count,
    round(avg(payment_amount), 2) as avg_amount,
    min(payment_amount) as min_amount,
    max(payment_amount) as max_amount
from stg_opendental__payment
group by payment_type_id
order by count desc;
```

| Type | Count | Avg Amount | Description |
|------|-------|------------|-------------|
| 71   | 8,335 | $293      | Regular payments |
| 0    | 1,110 | $0        | Administrative |
| 69   | 965   | $1,036    | High value |
| 70   | 609   | $348      | Regular |
| 391  | 482   | $922      | High value |
| 412  | 187   | $199      | Newer type |
| 72   | 50    | -$699     | Refunds |
| 634  | 16    | $6,009    | New since Sept 2024 |
| 574  | 6     | $26,071   | Very high value |
| 417  | 6     | $2,967    | Special cases |

### Validation Rules
1. Date Filtering:
   - Include: >= 2023-01-01
   - Exclude: Invalid dates (< 2000-01-01)
   - Exclude: Future dates

2. Payment Type Rules:
   - Type 0: Must have $0 amount
   - Type 72: Must be negative

### Changes Made
1. Added unique_key config to prevent duplicates
2. Implemented date filtering
3. Added comprehensive validation tests
4. Documented payment type patterns