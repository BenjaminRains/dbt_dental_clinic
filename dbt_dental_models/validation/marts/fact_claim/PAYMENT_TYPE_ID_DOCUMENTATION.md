# Payment Type ID Documentation Across Models

## Summary

`payment_type_id` values are documented in multiple YAML files across the dbt models. This document tracks where each value is documented and ensures consistency.

---

## Models with `payment_type_id` Documentation

### 1. `stg_opendental__claimpayment` (Source)
**File**: `models/staging/opendental/_stg_opendental__claimpayment.yml`

**Accepted Values**: `[0, 69, 70, 71, 72, 261, 303, 391, 412, 417, 464, 465, 466, 467, 469, 574, 634, 645, 646, 647, 661]`

**Documentation**:
- **Regular Payments**: 0, 69, 70, 71, 261, 303, 391, 412, 464, 465, 466, 467, 469, 574, 634
- **Refund Types**: 72, 645, 646, 647
- **661**: Listed in accepted_values but not described (needs documentation)

**Status**: ✅ **Most Complete** - This is the source of truth for all payment types

---

### 2. `int_insurance_payment_allocated` (Insurance Payments Only)
**File**: `models/intermediate/system_c_payment/_int_insurance_payment_allocated.yml`

**Accepted Values**: `[261, 303, 464, 465, 466, 469]`

**Documentation**:
- 261: Insurance Check
- 303: Insurance Electronic Payment
- 464: Insurance Credit (Cigna EFT)
- 465: Insurance Credit (Delta EFT)
- 466: Insurance Electronic Payment (Echo E Check)
- 469: Insurance Check (United Concordia EFT)

**Status**: ✅ **Insurance-specific** - Only includes insurance payment types (excludes patient payment types and refunds)

---

### 3. `int_patient_payment_allocated` (Patient Payments Only)
**File**: `models/intermediate/system_c_payment/_int_patient_payment_allocated.yml`

**Accepted Values**: `[0, 69, 70, 71, 72, 391, 412, 417, 574, 634]`

**Documentation**:
- Patient payment types (not insurance)
- Includes administrative entries (0) and refunds (72)

**Status**: ✅ **Patient-specific** - Only includes patient payment types

---

### 4. `stg_opendental__payment` (All Payments)
**File**: `models/staging/opendental/_stg_opendental__payment.yml`

**Accepted Values**: `[0, 69, 70, 71, 72, 391, 412, 417, 574, 634, 676]`

**Documentation**: Patient payment types (similar to int_patient_payment_allocated)

**Status**: ✅ **Patient payments** - Includes newer type 676

---

### 5. `fact_claim` (Insurance Payments Only)
**File**: `models/marts/_fact_claim.yml`

**Accepted Values**: `[261, 303, 464, 465, 466, 467, 469, 645, 646, 647, 661]`

**Documentation**: 
- Regular insurance payments: 261, 303, 464, 465, 466, 467, 469
- Refund types: 645, 646, 647, 661

**Status**: ✅ **Updated** - Now matches insurance payment types from staging model

---

## Value Mapping

### Insurance Payment Types (in `fact_claim`):
| Value | Description | Source | Status |
|-------|-------------|--------|--------|
| 261 | Insurance Check (High volume) | `_stg_opendental__claimpayment.yml` | ✅ Documented |
| 303 | Insurance Electronic Payment | `_stg_opendental__claimpayment.yml` | ✅ Documented |
| 464 | Insurance Credit (Cigna EFT) | `_stg_opendental__claimpayment.yml` | ✅ Documented |
| 465 | Insurance Credit (Delta EFT) | `_stg_opendental__claimpayment.yml` | ✅ Documented |
| 466 | Insurance Electronic Payment (Echo E Check) | `_stg_opendental__claimpayment.yml` | ✅ Documented |
| 467 | Electronic payment | `_stg_opendental__claimpayment.yml` | ✅ Documented |
| 469 | Insurance Check (United Concordia EFT) | `_stg_opendental__claimpayment.yml` | ✅ Documented |
| 645 | Large refunds (avg -$268) | `_stg_opendental__claimpayment.yml` | ✅ Documented |
| 646 | Small refunds (avg -$1) | `_stg_opendental__claimpayment.yml` | ✅ Documented |
| 647 | Medium refunds (avg -$88) | `_stg_opendental__claimpayment.yml` | ✅ Documented |
| 661 | Insurance refunds (avg -$106, range -$196 to -$55) | `_stg_opendental__claimpayment.yml` | ✅ **Documented** - All 12 records are negative (refunds) |

---

## Recommendations

### 1. Add Description for 661
The value `661` is in the accepted_values list in `_stg_opendental__claimpayment.yml` but doesn't have a description. Should add:
- Query to see if it's a refund type (negative amounts) or regular payment
- Check average amount and carrier patterns
- Add description to staging model YAML

### 2. Consistency Check
- ✅ `fact_claim` now matches insurance payment types from staging
- ✅ `int_insurance_payment_allocated` is subset of insurance types (excludes refunds)
- ✅ All values in `fact_claim` are documented in `_stg_opendental__claimpayment.yml`

### 3. Documentation Source
- **Primary Source**: `_stg_opendental__claimpayment.yml` (most complete)
- **Reference**: `fact_claim` should reference staging model for complete documentation
- **Insurance-specific**: `int_insurance_payment_allocated.yml` for insurance-only context

---

## Action Items

1. ✅ **Completed**: Updated `fact_claim.yml` to include all insurance payment types (645, 646, 647, 661)
2. ✅ **Completed**: Confirmed `661` is a refund type (all 12 records have negative amounts, avg -$106)
3. ✅ **Completed**: Updated `fact_claim.yml` description to reference staging model

---

## Query to Investigate 661

```sql
-- Investigate payment_type_id = 661
SELECT 
    payment_type_id,
    COUNT(*) as record_count,
    AVG(check_amount) as avg_amount,
    MIN(check_amount) as min_amount,
    MAX(check_amount) as max_amount,
    COUNT(CASE WHEN check_amount < 0 THEN 1 END) as negative_count,
    COUNT(CASE WHEN check_amount > 0 THEN 1 END) as positive_count,
    COUNT(CASE WHEN check_amount = 0 THEN 1 END) as zero_count,
    STRING_AGG(DISTINCT carrier_id::text, ', ' ORDER BY carrier_id::text) as carrier_ids,
    MIN(check_date) as earliest_payment_date,
    MAX(check_date) as latest_payment_date
FROM marts.fact_claim
WHERE payment_type_id = 661
GROUP BY payment_type_id;

-- Additional query: Sample records to see patterns
SELECT 
    claim_id,
    procedure_id,
    check_amount,
    check_date,
    carrier_id,
    payment_type_id,
    CASE 
        WHEN check_amount < 0 THEN 'Refund'
        WHEN check_amount > 0 THEN 'Payment'
        ELSE 'Zero'
    END as payment_category
FROM marts.fact_claim
WHERE payment_type_id = 661
ORDER BY check_date DESC
LIMIT 20;
```

This will help determine if 661 is a refund type or regular payment type.
