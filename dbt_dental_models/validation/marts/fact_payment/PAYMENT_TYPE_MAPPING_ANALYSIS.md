# Payment Type Mapping Investigation

**Date**: 2026-01-23  
**Status**: 🔍 **IN PROGRESS**  
**Investigation File**: `investigate_payment_type_mappings.sql`  
**Related Documentation**: `validation/VALIDATION_TEMPLATE.md` - Validation framework and template

---

## Executive Summary

**Issue**: Need to verify that `payment_type` correctly maps all `payment_type_id` values from the source system.

**Current Logic** (from `fact_payment.sql` lines 133-141):
```sql
case ps.payment_type_id
    when 0 then 'Patient'
    when 1 then 'Insurance'
    when 2 then 'Partial'
    when 3 then 'PrePayment'
    when 4 then 'Adjustment'
    when 5 then 'Refund'
    else 'Unknown'
end as payment_type
```

**Documented Valid Values** (from `PAYMENT_TYPE_ID_DOCUMENTATION.md`):
- **Source Values**: `[0, 69, 70, 71, 72, 261, 303, 391, 412, 417, 464, 465, 466, 467, 469, 574, 634, 645, 646, 647, 661]`
- **Currently Mapped**: `[0, 1, 2, 3, 4, 5]` (6 values)
- **Potentially Unmapped**: `[69, 70, 71, 72, 261, 303, 391, 412, 417, 464, 465, 466, 467, 469, 574, 634, 645, 646, 647, 661]` (15+ values)

**Investigation Goals**:
1. Verify what `payment_type_id` values exist in source data
2. Check count of `payment_type = 'Unknown'` records
3. Identify which unmapped `payment_type_id` values should be categorized
4. Determine appropriate payment_type categories for unmapped values
5. Verify `payment_direction` and `payment_size_category` completeness (likely complete based on amount ranges)

---

## Investigation Queries

Run the following investigation queries to understand the data:

**Location**: `validation/marts/fact_payment/investigate_payment_type_mappings.sql`

**Key Queries**:
1. **Query 1**: Payment Type ID Distribution in Source
   - Distribution of `payment_type_id` values in staging
   - NULL value check
   - Distribution in intermediate model
   
2. **Query 2**: Payment Type Distribution in Mart
   - Summary of `payment_type` values
   - Breakdown by `payment_type_id` and `payment_type`
   - Sample records for each category

3. **Query 3**: Unknown Payment Type Analysis
   - Detailed analysis of Unknown records
   - Breakdown by `payment_type_id` values
   - Sample records

4. **Query 4**: Payment Type ID vs Payment Type Comparison
   - Cross-tabulation of ID and type
   - Check for unexpected mappings

5. **Query 5**: Payment Type ID Values from Documentation
   - Check which documented values exist in source
   - Compare documented values vs current mappings

6. **Query 6**: Payment Direction and Size Category Analysis
   - Distribution of `payment_direction` and `payment_size_category`
   - Check for edge cases

7. **Query 7**: Summary Statistics
   - Overall counts and percentages
   - Payment Type ID coverage summary

---

## Expected Findings

Based on the current logic and documentation, we expect:
- **Currently Mapped**: `payment_type_id` values [0, 1, 2, 3, 4, 5] should map correctly
- **Unknown**: Any `payment_type_id` values not in [0, 1, 2, 3, 4, 5] will map to 'Unknown'
- **Documented but Unmapped**: Values like [69, 70, 71, 72, 261, 303, 391, 412, 417, 464, 465, 466, 467, 469, 574, 634, 645, 646, 647, 661] from documentation

**Questions to Answer**:
1. How many payments have `payment_type = 'Unknown'`?
2. What `payment_type_id` values are causing Unknown categorizations?
3. Are these unmapped values patient payments, insurance payments, or other types?
4. Should unmapped values be grouped into existing categories (Patient, Insurance, etc.) or need new categories?
5. Are `payment_direction` and `payment_size_category` working correctly for all payment types?

---

## Payment Type ID Documentation Reference

From `PAYMENT_TYPE_ID_DOCUMENTATION.md`:

### Insurance Payment Types (in `fact_claim`):
- **Regular**: 261, 303, 464, 465, 466, 467, 469
- **Refunds**: 645, 646, 647, 661

### Patient Payment Types (in `stg_opendental__payment`):
- **Regular**: 0, 69, 70, 71, 391, 412, 417, 574, 634
- **Refunds**: 72
- **Other**: 676 (newer type)

### All Documented Values:
`[0, 69, 70, 71, 72, 261, 303, 391, 412, 417, 464, 465, 466, 467, 469, 574, 634, 645, 646, 647, 661]`

**Note**: `fact_payment` includes both patient and insurance payments, so it should handle all these values.

---

## Investigation Results

*Results will be documented here after running investigation queries*

---

## Root Cause Analysis

*To be completed after investigation*

---

## Recommended Actions

*To be completed after investigation*

**Potential Actions**:
1. Map additional `payment_type_id` values to appropriate categories
2. Group similar payment types (e.g., all insurance EFT types → 'Insurance')
3. Update CASE statement to handle all documented values
4. Update documentation in `_fact_payment.yml` with complete mapping
5. Verify `payment_direction` and `payment_size_category` handle all cases correctly

---

## Related Documentation

- **Investigation SQL**: `validation/marts/fact_payment/investigate_payment_type_mappings.sql`
- **Model Logic**: `models/marts/fact_payment.sql` (lines 133-141)
- **Model Documentation**: `models/marts/_fact_payment.yml` (need to verify)
- **Payment Type ID Documentation**: `validation/marts/fact_claim/PAYMENT_TYPE_ID_DOCUMENTATION.md`
- **Validation Framework**: `validation/README.md` - How validation works
- **Staging Model**: `models/staging/opendental/_stg_opendental__payment.yml`
- **Staging Claim Payment**: `models/staging/opendental/_stg_opendental__claimpayment.yml`
