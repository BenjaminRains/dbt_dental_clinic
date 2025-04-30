# Payment Data Quality Issues

## Known Issues

### 1. Incorrect Payment Amount Calculations

**Status**: Open
**Severity**: High
**Affected Models**: 
- `int_patient_payment_allocated`
- `int_ar_shared_calculations`
- `int_ar_analysis`

**Description**:
The payment calculations are currently producing extremely large negative values for patient payments. This issue manifests in the following ways:

1. Patient payments show unreasonably large negative amounts (e.g., -683,167,104, -556,361,365)
2. Insurance payments appear to be more reasonable (e.g., 175.0, 1295.0)
3. Total payments are dominated by these large negative patient payments

**Impact**:
- AR analysis reports incorrect payment amounts
- Patient balance calculations are inaccurate
- Payment reconciliation is not reliable

**Root Cause Investigation Needed**:
1. Trace payment amounts from source tables (`stg_opendental__payment` and `stg_opendental__paysplit`)
2. Verify payment amount transformations in intermediate models
3. Check aggregation logic in `int_ar_analysis.sql`
4. Investigate potential data quality issues in raw payment data

**Temporary Workaround**:
None currently implemented. This issue needs to be addressed before the payment calculations can be used reliably.

**Next Steps**:
1. Review payment amount handling in each model
2. Add data quality checks for payment amounts
3. Implement proper sign handling for different payment types
4. Add validation tests to catch similar issues in the future

**Related Files**:
- `models/intermediate/system_c_payment/int_patient_payment_allocated.sql`
- `models/intermediate/system_d_ar_analysis/int_ar_shared_calculations.sql`
- `models/intermediate/system_d_ar_analysis/int_ar_analysis.sql` 