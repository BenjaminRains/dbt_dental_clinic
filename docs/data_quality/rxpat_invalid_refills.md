# Prescription Refills Data Quality Issue

## Issue Summary
During data quality validation on October 20, 2025, we identified **1 prescription** in `stg_opendental__rxpat` with an invalid refills value that falls outside the expected range of 0-9.

**Identified Date:** October 20, 2025  
**Severity:** WARN (currently), should be cleaned up  
**Status:** Single data entry error requiring correction

## Affected Prescription

| RX ID | Patient ID | Drug | Refills | Prescription Date | Status |
|-------|------------|------|---------|-------------------|--------|
| 55 | 6940 | Amoxicillin 500mg #12 | **999** | 2017-04-11 | Active |

**Full Details:**
- **Instructions (SIG):** "Take 4 tabs 1 hr prior to appt"
- **Dispense Instructions:** "12"
- **Provider ID:** 1
- **Pharmacy ID:** 0 (no specific pharmacy)
- **Created:** 2020-04-24 (entered retrospectively for 2017 prescription)
- **Last Updated:** 2020-04-24

## Business Rules for Refills

### Valid Refills Values
- **0**: No refills authorized (one-time dispensing)
- **1-9**: Specific number of refills authorized
- **Empty string**: Refills not specified (often for one-time medications)

### Invalid Values
- **999**: Not a valid refills count
  - Likely represents:
    - Data entry error (typo)
    - Attempted to code "unlimited" or "as needed"
    - Misunderstanding of the field purpose
  - **Not acceptable** in a regulated prescription system

## Clinical Context

**Amoxicillin Pre-Medication:**
- This is a **pre-procedural antibiotic** (prophylactic dose)
- Standard protocol: Take 4 tablets 1 hour before dental appointment
- Typical dosing: Single use only (no refills needed)
- **Expected refills value: 0** (one-time prophylactic dose)

**Why 999 is Wrong:**
1. **Medical appropriateness:** Prophylactic antibiotics are single-use, not refillable
2. **Regulatory compliance:** DEA and state pharmacy boards track refill patterns
3. **Data integrity:** Prescription systems expect numeric 0-9 for refill counts
4. **Clinical safety:** Unlimited antibiotics could lead to resistance or adverse effects

## Root Cause Analysis

**Data Entry Error:**
- Prescription entered in 2020 for a 2017 appointment (retrospective documentation)
- Staff member may have been unsure of refills field and entered '999' as placeholder
- No form validation prevented the invalid entry
- Single occurrence suggests isolated mistake, not systemic issue

## Business Impact

### Current Impact (Low)
- **Single record:** Only 1 out of thousands of prescriptions
- **Old prescription:** From 2017, likely already filled and completed
- **No ongoing risk:** Prescription is historical, not active renewal

### Potential Risks if Unresolved
1. **Reporting accuracy:** Prescription analytics may miscount refills
2. **System integration:** e-Prescribing systems may reject invalid refills values
3. **Compliance audits:** Pharmacy board audits could flag invalid refills data
4. **Future patterns:** If left uncorrected, staff may repeat the error

## Immediate Action Required

**Priority: LOW (single historical record, but should be cleaned up)**

### 1. Data Cleanup (This Week)
```sql
-- Fix the refills value for rx_id 55
UPDATE rxpat 
SET Refills = '0' 
WHERE RxNum = 55;
```

**Rationale:** Pre-procedural antibiotics should have 0 refills (single use only)

### 2. Verify No Other Invalid Values
```sql
-- Check for any other non-numeric or out-of-range refills
SELECT RxNum, Refills, Drug, RxDate
FROM rxpat
WHERE Refills NOT IN ('0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '')
  AND Refills IS NOT NULL;
```

### 3. Add Form Validation (Optional)
- Consider adding dropdown selection for refills (0-9 only)
- Or add field validation to reject non-numeric values
- **Note:** This may not be necessary if it's truly a one-time error

## Monitoring Strategy

### Current dbt Test Configuration
```yaml
- name: refills
  tests:
    - accepted_values:
        values: ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '']
        severity: warn
```

**Test Behavior:**
- Currently set to **WARN** severity
- Will flag this record but not block dbt builds
- Once rx_id 55 is fixed, the test will pass
- Future invalid refills will be caught and flagged as warnings

**Recommendation:** Keep as WARN
- This is a single historical record
- Low business impact
- Not worth blocking dbt builds over
- Test will automatically pass once cleaned up

## How to Fix in OpenDental

### Step 1: Locate the Prescription
1. Open OpenDental
2. Navigate to patient ID 6940
3. Go to **Chart Module → Medications/Prescriptions**
4. Find prescription for "Amoxicillin 500mg" dated 2017-04-11

### Step 2: Edit the Prescription
1. Right-click on the prescription → Edit
2. Change **Refills** field from "999" to "0"
3. Save the prescription

### Step 3: Verify the Fix
1. Re-run the ETL pipeline to reload prescription data
2. Re-run dbt tests: `dbt test --select stg_opendental__rxpat`
3. Verify the refills test passes

## Expected Outcome

Once corrected:
- ✅ Prescription will have refills = '0' (clinically appropriate)
- ✅ dbt test will pass (no warnings)
- ✅ Prescription data will be clean for reporting
- ✅ Compliance audits will not flag invalid refills

## Summary for Stakeholders

> A single historical prescription (rx_id 55, Amoxicillin 500mg from 2017) has an invalid refills value of '999' instead of the expected range 0-9. This appears to be a data entry error from retrospective documentation in 2020. The prescription should have 0 refills (appropriate for pre-procedural antibiotics). This is a low-priority cleanup item that can be corrected in OpenDental by updating the refills field to '0'. The dbt test is set to WARN severity and will automatically pass once the data is corrected.

## Related Documentation
- See `models/staging/opendental/_stg_opendental__rxpat.yml` for complete prescription data model
- Consult OpenDental documentation for prescription management workflows
- Review pharmacy compliance requirements for refills tracking

