# Synthetic Data Dashboard Fix Plan

## Problem Summary

The executive dashboard shows unrealistic zero values for:
- **Revenue Lost**: $0.00 (should show lost revenue from broken/no-show appointments)
- **Recovery Potential**: $0.00 (should show estimated recoverable amounts)
- **Collection Rate**: 0.0% (should show realistic collection percentage)

## Root Cause Analysis

### Issue 1: Revenue Lost = $0.00

**Root Cause:**
1. `fact_appointment.scheduled_production_amount` is **hardcoded to 0.00** (line 140 in `fact_appointment.sql`)
2. `mart_revenue_lost` filters: `WHERE scheduled_production_amount > 0` (line 61)
3. **Broken appointments don't get procedures** in synthetic data generator (only completed appointments do - line 245-252 in `clinical_generator.py`)
4. Result: No broken appointments pass the filter, so `mart_revenue_lost` is empty

**Evidence:**
- `fact_appointment.sql` line 140: `0.00 as scheduled_production_amount,`
- `mart_revenue_lost.sql` line 61: `and scheduled_production_amount > 0`
- `clinical_generator.py` line 245: `if status == APPOINTMENT_STATUS['COMPLETED']:` (only creates procedures for completed appointments)

### Issue 2: Collection Rate = 0.0%

**Root Cause:**
Collection rate calculation has TWO different paths:

1. **Dashboard endpoint** (`/reports/dashboard/kpis`) uses:
   - `mart_provider_performance.collection_efficiency` 
   - Which comes from `mart_production_summary.collections`
   - Which uses `fact_claim.paid_amount` (line 73) - **INSURANCE PAYMENTS ONLY**

2. **AR service** (`get_ar_kpi_summary`) uses:
   - `fact_payment.payment_amount` directly - **BOTH patient and insurance payments**

**The Problem:**
- `mart_production_summary` line 73: `coalesce(fc.paid_amount, 0) as collected_amount`
- This only includes insurance claim payments, not patient direct payments
- For synthetic data (mostly direct-pay), `fact_claim` may be empty or have no paid amounts
- Result: `collections = 0`, so `collection_efficiency = 0%`

**The Fix:**
Update `mart_production_summary` to calculate `collections` from `int_payment_split` (which includes both patient and insurance payments) instead of just `fact_claim.paid_amount`.

**Note:** The model already has `payment_metrics` CTE that uses `int_payment_split`, but it's not being used for the `collections` field - it's only used for separate `patient_payments` and `insurance_payments` fields.

## Solution Plan

### Phase 1: Fix `fact_appointment.scheduled_production_amount` Calculation

**File:** `dbt_dental_models/models/marts/fact_appointment.sql`

**Current Code (line 140):**
```sql
0.00 as scheduled_production_amount,
```

**Fix:**
Calculate from linked procedures using `int_procedure_complete`:
```sql
coalesce(
    (select sum(pc.procedure_fee)
     from {{ ref('int_procedure_complete') }} pc
     where pc.appointment_id = ab.appointment_id
       and pc.procedure_date = date(ab.appointment_datetime)),
    0.00
) as scheduled_production_amount,
```

**Why This Works:**
- Links to procedures via `int_procedure_complete` (which includes all procedures, even for broken appointments if they exist)
- Uses `procedure_fee` to calculate total scheduled production
- Falls back to 0.00 if no procedures exist

### Phase 2: Update Synthetic Data Generator to Create Procedures for Broken Appointments

**File:** `etl_pipeline/synthetic_data_generator/generators/clinical_generator.py`

**Current Code (line 245-252):**
```python
# Generate procedures for completed appointments
if status == APPOINTMENT_STATUS['COMPLETED']:
    appt_procedures = self._generate_procedures_for_appointment(...)
```

**Fix:**
Create procedures for **both completed AND broken appointments** (but mark broken appointment procedures differently):
```python
# Generate procedures for appointments (both completed and broken)
# Broken appointments should have procedures to calculate lost revenue
if status in [APPOINTMENT_STATUS['COMPLETED'], APPOINTMENT_STATUS['BROKEN']]:
    appt_procedures = self._generate_procedures_for_appointment(
        proc_id, appt_id, patient_id, provider_id, clinic_id,
        appt_date, appt_type_idx
    )
    # For broken appointments, mark procedures as "planned but not completed"
    if status == APPOINTMENT_STATUS['BROKEN']:
        # Optionally mark procedures differently (e.g., ProcStatus = 1 for treatment planned)
        # But keep ProcFee so scheduled_production_amount can be calculated
        pass
    procedures.extend(appt_procedures)
    proc_id += len(appt_procedures)
```

**Alternative Approach (Recommended):**
Keep broken appointments without procedures, but calculate `scheduled_production_amount` from appointment type fees:
- Use appointment type duration and standard procedure codes
- Calculate estimated production based on appointment type
- This is more realistic (broken appointments had planned procedures, but they weren't performed)

**Recommended Implementation:**
Add a helper method to calculate scheduled production from appointment type:
```python
def _calculate_scheduled_production_from_appointment_type(self, appt_type_idx: int) -> float:
    """Calculate estimated production amount based on appointment type"""
    # Map appointment types to typical procedure fees
    appt_type_fees = {
        0: 250.00,  # Comprehensive Exam (D0150 + D0210 + D0274)
        1: 150.00,  # Periodic Exam (D0120 + D0274)
        2: 200.00,  # Emergency
        3: 120.00,  # Hygiene Adult
        4: 100.00,  # Hygiene Child
        5: 1200.00, # Crown Prep
        6: 800.00,  # Crown Seat
        7: 300.00,  # Filling
        8: 1500.00, # Root Canal
        9: 250.00,  # Extraction
        # ... etc
    }
    return appt_type_fees.get(appt_type_idx, 200.00)  # Default $200
```

Then in `fact_appointment.sql`, calculate from appointment type if no procedures exist:
```sql
coalesce(
    (select sum(pc.procedure_fee)
     from {{ ref('int_procedure_complete') }} pc
     where pc.appointment_id = ab.appointment_id
       and pc.procedure_date = date(ab.appointment_datetime)),
    -- Fallback: estimate from appointment type if no procedures
    case 
        when ab.appointment_type_name = 'Comprehensive Exam' then 250.00
        when ab.appointment_type_name = 'Periodic Exam' then 150.00
        when ab.appointment_type_name = 'Hygiene Adult' then 120.00
        when ab.appointment_type_name = 'Crown Prep' then 1200.00
        when ab.appointment_type_name = 'Filling' then 300.00
        -- ... etc
        else 200.00  -- Default estimate
    end
) as scheduled_production_amount,
```

### Phase 3: Fix Collection Rate Calculation ✅ COMPLETE

**Root Cause Identified:**
- `mart_production_summary` was using `fact_claim.paid_amount` for collections (line 73)
- This only includes **insurance payments**, not patient direct payments
- For synthetic data (mostly direct-pay), this results in `collections = 0`
- Dashboard uses `mart_provider_performance.collection_efficiency` which comes from this

**Fix Applied:**
- Updated `mart_production_summary.sql` to calculate `collections` from `payment_metrics` (int_payment_split)
- Now uses: `patient_payments + insurance_payments` instead of just `fact_claim.paid_amount`
- This includes ALL payment types (patient direct-pay + insurance), not just insurance claims

**Files Modified:**
- `dbt_dental_models/models/marts/mart_production_summary.sql`:
  - Line 73: Renamed `collected_amount` to `collected_amount_from_claims` (for reference)
  - Line 187: Set `collections = 0.0` (placeholder, calculated in final CTE)
  - Line 268: Calculate `collections` from `payment_metrics`: `patient_payments + insurance_payments`
  - Line 270: Updated `collection_rate` calculation to use new `collections` value

**Verification Needed:**
- [ ] Run dbt models: `dbt run --select mart_production_summary mart_provider_performance --target demo`
- [ ] Verify collection rate is now > 0% in dashboard
- [ ] Check that payment dates are within last 365 days (synthetic data should have recent payments)

### Phase 4: Update Synthetic Data Generator Configuration

**File:** `etl_pipeline/synthetic_data_generator/main.py`

**Current:**
- `appointment_completion_rate: float = 0.75` (75% completed, 25% broken)

**Consider:**
- Keep 75% completion rate (realistic)
- But ensure broken appointments have scheduled production amounts calculated

## Implementation Order

1. **Phase 1** (Fix `fact_appointment.scheduled_production_amount`) - **HIGHEST PRIORITY**
   - This will immediately fix Revenue Lost calculation
   - Can use appointment type fallback if procedures don't exist

2. **Phase 2** (Update synthetic data generator) - **MEDIUM PRIORITY**
   - Either create procedures for broken appointments OR
   - Use appointment type-based calculation in `fact_appointment`

3. **Phase 3** (Verify collection rate) - **MEDIUM PRIORITY**
   - Investigate payment data
   - Fix payment generation if needed

4. **Phase 4** (Configuration updates) - **LOW PRIORITY**
   - Fine-tune completion rates if needed

## Testing Plan

After implementing fixes:

1. **Regenerate synthetic data** (if Phase 2 changes are made)
2. **Run dbt models:**
   ```bash
   dbt run --select fact_appointment mart_revenue_lost mart_provider_performance --target demo
   ```
3. **Verify data:**
   ```sql
   -- Check revenue lost
   SELECT 
       COUNT(*) as opportunities,
       SUM(lost_revenue) as total_revenue_lost,
       SUM(estimated_recoverable_amount) as total_recovery_potential
   FROM raw_marts.mart_revenue_lost;
   
   -- Check collection rate
   SELECT 
       (SELECT SUM(payment_amount) 
        FROM raw_marts.fact_payment 
        WHERE payment_date >= CURRENT_DATE - INTERVAL '365 days'
          AND payment_direction = 'Income') as total_collections,
       (SELECT SUM(actual_fee) 
        FROM raw_marts.fact_procedure fp
        INNER JOIN raw_marts.dim_date dd ON fp.date_id = dd.date_id
        WHERE dd.date_day >= CURRENT_DATE - INTERVAL '365 days') as total_production;
   ```
4. **Test API endpoint:**
   ```bash
   curl https://api.dbtdentalclinic.com/reports/dashboard/kpis \
     -H "X-API-Key: <DEMO_API_KEY>"
   ```
5. **Verify dashboard:**
   - Visit `https://dbtdentalclinic.com`
   - Check executive dashboard shows non-zero values
   - Verify values are realistic (not all zeros)

## Success Criteria

- ✅ Revenue Lost > $0 (should be 5-15% of total production)
- ✅ Recovery Potential > $0 (should be 30-60% of Revenue Lost)
- ✅ Collection Rate between 85-100% (realistic for dental practice)
- ✅ Dashboard displays realistic metrics
- ✅ All dbt models run successfully
- ✅ No data quality errors

## Estimated Effort

- **Phase 1**: 1-2 hours (SQL fix in `fact_appointment.sql`)
- **Phase 2**: 2-3 hours (synthetic data generator updates)
- **Phase 3**: 1-2 hours (payment verification and fixes)
- **Phase 4**: 0.5 hours (configuration updates)
- **Testing**: 1-2 hours (regenerate data, run dbt, verify)

**Total: 5.5-9.5 hours**

## Notes

- Phase 1 can be implemented immediately and will fix Revenue Lost
- Phase 2 may require regenerating synthetic data (backup current data first)
- Collection rate fix depends on investigation results
- All changes should be tested on demo database first before production
