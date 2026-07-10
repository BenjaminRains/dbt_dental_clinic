# SCD (Slowly Changing Dimension) Analysis for dbt Dental Models

## Overview
This document identifies fact and dimension models in the marts layer that could benefit from SCD Type 1 (overwrite) or SCD Type 2 (historical tracking) implementations.

---

## Dimensions Analysis

### 1. **dim_patient** - **SCD Type 2 Recommended** ⭐⭐⭐

**Current State:** Single row per patient_id with current values

**Fields That Change Over Time:**
- `patient_status` - Patient status can change (Active → Inactive → Deceased)
- `primary_provider_id` - Patients can switch providers
- `secondary_provider_id` - Secondary provider assignments change
- `clinic_id` - Patients can move between clinics
- `fee_schedule_id` - Fee schedule assignments can change
- `estimated_balance`, `total_balance` - Financial balances change daily
- `balance_0_30_days`, `balance_31_60_days`, etc. - Aging buckets change
- `has_insurance_flag` - Insurance status changes
- `disease_count`, `disease_ids` - Disease tracking changes
- `document_count` - Document counts change
- `age`, `age_category` - Age changes (though this is calculated)
- `preferred_contact_method`, `preferred_confirmation_method` - Contact preferences change
- `guarantor_id` - Guarantor relationships can change

**Business Value:**
- Track patient status changes over time
- Historical provider assignments for attribution analysis
- Financial balance history for AR aging trends
- Insurance status history for coverage analysis

**Implementation Notes:**
- Natural key: `patient_id`
- Effective date: `_updated_at` or create `effective_date`
- Expiration date: `expiration_date` or `is_current` flag
- Current record indicator: `is_current = true`

---

### 2. **dim_provider** - **SCD Type 2 Recommended** ⭐⭐⭐

**Current State:** Single row per provider_id with current values

**Fields That Change Over Time:**
- `provider_status`, `provider_status_description` - Status changes (Active → Inactive → Terminated)
- `termination_date` - Provider termination tracking
- `specialty_id`, `specialty_description` - Specialties can change
- `fee_schedule_id` - Fee schedule assignments change
- `is_hidden`, `is_secondary` - Visibility flags change
- `hourly_production_goal_amount` - Goals change over time
- `scheduled_days`, `availability_percentage` - Availability metrics change (90-day rolling)
- `provider_status_category` - Status category changes
- `availability_performance_tier` - Performance tier changes

**Business Value:**
- Historical provider performance analysis
- Track provider status changes and terminations
- Historical availability metrics for scheduling optimization
- Provider goal changes over time

**Implementation Notes:**
- Natural key: `provider_id`
- Effective date: `_updated_at` or `termination_date`
- Current record indicator: `is_current = true`
- Note: Availability metrics are 90-day rolling, so Type 2 would capture snapshots

---

### 3. **dim_procedure** - **SCD Type 1 Recommended** ⭐

**Current State:** Single row per procedure_code_id

**Fields That Change Over Time:**
- `standard_fee`, `min_available_fee`, `max_available_fee`, `avg_fee_amount` - Fees change
- `fee_schedule_id`, `standard_fee_id` - Fee schedule assignments change
- `has_standard_fee`, `has_multiple_fees` - Fee availability changes

**Fields That Don't Change:**
- `procedure_code`, `procedure_description` - Procedure codes are static
- `procedure_category`, `complexity_level` - Business logic categorizations are stable
- `is_hygiene`, `is_prosthetic`, `is_radiology` - Clinical flags are static

**Business Value:**
- Track fee changes for pricing analysis
- Historical fee trends for procedure profitability

**Implementation Notes:**
- Natural key: `procedure_code_id`
- SCD Type 1 sufficient for fee updates (overwrite old values)
- If historical fee tracking needed, consider SCD Type 2

---

### 4. **dim_fee_schedule** - **SCD Type 2 Recommended** ⭐⭐

**Current State:** Single row per fee_schedule_id

**Fields That Change Over Time:**
- `is_hidden` - Visibility flags change
- `is_active_schedule` - Active status changes
- `fee_schedule_status`, `fee_schedule_category` - Status changes
- `date_updated` - Schedule updates

**Business Value:**
- Track fee schedule lifecycle
- Historical fee schedule assignments
- Audit trail for schedule changes

**Implementation Notes:**
- Natural key: `fee_schedule_id`
- Effective date: `date_updated` or `_updated_at`
- Current record indicator: `is_current = true`

---

### 5. **dim_clinic** - **SCD Type 2 Recommended** ⭐⭐

**Current State:** Single row per clinic_id

**Fields That Change Over Time:**
- `clinic_name`, `clinic_abbreviation` - Names can change
- `address_line_1`, `address_line_2`, `city`, `state`, `zip_code` - Addresses change
- `phone_number`, `fax_number`, `email_alias` - Contact info changes
- `billing_address_*` - Billing addresses change
- `is_hidden` - Visibility flags change
- `is_active_clinic` - Active status changes
- `clinic_status`, `clinic_type` - Status changes
- `default_provider_id` - Default provider changes
- `timezone` - Timezone could change
- `sms_monthly_limit` - SMS limits change

**Business Value:**
- Historical clinic information for location analysis
- Track clinic status changes and closures
- Address change history for compliance
- Configuration change tracking

**Implementation Notes:**
- Natural key: `clinic_id`
- Effective date: `_updated_at` or create `effective_date`
- Current record indicator: `is_current = true`

---

### 6. **dim_insurance** - **SCD Type 2 Recommended** ⭐⭐⭐

**Current State:** Single row per insurance_plan_id

**Fields That Change Over Time:**
- `is_active` - Active status changes frequently
- `verification_date` - Verification dates update
- `network_status_current` - Network status changes
- `claim_approval_rate`, `average_reimbursement_rate` - Performance metrics change
- `total_claims`, `approved_claims`, `denied_claims` - Claim counts change
- `total_billed_amount`, `total_paid_amount` - Financial metrics change
- `benefit_details` - Benefit details can change
- `effective_date`, `termination_date` - Coverage periods change

**Business Value:**
- Historical insurance plan performance
- Track network status changes
- Claim performance trends over time
- Coverage period tracking

**Implementation Notes:**
- Natural key: `insurance_plan_id`
- Effective date: `verification_date` or `_updated_at`
- Current record indicator: `is_current = true`
- Note: Performance metrics are aggregated, so Type 2 would capture periodic snapshots

---

## **FOCUS: dim_insurance SCD Type 2 for Benefit Amount Tracking**

### Business Use Case: Year-End Insurance Benefit Targeting

**Business Owner's Need:**
> "I want more insight on patient's insurance amounts at different points in time. I think I can target certain patients that have production noted in their treatment plans and they might be more responsive if they have insurance money available - especially if the year is almost over."

### Key Fields to Track with SCD Type 2

#### 1. **Benefit Amount Fields** (Currently Missing from dim_insurance)
These fields exist in `int_ar_analysis` but should be added to `dim_insurance`:
- `annual_max_remaining` - **CRITICAL** - Remaining annual maximum benefit
- `annual_max_met` - Amount of annual maximum already used
- `deductible_remaining` - **CRITICAL** - Remaining deductible amount
- `deductible_met` - Amount of deductible already met
- `coverage_percent` - Insurance coverage percentage

#### 2. **Time-Sensitive Fields**
- `effective_date` - When coverage starts
- `termination_date` - When coverage ends
- `verification_date` - Last verification date
- `benefit_reset_date` - When annual benefits reset (typically Jan 1 or plan anniversary)

### SCD Type 2 Implementation Strategy

#### **Approach 1: Daily Snapshots (Recommended for Year-End Targeting)**
Create daily snapshots of insurance benefit amounts to track:
- How much insurance money is available each day
- How benefits deplete over time as claims are processed
- Year-end urgency (benefits expiring Dec 31)

**Schema Changes:**
```sql
-- Add to dim_insurance
effective_date DATE,           -- When this snapshot is valid
expiration_date DATE,          -- When next snapshot takes over (NULL for current)
is_current BOOLEAN,           -- TRUE for current snapshot
snapshot_date DATE,           -- Date of the snapshot

-- Benefit tracking fields
annual_max_remaining NUMERIC,
annual_max_met NUMERIC,
deductible_remaining NUMERIC,
deductible_met NUMERIC,
coverage_percent NUMERIC,
benefit_reset_date DATE,      -- When benefits reset next

-- Calculated urgency fields
days_until_benefit_reset INTEGER,
benefit_utilization_percent NUMERIC,  -- annual_max_met / annual_max_total
is_year_end_urgency BOOLEAN,  -- TRUE if within 30 days of reset
```

#### **Approach 2: Event-Based Snapshots**
Create snapshots when benefit amounts change:
- When a claim is processed (reduces annual_max_remaining)
- When verification occurs (updates benefit amounts)
- When coverage period changes

**Trade-off:** More complex but fewer rows. Daily snapshots are simpler and better for year-end queries.

### Business Value Scenarios

#### **Scenario 1: Year-End Patient Targeting**
**Query:** Find patients with:
- Treatment plans with remaining production > $500
- Annual max remaining > $500
- Within 30 days of benefit reset (year-end)
- Active insurance coverage

```sql
-- Point-in-time query for Nov 15, 2024
SELECT 
    p.patient_id,
    p.patient_name,
    tp.total_planned_amount as treatment_plan_production,
    tp.remaining_amount as treatment_plan_remaining,
    di.annual_max_remaining,
    di.deductible_remaining,
    di.days_until_benefit_reset,
    di.benefit_utilization_percent
FROM dim_insurance di
JOIN dim_patient p ON di.patient_id = p.patient_id
JOIN int_treatment_plan tp ON p.patient_id = tp.patient_id
WHERE di.is_current = true
    AND di.effective_date <= '2024-11-15'
    AND (di.expiration_date > '2024-11-15' OR di.expiration_date IS NULL)
    AND di.annual_max_remaining > 500
    AND tp.remaining_amount > 500
    AND di.days_until_benefit_reset <= 30
    AND di.is_active = true
ORDER BY di.annual_max_remaining DESC, di.days_until_benefit_reset ASC;
```

**Business Impact:**
- Target patients who will lose benefits if not used
- Prioritize by amount available and urgency
- Increase treatment plan acceptance rates
- Capture revenue before benefits expire

#### **Scenario 2: Benefit Depletion Tracking**
**Query:** Track how benefits deplete over time for a patient

```sql
-- Historical view of benefit changes
SELECT 
    snapshot_date,
    annual_max_remaining,
    annual_max_met,
    deductible_remaining,
    days_until_benefit_reset
FROM dim_insurance
WHERE patient_id = 12345
    AND insurance_plan_id = 67890
ORDER BY snapshot_date DESC;
```

**Business Impact:**
- Understand patient benefit utilization patterns
- Identify optimal timing for treatment plan presentation
- Track benefit depletion velocity

#### **Scenario 3: Treatment Plan + Insurance Alignment**
**Query:** Match treatment plan production to available insurance benefits

```sql
-- Find patients where treatment plan fits within insurance benefits
SELECT 
    p.patient_id,
    tp.total_planned_amount,
    tp.remaining_amount,
    di.annual_max_remaining,
    di.deductible_remaining,
    CASE 
        WHEN tp.remaining_amount <= di.annual_max_remaining 
        THEN 'Fully Covered'
        WHEN tp.remaining_amount <= di.annual_max_remaining * 1.2 
        THEN 'Mostly Covered'
        ELSE 'Partially Covered'
    END as coverage_status,
    di.days_until_benefit_reset
FROM dim_insurance di
JOIN dim_patient p ON di.patient_id = p.patient_id
JOIN int_treatment_plan tp ON p.patient_id = tp.patient_id
WHERE di.is_current = true
    AND tp.treatment_plan_status = 'Active'
    AND di.is_active = true
    AND di.annual_max_remaining > 0;
```

**Business Impact:**
- Present treatment plans when insurance coverage is optimal
- Increase patient acceptance by showing insurance will cover costs
- Prioritize patients with expiring benefits

### Implementation Considerations

#### **Data Sources for Benefit Amounts:**
1. **From `int_ar_analysis`** (already calculated):
   - `annual_max_remaining`
   - `annual_max_met`
   - `deductible_remaining`
   - `deductible_met`

2. **From `int_insurance_coverage`**:
   - `benefit_details` JSON (contains benefit rules)
   - `effective_date`, `termination_date`

3. **From Claims Processing**:
   - Track when claims reduce annual_max_remaining
   - Track when deductibles are met

#### **Snapshot Frequency:**
- **Daily snapshots** recommended for year-end targeting
- Run after ETL processes claims (typically overnight)
- Capture benefit amounts at end of each day

#### **Performance Optimization:**
- Index on `(patient_id, snapshot_date)` for patient history queries
- Index on `(is_current, days_until_benefit_reset)` for year-end queries
- Partition by `snapshot_date` for large historical tables

#### **Query Patterns:**
```sql
-- Current benefits (most common)
WHERE is_current = true

-- Point-in-time (for historical analysis)
WHERE effective_date <= @target_date 
    AND (expiration_date > @target_date OR expiration_date IS NULL)

-- Year-end urgency (for targeting)
WHERE is_current = true
    AND days_until_benefit_reset <= 30
    AND annual_max_remaining > 500
```

### Expected Business Outcomes

1. **Increased Treatment Plan Acceptance**
   - Target patients when insurance benefits are available
   - Show clear financial benefit to patients

2. **Year-End Revenue Capture**
   - Identify patients with expiring benefits
   - Prioritize outreach by benefit amount and urgency

3. **Better Patient Communication**
   - "You have $1,200 in insurance benefits expiring in 15 days"
   - "Your treatment plan of $800 is fully covered by insurance"

4. **Data-Driven Scheduling**
   - Schedule appointments before benefits expire
   - Optimize timing for maximum insurance coverage

### Next Steps

1. **Enhance dim_insurance** with benefit amount fields
2. **Implement daily snapshot process** for SCD Type 2
3. **Create year-end targeting query** for business owner
4. **Build dashboard** showing:
   - Patients with expiring benefits
   - Treatment plan + insurance alignment
   - Benefit utilization trends

---

### 7. **dim_date** - **No SCD Needed** ❌

**Current State:** Static date dimension

**Analysis:** Date dimensions are static reference tables. No SCD implementation needed.

---

## Facts Analysis

### General Note on Facts
Fact tables typically represent events or transactions at a point in time and don't require SCD. However, some considerations:

### 1. **fact_appointment** - **No SCD Needed** ❌
- Represents appointment events at specific points in time
- Historical snapshots not needed (events are immutable)

### 2. **fact_payment** - **No SCD Needed** ❌
- Represents payment transactions at specific points in time
- Historical snapshots not needed (transactions are immutable)

### 3. **fact_claim** - **No SCD Needed** ❌
- Represents claim transactions at specific points in time
- Historical snapshots not needed (transactions are immutable)

### 4. **fact_procedure** - **No SCD Needed** ❌
- Represents procedure executions at specific points in time
- Historical snapshots not needed (events are immutable)

### 5. **fact_communication** - **No SCD Needed** ❌
- Represents communication events at specific points in time
- Historical snapshots not needed (events are immutable)

---

## Summary Recommendations

### High Priority SCD Type 2 Implementations:
1. **dim_patient** - Critical for patient lifecycle and financial tracking
2. **dim_provider** - Important for provider performance and status tracking
3. **dim_insurance** - Essential for insurance plan performance and network status

### Medium Priority SCD Type 2 Implementations:
4. **dim_clinic** - Useful for clinic configuration and status tracking
5. **dim_fee_schedule** - Helpful for fee schedule lifecycle tracking

### Low Priority SCD Type 1 Implementation:
6. **dim_procedure** - Fee updates can be handled with Type 1 (overwrite)

### No SCD Needed:
- **dim_date** - Static dimension
- All **fact_*** tables - Event/transaction tables don't need SCD

---

## Implementation Considerations

### SCD Type 2 Standard Columns:
- `effective_date` - When the record becomes valid
- `expiration_date` - When the record expires (NULL for current)
- `is_current` - Boolean flag for current record (true/false)
- `surrogate_key` - May need to change from natural key to composite key

### dbt Packages:
Consider using `dbt_scd_type_2` macro or similar for automated SCD Type 2 implementation:
- `dbt-utils` for incremental logic
- Custom macros for SCD Type 2 logic

### Performance Considerations:
- Index on `is_current = true` for current record queries
- Index on natural key + effective_date for historical lookups
- Consider partitioning large SCD tables by effective_date

### Query Patterns:
- Current records: `WHERE is_current = true`
- Point-in-time: `WHERE effective_date <= @date AND (expiration_date > @date OR expiration_date IS NULL)`
- Historical changes: `WHERE natural_key = @key ORDER BY effective_date`

---

## Next Steps

1. **Prioritize** which dimensions to implement SCD Type 2 first
2. **Design** SCD Type 2 schema changes (add effective_date, expiration_date, is_current)
3. **Create** dbt macros for SCD Type 2 logic
4. **Implement** incremental models for SCD Type 2 updates
5. **Update** downstream models to use `is_current = true` filters
6. **Test** historical queries and point-in-time analysis
