# Treatment Acceptance - Model Analysis & Design

## Executive Summary

This document analyzes existing models and outlines how to build the Treatment Acceptance dashboard using existing models plus one new intermediate model and one new mart.

**Key Finding**: We can reuse 95% of existing models. We only need to add:
1. **`int_treatment_plan_acceptance`** - New intermediate model for exam linking and acceptance logic
2. **`mart_treatment_plan_summary`** - New mart for daily grain metrics

---

## Existing Models Analysis

### ✅ Models We Can Use Directly

#### 1. `int_treatment_plan` ✅
**What it provides:**
- Treatment plan headers (date, status, patient, signatures)
- Procedure aggregations (count, amounts, completion tracking)
- Acceptance flags (patient signed, practice signed, fully accepted)
- Completion tracking (completed procedures, completed amounts)
- Provider/clinic from procedures
- Timeline analysis (days to first procedure, days since last activity)

**What we need from it:**
- Treatment plan date (for date filtering)
- Patient ID (for exam linking)
- Treatment plan status (Active only)
- Procedure aggregations (for acceptance calculations)
- Treatment plan amounts (for financial metrics)

**Limitations:**
- ❌ Uses provider from procedures (not exam provider)
- ❌ Only tracks completed procedures (not scheduled)
- ❌ No exam linking
- ❌ No same-day treatment identification

#### 2. `int_procedure_complete` ✅
**What it provides:**
- Complete procedure data with fees
- Procedure dates and status
- Appointment ID (for scheduled procedure identification)
- Procedure codes and categories
- Provider and clinic information

**What we need from it:**
- Procedure status (for completed vs scheduled)
- Appointment ID (for scheduled procedure identification)
- Procedure date (for same-day treatment)
- Procedure fee (for financial metrics)
- Procedure code/category (for exam identification)

**Key Field:**
- `appointment_id` - Links to appointments for scheduled procedure identification

#### 3. `fact_appointment` ✅
**What it provides:**
- Appointment status (Scheduled=1, Completed=2, etc.)
- Appointment dates (for scheduled procedure identification)
- Provider and clinic information
- Patient information

**What we need from it:**
- Appointment status (to identify scheduled appointments)
- Appointment date (to identify future appointments)
- Provider/clinic (for exam provider assignment)

**Key Fields:**
- `appointment_status` - Status 1 = Scheduled, Status 2 = Completed
- `appointment_date` - Date of appointment (for future date comparison)

#### 4. `dim_procedure` ✅
**What it provides:**
- Procedure catalog with categories
- Procedure category (Diagnostic for exams)
- Clinical flags (is_radiology, is_hygiene, etc.)

**What we need from it:**
- Procedure category (to identify exams: Diagnostic = D0 codes)
- `is_radiology` flag (alternative exam identification)

**Key Fields:**
- `procedure_category` - 'Diagnostic' for exams
- `is_radiology` - Alternative exam identification

#### 5. `fact_procedure` ⚠️
**What it provides:**
- Procedure facts with date_id
- Completed procedures only (filters for status 2, 4)

**Limitation:**
- ❌ Only includes completed procedures (filters out scheduled)
- ❌ Not useful for acceptance logic (need scheduled procedures)

**Recommendation:** Use `int_procedure_complete` instead (includes all procedures)

---

## What We Need to Build

### 1. New Intermediate: `int_treatment_plan_acceptance`

**Purpose:** Enhance `int_treatment_plan` with exam linking and acceptance logic

**Builds on:**
- `int_treatment_plan` (treatment plan data)
- `int_procedure_complete` (procedure data with appointment_id)
- `fact_appointment` (appointment status and dates)
- `dim_procedure` (procedure categories for exam identification)

**Adds:**
1. **Exam Linking Logic**
   - Find exams performed within 30 days before treatment plan creation
   - Use most recent exam before treatment plan date
   - Link exam provider and clinic to treatment plan

2. **Acceptance Logic**
   - **Completed**: Procedure status = 2 (Completed)
   - **Scheduled**: Procedure has appointment_id AND appointment_date > treatment_plan_date AND appointment_status = 1 (Scheduled)
   - **Accepted**: Completed OR Scheduled
   - **Patient Accepted**: At least one procedure accepted

3. **Same-Day Treatment**
   - Procedures completed on treatment_plan_date
   - Sum of procedure fees for same-day completions

4. **Enhanced Metrics**
   - Accepted procedure count (completed + scheduled)
   - Accepted amount (completed + scheduled)
   - Same-day treatment amount
   - Exam provider ID (for provider assignment)
   - Exam clinic ID (for clinic assignment)

**Grain:** One row per treatment_plan_id (same as `int_treatment_plan`)

---

### 2. New Mart: `mart_treatment_plan_summary`

**Purpose:** Daily grain mart for treatment acceptance metrics

**Builds on:**
- `int_treatment_plan_acceptance` (enhanced treatment plan data)
- `fact_appointment` (for patients seen metric)
- `int_procedure_complete` (for exam identification)

**Metrics:**
- **Volume Metrics:**
  - `patients_seen` - Unique patients with appointments (from fact_appointment)
  - `patients_with_exams` - Unique patients with exam procedures (from int_procedure_complete)
  - `patients_presented` - Unique patients with treatment plans (from int_treatment_plan_acceptance)
  - `patients_accepted` - Unique patients who accepted at least one item
  - `treatment_plans_presented` - Count of treatment plans
  - `treatment_plans_accepted` - Count of treatment plans with at least one accepted item

- **Financial Metrics:**
  - `tx_presented_amount` - Total treatment plan value presented
  - `tx_accepted_amount` - Total treatment plan value accepted
  - `same_day_treatment_amount` - Same-day treatment value

- **Percentage Metrics:**
  - `tx_acceptance_rate` - (Tx Accepted / Tx Presented) * 100
  - `patient_acceptance_rate` - (Patients Accepted / Patients Presented) * 100
  - `diagnosis_rate` - (Patients Presented / Patients with Exams) * 100

**Grain:** One row per date + exam_provider_id + exam_clinic_id
- Date = treatment_plan_date (date when treatment plan was created)
- Provider = exam_provider_id (provider who performed the exam)
- Clinic = exam_clinic_id (clinic where exam was performed)

**Filtering:**
- Only Active treatment plans (treatment_plan_status = 0)
- Only treatment plans with exams within 30 days

---

## Implementation Strategy

### Phase 1: Build `int_treatment_plan_acceptance`

**Step 1: Exam Identification**
```sql
-- Identify exam procedures (Diagnostic category or is_radiology)
WITH exam_procedures AS (
    SELECT
        pc.procedure_id,
        pc.patient_id,
        pc.procedure_date,
        pc.provider_id,
        pc.clinic_id,
        pc.procedure_status,
        dp.procedure_category,
        dp.is_radiology
    FROM {{ ref('int_procedure_complete') }} pc
    INNER JOIN {{ ref('dim_procedure') }} dp
        ON pc.procedure_code_id = dp.procedure_code_id
    WHERE dp.procedure_category = 'Diagnostic'
        OR dp.is_radiology = true
    AND pc.procedure_status = 2  -- Completed exams only
)
```

**Step 2: Exam Linking to Treatment Plans**
```sql
-- Link exams to treatment plans (within 30 days before treatment plan)
WITH treatment_plan_exams AS (
    SELECT
        tp.treatment_plan_id,
        tp.patient_id,
        tp.treatment_plan_date,
        e.procedure_id as exam_procedure_id,
        e.procedure_date as exam_date,
        e.provider_id as exam_provider_id,
        e.clinic_id as exam_clinic_id,
        ROW_NUMBER() OVER (
            PARTITION BY tp.treatment_plan_id
            ORDER BY e.procedure_date DESC
        ) as exam_rank
    FROM {{ ref('int_treatment_plan') }} tp
    INNER JOIN exam_procedures e
        ON tp.patient_id = e.patient_id
        AND e.procedure_date >= tp.treatment_plan_date - INTERVAL '30 days'
        AND e.procedure_date <= tp.treatment_plan_date
    WHERE tp.treatment_plan_status = 0  -- Active only
)
SELECT * FROM treatment_plan_exams WHERE exam_rank = 1
```

**Step 3: Acceptance Logic**
```sql
-- Identify accepted procedures (completed OR scheduled)
WITH accepted_procedures AS (
    SELECT
        tpa.treatment_plan_id,
        tpa.procedure_id,
        pc.procedure_status,
        pc.procedure_fee,
        pc.procedure_date,
        pc.appointment_id,
        a.appointment_date,
        a.appointment_status,
        -- Accepted if completed OR scheduled
        CASE
            WHEN pc.procedure_status = 2 THEN true  -- Completed
            WHEN pc.appointment_id IS NOT NULL 
                AND a.appointment_date > tp.treatment_plan_date
                AND a.appointment_status = 1 THEN true  -- Scheduled
            ELSE false
        END as is_accepted,
        -- Same-day treatment
        CASE
            WHEN pc.procedure_status = 2 
                AND pc.procedure_date = tp.treatment_plan_date THEN true
            ELSE false
        END as is_same_day_treatment
    FROM {{ ref('int_treatment_plan') }} tp
    INNER JOIN {{ ref('stg_opendental__treatplanattach') }} tpa
        ON tp.treatment_plan_id = tpa.treatplan_id
    LEFT JOIN {{ ref('int_procedure_complete') }} pc
        ON tpa.procedure_id = pc.procedure_id
    LEFT JOIN {{ ref('fact_appointment') }} a
        ON pc.appointment_id = a.appointment_id
)
```

**Step 4: Aggregate Acceptance Metrics**
```sql
-- Aggregate acceptance metrics per treatment plan
WITH treatment_plan_acceptance AS (
    SELECT
        tp.treatment_plan_id,
        tp.patient_id,
        tp.treatment_plan_date,
        tp.treatment_plan_status,
        tp.total_planned_amount,
        -- Exam information
        tpe.exam_provider_id,
        tpe.exam_clinic_id,
        -- Acceptance metrics
        COUNT(DISTINCT CASE WHEN ap.is_accepted THEN ap.procedure_id END) as accepted_procedure_count,
        SUM(CASE WHEN ap.is_accepted THEN ap.procedure_fee ELSE 0 END) as accepted_amount,
        SUM(CASE WHEN ap.is_same_day_treatment THEN ap.procedure_fee ELSE 0 END) as same_day_treatment_amount,
        -- Patient acceptance
        CASE
            WHEN COUNT(DISTINCT CASE WHEN ap.is_accepted THEN ap.procedure_id END) > 0 THEN true
            ELSE false
        END as patient_accepted
    FROM {{ ref('int_treatment_plan') }} tp
    LEFT JOIN treatment_plan_exams tpe
        ON tp.treatment_plan_id = tpe.treatment_plan_id
    LEFT JOIN accepted_procedures ap
        ON tp.treatment_plan_id = ap.treatment_plan_id
    WHERE tp.treatment_plan_status = 0  -- Active only
    GROUP BY tp.treatment_plan_id, tp.patient_id, tp.treatment_plan_date, 
             tp.treatment_plan_status, tp.total_planned_amount,
             tpe.exam_provider_id, tpe.exam_clinic_id
)
```

---

### Phase 2: Build `mart_treatment_plan_summary`

**Step 1: Aggregate by Date + Provider + Clinic**
```sql
-- Aggregate treatment plan metrics by date + provider + clinic
WITH treatment_plan_daily AS (
    SELECT
        DATE(tpa.treatment_plan_date) as treatment_plan_date,
        tpa.exam_provider_id as provider_id,
        COALESCE(tpa.exam_clinic_id, 0) as clinic_id,
        -- Volume metrics
        COUNT(DISTINCT tpa.patient_id) as patients_presented,
        COUNT(DISTINCT CASE WHEN tpa.patient_accepted THEN tpa.patient_id END) as patients_accepted,
        COUNT(DISTINCT tpa.treatment_plan_id) as treatment_plans_presented,
        COUNT(DISTINCT CASE WHEN tpa.patient_accepted THEN tpa.treatment_plan_id END) as treatment_plans_accepted,
        -- Financial metrics
        SUM(tpa.total_planned_amount) as tx_presented_amount,
        SUM(tpa.accepted_amount) as tx_accepted_amount,
        SUM(tpa.same_day_treatment_amount) as same_day_treatment_amount
    FROM {{ ref('int_treatment_plan_acceptance') }} tpa
    WHERE tpa.exam_provider_id IS NOT NULL  -- Only treatment plans with exams
    GROUP BY DATE(tpa.treatment_plan_date), tpa.exam_provider_id, tpa.exam_clinic_id
)
```

**Step 2: Add Patients Seen and Patients with Exams**
```sql
-- Add patients seen and patients with exams
WITH patients_seen AS (
    SELECT
        DATE(a.appointment_date) as appointment_date,
        a.provider_id,
        COALESCE(a.clinic_id, 0) as clinic_id,
        COUNT(DISTINCT a.patient_id) as patients_seen
    FROM {{ ref('fact_appointment') }} a
    WHERE a.appointment_status IN (1, 2)  -- Scheduled or Completed
    GROUP BY DATE(a.appointment_date), a.provider_id, a.clinic_id
),
patients_with_exams AS (
    SELECT
        DATE(pc.procedure_date) as procedure_date,
        pc.provider_id,
        COALESCE(pc.clinic_id, 0) as clinic_id,
        COUNT(DISTINCT pc.patient_id) as patients_with_exams
    FROM {{ ref('int_procedure_complete') }} pc
    INNER JOIN {{ ref('dim_procedure') }} dp
        ON pc.procedure_code_id = dp.procedure_code_id
    WHERE (dp.procedure_category = 'Diagnostic' OR dp.is_radiology = true)
        AND pc.procedure_status = 2  -- Completed exams
    GROUP BY DATE(pc.procedure_date), pc.provider_id, pc.clinic_id
)
```

**Step 3: Final Mart with All Metrics**
```sql
-- Final mart with all metrics
SELECT
    tpd.treatment_plan_date as date_id,
    tpd.provider_id,
    tpd.clinic_id,
    -- Volume metrics
    COALESCE(ps.patients_seen, 0) as patients_seen,
    COALESCE(pwe.patients_with_exams, 0) as patients_with_exams,
    tpd.patients_presented,
    tpd.patients_accepted,
    tpd.treatment_plans_presented,
    tpd.treatment_plans_accepted,
    -- Financial metrics
    tpd.tx_presented_amount,
    tpd.tx_accepted_amount,
    tpd.same_day_treatment_amount,
    -- Percentage metrics
    CASE
        WHEN tpd.tx_presented_amount > 0
        THEN (tpd.tx_accepted_amount / tpd.tx_presented_amount) * 100
        ELSE 0
    END as tx_acceptance_rate,
    CASE
        WHEN tpd.patients_presented > 0
        THEN (tpd.patients_accepted::numeric / tpd.patients_presented) * 100
        ELSE 0
    END as patient_acceptance_rate,
    CASE
        WHEN COALESCE(pwe.patients_with_exams, 0) > 0
        THEN (tpd.patients_presented::numeric / pwe.patients_with_exams) * 100
        ELSE 0
    END as diagnosis_rate
FROM treatment_plan_daily tpd
LEFT JOIN patients_seen ps
    ON tpd.treatment_plan_date = ps.appointment_date
    AND tpd.provider_id = ps.provider_id
    AND tpd.clinic_id = ps.clinic_id
LEFT JOIN patients_with_exams pwe
    ON tpd.treatment_plan_date = pwe.procedure_date
    AND tpd.provider_id = pwe.provider_id
    AND tpd.clinic_id = pwe.clinic_id
```

---

## Model Dependencies

```
int_treatment_plan (existing)
    ↓
int_treatment_plan_acceptance (NEW)
    ├── int_treatment_plan (treatment plan data)
    ├── int_procedure_complete (procedure data with appointment_id)
    ├── fact_appointment (appointment status and dates)
    └── dim_procedure (procedure categories for exam identification)
    ↓
mart_treatment_plan_summary (NEW)
    ├── int_treatment_plan_acceptance (enhanced treatment plan data)
    ├── fact_appointment (patients seen metric)
    └── int_procedure_complete (patients with exams metric)
```

---

## Key Design Decisions

### 1. Exam Identification
- **Use:** `dim_procedure.procedure_category = 'Diagnostic'` OR `is_radiology = true`
- **Reason:** Most accurate exam identification
- **Alternative:** Could use procedure codes starting with 'D0', but category is more reliable

### 2. Exam Selection
- **Use:** Most recent exam within 30 days before treatment plan date
- **Reason:** Aligns with PBN requirement
- **Implementation:** `ROW_NUMBER() OVER (PARTITION BY treatment_plan_id ORDER BY exam_date DESC)`

### 3. Provider Assignment
- **Use:** Exam provider (not procedure provider)
- **Reason:** Aligns with PBN requirement ("Provider must have performed an exam within 30 days")
- **Implementation:** Use `exam_provider_id` from exam linking

### 4. Acceptance Definition
- **Completed:** Procedure status = 2
- **Scheduled:** Procedure has appointment_id AND appointment_date > treatment_plan_date AND appointment_status = 1
- **Reason:** Matches PBN definition ("Accepted = completed OR scheduled")

### 5. Same-Day Treatment
- **Use:** Procedures completed on treatment_plan_date
- **Reason:** Most literal interpretation of "same day"
- **Implementation:** `procedure_date = treatment_plan_date AND procedure_status = 2`

### 6. Patients Seen
- **Use:** Unique patients with appointments (from fact_appointment)
- **Reason:** Broader definition, aligns with "patients seen" concept
- **Implementation:** Count distinct patient_id from fact_appointment where appointment_status IN (1, 2)

### 7. Treatment Plan Status
- **Use:** Only Active treatment plans (status = 0)
- **Reason:** Inactive/Saved plans are not presented to patients
- **Implementation:** Filter `treatment_plan_status = 0`

---

## Testing Strategy

### 1. Exam Linking Tests
- Verify exams are linked within 30 days
- Verify most recent exam is selected when multiple exams exist
- Verify treatment plans without exams are excluded

### 2. Acceptance Logic Tests
- Verify completed procedures are marked as accepted
- Verify scheduled procedures are marked as accepted
- Verify unscheduled procedures are not marked as accepted
- Verify patient acceptance when at least one procedure accepted

### 3. Same-Day Treatment Tests
- Verify same-day treatment only includes procedures on treatment_plan_date
- Verify same-day treatment only includes completed procedures

### 4. Mart Aggregation Tests
- Verify daily grain (one row per date + provider + clinic)
- Verify metrics are aggregated correctly
- Verify percentage calculations are accurate
- Verify patients_seen matches fact_appointment data
- Verify patients_with_exams matches exam procedure data

---

## Performance Considerations

### 1. Indexing
- Index `int_treatment_plan_acceptance` on:
  - `treatment_plan_id` (primary key)
  - `treatment_plan_date` (for date filtering)
  - `exam_provider_id` (for provider filtering)
  - `exam_clinic_id` (for clinic filtering)
  - `patient_id` (for patient-level aggregation)

- Index `mart_treatment_plan_summary` on:
  - `date_id`, `provider_id`, `clinic_id` (composite key)
  - `date_id` (for date range filtering)
  - `provider_id` (for provider filtering)

### 2. Materialization
- `int_treatment_plan_acceptance`: Table (treatment plans change infrequently)
- `mart_treatment_plan_summary`: Table (daily aggregation, query performance)

### 3. Refresh Frequency
- `int_treatment_plan_acceptance`: Weekly (treatment plans change infrequently)
- `mart_treatment_plan_summary`: Daily (for dashboard freshness)

---

## Next Steps

1. **Build `int_treatment_plan_acceptance`**
   - Implement exam linking logic
   - Implement acceptance logic
   - Add same-day treatment identification
   - Test with real data

2. **Build `mart_treatment_plan_summary`**
   - Implement daily aggregation
   - Add all metrics
   - Test calculations
   - Add dbt tests

3. **Build API Endpoints**
   - `/treatment-acceptance/summary` - Summary metrics
   - `/treatment-acceptance/provider-performance` - Provider breakdown
   - `/treatment-acceptance/trending` - Time series data

4. **Build Dashboard**
   - Create React components
   - Add PBN-style gauges
   - Add date range filtering
   - Add provider/clinic filtering

---

## References

- `int_treatment_plan.sql` - Treatment plan intermediate model
- `int_procedure_complete.sql` - Procedure complete intermediate model
- `fact_appointment.sql` - Appointment fact table
- `dim_procedure.sql` - Procedure dimension table
- PBN Treatment Acceptance Dashboard Image
- PBN KPI Definitions

