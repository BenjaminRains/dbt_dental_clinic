# Hygiene Retention Dashboard Development Plan

## Overview

This document outlines the development plan for building a Hygiene Retention dashboard that answers the operational question: **"Are hygiene appointments productive?"** (Q5 from EXPOSURES_DEVELOPMENT_PLAN.md).

The dashboard will follow Practice By Numbers (PBN) design patterns and implement their key performance indicators (KPIs) for hygiene retention and recall management.

**Target URL**: `http://localhost:3000/hygiene-retention`  
**Status**: Planning Phase - Updated with PBN Business Logic  
**Estimated Effort**: 6-8 hours total (3-4 hours backend, 3-4 hours frontend)

---

## PBN Business Logic Summary

This plan has been updated to align with Practice By Numbers (PBN) business logic definitions. Key clarifications:

1. **Recall Current %**: Denominator is ALL active patients (visited in past 18 months), numerator is those current on recall
2. **Hyg Pre-Appointment Any %**: Based on hygiene PROCEDURES (not just appointments), checks if patient scheduled ANY future appointment after hygiene. Numerator: patients with hygiene procedure who scheduled another appointment. Denominator: total patients with hygiene procedures.
3. **Hyg Patients Seen**: Unique count of patients with hygiene PROCEDURES (each patient counted once, regardless of number of procedures). Based on `int_procedure_complete.is_hygiene = true` as primary method.
4. **Hyg Pts Re-appntd**: COUNT (not percentage) of unique patients who had hygiene procedure and then established another appointment (any type). Step 1: identify unique hygiene patients. Step 2: count those with future appointments.
5. **Recall Overdue %**: Denominator is active patients ON recall program (not all active patients)
6. **Not on Recall %**: Denominator is ALL patients in database (not just active patients)

All SQL logic has been updated to reflect these PBN definitions.

---

## Business Context

### Operational Question: Q5 - Are hygiene appointments productive?

**Business Impact**: 
- Hygiene is high-margin revenue
- Preventive care drives patient retention
- Recare scheduling is critical for practice health

**Key Metrics**:
- Recall compliance rates
- Pre-appointment scheduling effectiveness
- Patient retention after hygiene visits
- Recall program enrollment

**Update Frequency**: Daily to weekly

---

## KPI Definitions and Calculations

### KPI 1: Recall Current % (Primary Metric)

**KPI Name**: Recall Current %  
**KPI Long Name**: % Active Patients Current on Recall  
**Target Value**: 70-80% (industry benchmark)

**Description**:
Tracks the percentage of active patients in the dental office who are up-to-date with their recall appointments. Being current on recall means that patients have attended their scheduled follow-up dental check-ups within the recommended time frame.

**Active Patients Definition**:
- Patients who have visited the dental office in the past 18 months
- Have an ongoing relationship with the practice
- Excludes inactive, archived, or deleted patients

**Current on Recall Definition** (PBN Business Logic):
- Patient has attended their scheduled follow-up dental check-ups within the recommended time frame
- Being "current" means the patient is up-to-date with their recall appointments
- This typically means:
  - Recall due date is in the future (not overdue), OR
  - Recall appointment is scheduled on time (before due date), OR
  - Recall appointment was completed within the compliance window

**Calculation**:
```sql
Recall Current % = (Active patients current on recall / Total active patients) * 100
```

**Example**:
- Total active patients: 200
- Patients current on recall: 150
- Recall Current % = (150 / 200) * 100 = 75%

**Data Sources**:
- `dim_patient` - Patient demographics and status
- `fact_appointment` - Last visit date (to determine active patients)
- `int_recall_management` - Recall status, date_due, compliance_status

**SQL Logic**:
```sql
WITH active_patients AS (
    SELECT DISTINCT p.patient_id
    FROM raw_marts.dim_patient p
    INNER JOIN raw_marts.fact_appointment fa
        ON p.patient_id = fa.patient_id
    WHERE fa.appointment_date >= CURRENT_DATE - INTERVAL '18 months'
        AND p.patient_status IN ('Patient', 'Active')  -- Exclude inactive/archived
),
patients_with_recall AS (
    SELECT DISTINCT irm.patient_id,
        CASE 
            WHEN irm.date_due IS NULL THEN false
            WHEN irm.date_due >= CURRENT_DATE THEN true  -- Future due date = current
            WHEN irm.recall_status_description = 'Scheduled' 
                AND irm.date_scheduled <= irm.date_due THEN true  -- Scheduled on time
            WHEN irm.recall_status_description = 'Completed' THEN true  -- Completed
            WHEN irm.compliance_status = 'Compliant' THEN true
            ELSE false
        END as is_current_on_recall
    FROM raw_intermediate.int_recall_management irm
    WHERE irm.is_disabled = false
        AND irm.is_valid_recall = true
)
SELECT 
    COUNT(DISTINCT CASE WHEN pwr.is_current_on_recall THEN ap.patient_id END)::numeric 
        / COUNT(DISTINCT ap.patient_id)::numeric * 100 as recall_current_percent
FROM active_patients ap
LEFT JOIN patients_with_recall pwr ON ap.patient_id = pwr.patient_id
```

---

### KPI 2: Hyg Pre-Appointment Any % (Primary Metric)

**KPI Name**: Hyg Pre-Appointment Any %  
**KPI Long Name**: Hygiene Pre-Appointment Any %  
**Target Value**: 80-90% (industry benchmark)

**Description** (PBN Business Logic):
Measures the percentage of patients who, after having any hygiene procedure done within a specified time range, scheduled another appointment. This subsequent appointment can be for either a doctor or another hygiene procedure.

**Purpose**: 
- Assess effectiveness of patient retention strategies after hygiene procedures
- Ensures patients continue to receive ongoing care
- Improves overall patient health and clinic revenue

**Calculation Overview** (PBN):
The calculation is simple arithmetic:
- **Numerator**: The number of patients who had any hygiene procedure and scheduled another appointment within the time range
- **Denominator**: The total number of patients who had any hygiene procedure within the time range
- **Formula**: Divide the numerator by the denominator and multiply by 100

**Calculation**:
```sql
Hyg Pre-Appointment % = (Patients with next appointment after hygiene / Total hygiene patients) * 100
```

**Example**:
- Total hygiene patients: 356
- Patients with next appointment scheduled: 314
- Hyg Pre-Appointment % = (314 / 356) * 100 = 88.2%

**Data Sources** (PBN Business Logic):
- **Primary**: `int_procedure_complete` - Hygiene procedures identified by `is_hygiene = true` (PBN emphasizes "any hygiene procedure")
- **Secondary**: `fact_appointment` - Hygiene appointments identified by `is_hygiene_appointment = true` OR `hygienist_id IS NOT NULL`
- **Appointment Tracking**: Check if patient has any future appointment (any type) scheduled after hygiene procedure date
- **Key Point**: Based on hygiene PROCEDURES, not just appointments with hygienists

**SQL Logic** (PBN Approach - Procedure-Based):
```sql
WITH hygiene_procedures AS (
    -- Identify patients who had ANY hygiene procedure in the time range (PBN: procedure-based)
    SELECT DISTINCT
        pc.patient_id,
        pc.procedure_date as hygiene_date,
        pc.appointment_id
    FROM raw_intermediate.int_procedure_complete pc
    WHERE pc.is_hygiene = true
        AND pc.procedure_date >= COALESCE(:start_date, CURRENT_DATE - INTERVAL '12 months')
        AND pc.procedure_date <= COALESCE(:end_date, CURRENT_DATE)
),
hygiene_patients AS (
    -- Unique patients who had hygiene procedures (fallback to appointments if no procedure data)
    SELECT DISTINCT
        COALESCE(hp.patient_id, fa.patient_id) as patient_id,
        COALESCE(hp.hygiene_date, fa.appointment_date) as hygiene_date
    FROM hygiene_procedures hp
    FULL OUTER JOIN (
        SELECT DISTINCT
            fa.patient_id,
            fa.appointment_date
        FROM raw_marts.fact_appointment fa
        WHERE (fa.is_hygiene_appointment = true 
               OR fa.hygienist_id IS NOT NULL)
            AND fa.appointment_date >= COALESCE(:start_date, CURRENT_DATE - INTERVAL '12 months')
            AND fa.appointment_date <= COALESCE(:end_date, CURRENT_DATE)
    ) fa ON hp.patient_id = fa.patient_id
),
patients_with_next_appt AS (
    -- Patients who scheduled another appointment (any type) after hygiene
    -- PBN: "scheduled another appointment" means they have a future appointment booked
    SELECT DISTINCT hp.patient_id
    FROM hygiene_patients hp
    WHERE EXISTS (
        SELECT 1 
        FROM raw_marts.fact_appointment fa2
        WHERE fa2.patient_id = hp.patient_id
            AND fa2.appointment_date > hp.hygiene_date
            AND fa2.appointment_date > CURRENT_DATE
    )
    OR EXISTS (
        SELECT 1 
        FROM raw_marts.fact_appointment fa3
        WHERE fa3.patient_id = hp.patient_id
            AND fa3.next_appointment_id IS NOT NULL
    )
)
SELECT 
    COALESCE(COUNT(DISTINCT pwna.patient_id)::numeric, 0) 
        / NULLIF(COUNT(DISTINCT hp.patient_id)::numeric, 0) * 100 as hyg_pre_appointment_percent
FROM hygiene_patients hp
LEFT JOIN patients_with_next_appt pwna ON hp.patient_id = pwna.patient_id
```

---

### KPI 3: Hyg Patients Seen (Volume Metric)

**KPI Name**: Hyg Patients Seen  
**KPI Long Name**: Unique Patients Seen in Hygienist(s)

**Description** (PBN Business Logic):
Tracks the number of unique patients who have undergone any hygiene procedure within a given time frame. This statistic is valuable for understanding the reach and workload of the hygienists in your dental office.

**Key Points** (PBN):
- **Unique Counts**: Each patient is counted once, regardless of the number of procedures they undergo
- **Procedure Types**: Includes a variety of hygiene procedures which can be adjusted as per the office's preferences
- **Calculation Method**: 
  1. Identify all hygiene procedures performed in the period
  2. List all patients who underwent these procedures
  3. Remove duplicate entries of any patient to ensure each patient is counted only once
  4. Sum the unique patient entries to get the total number of unique patients seen by the hygienist(s)

**Example** (PBN):
- If 10 patients visited the hygienist and 2 of them visited twice, you would count only 10 unique patients, not 12.

**Calculation**:
```sql
Hyg Patients Seen = COUNT(DISTINCT patient_id) 
WHERE hygiene procedure performed 
    AND procedure_date BETWEEN :start_date AND :end_date
```

**Example**:
- Unique patients seen: 175

**Data Source** (PBN Business Logic):
- **Primary**: `int_procedure_complete` - Hygiene procedures identified by `is_hygiene = true` (PBN emphasizes procedures, not just appointments)
- **Secondary**: `fact_appointment` - Hygiene appointments identified by `is_hygiene_appointment = true` OR `hygienist_id IS NOT NULL` (fallback)
- **Critical**: Each patient is counted ONCE, regardless of number of procedures/appointments

**SQL Logic** (PBN Approach - Procedure-Based):
```sql
SELECT COUNT(DISTINCT patient_id) as hyg_patients_seen
FROM (
    -- Primary: Hygiene procedures
    SELECT DISTINCT pc.patient_id
    FROM raw_intermediate.int_procedure_complete pc
    WHERE pc.is_hygiene = true
        AND pc.procedure_date >= COALESCE(:start_date, CURRENT_DATE - INTERVAL '12 months')
        AND pc.procedure_date <= COALESCE(:end_date, CURRENT_DATE)
    
    UNION
    
    -- Fallback: Hygiene appointments (if procedure data incomplete)
    SELECT DISTINCT fa.patient_id
    FROM raw_marts.fact_appointment fa
    WHERE (fa.is_hygiene_appointment = true 
           OR fa.hygienist_id IS NOT NULL)
        AND fa.appointment_date >= COALESCE(:start_date, CURRENT_DATE - INTERVAL '12 months')
        AND fa.appointment_date <= COALESCE(:end_date, CURRENT_DATE)
        AND NOT EXISTS (
            -- Exclude if already counted via procedures
            SELECT 1 
            FROM raw_intermediate.int_procedure_complete pc2
            WHERE pc2.patient_id = fa.patient_id
                AND pc2.is_hygiene = true
                AND pc2.procedure_date >= COALESCE(:start_date, CURRENT_DATE - INTERVAL '12 months')
                AND pc2.procedure_date <= COALESCE(:end_date, CURRENT_DATE)
        )
) as hygiene_patients
```

---

### KPI 4: Hyg Pts Re-appntd (Volume Metric)

**KPI Name**: Hyg Pts Re-appntd  
**KPI Long Name**: Hygiene Patients Re-appointed

**Description** (PBN Business Logic):
Tracks the number of patients who, after being seen for any hygiene procedure during the selected time range, have scheduled another appointment. It does not matter whether the subsequent appointment is with a doctor or for another hygiene procedure.

**Purpose**: 
- To understand patient retention and the effectiveness of scheduling follow-up appointments
- High pre-appointment rates can indicate good patient compliance and satisfaction with the service

**KPI Calculation** (PBN):
This KPI is calculated by counting the number of unique patients who had a hygiene procedure during the chosen time frame and then established another appointment, regardless of the type of follow-up appointment.

**Calculation Steps** (PBN):
1. **Step 1**: Identify the total number of unique patients seen for any hygiene procedure within the time range
2. **Step 2**: Among these patients, count those who have booked another appointment of any kind after their initial hygiene procedure
3. **Final Calculation**: The count from Step 2 represents the value for Hyg Pre-Appointment Any

**Calculation**:
```sql
Hyg Pts Re-appntd = COUNT(DISTINCT patient_id)
WHERE hygiene procedure performed in time range
    AND patient has scheduled another appointment (any type) after hygiene
```

**Example**:
- Patients re-appointed: 141

**Data Source** (PBN Business Logic):
- **Primary**: `int_procedure_complete` - Hygiene procedures identified by `is_hygiene = true` (PBN emphasizes procedures)
- **Secondary**: `fact_appointment` - Hygiene appointments identified by `is_hygiene_appointment = true` OR `hygienist_id IS NOT NULL`
- **Important**: This is a COUNT (not percentage) of unique patients who scheduled another appointment after hygiene
- Next appointment can be ANY type (doctor or hygiene)

**SQL Logic** (PBN Approach - Procedure-Based):
```sql
WITH hygiene_procedures AS (
    -- Step 1: Identify unique patients who had hygiene procedures in time range
    SELECT DISTINCT
        pc.patient_id,
        pc.procedure_date as hygiene_date
    FROM raw_intermediate.int_procedure_complete pc
    WHERE pc.is_hygiene = true
        AND pc.procedure_date >= COALESCE(:start_date, CURRENT_DATE - INTERVAL '12 months')
        AND pc.procedure_date <= COALESCE(:end_date, CURRENT_DATE)
),
hygiene_patients AS (
    -- Include procedure-based patients, with appointment fallback
    SELECT DISTINCT
        COALESCE(hp.patient_id, fa.patient_id) as patient_id,
        COALESCE(hp.hygiene_date, fa.appointment_date) as hygiene_date
    FROM hygiene_procedures hp
    FULL OUTER JOIN (
        SELECT DISTINCT
            fa.patient_id,
            fa.appointment_date
        FROM raw_marts.fact_appointment fa
        WHERE (fa.is_hygiene_appointment = true 
               OR fa.hygienist_id IS NOT NULL)
            AND fa.appointment_date >= COALESCE(:start_date, CURRENT_DATE - INTERVAL '12 months')
            AND fa.appointment_date <= COALESCE(:end_date, CURRENT_DATE)
    ) fa ON hp.patient_id = fa.patient_id
)
-- Step 2: Count unique patients who have booked another appointment after hygiene
SELECT COUNT(DISTINCT hp.patient_id) as hyg_pts_reappntd
FROM hygiene_patients hp
WHERE EXISTS (
    SELECT 1 
    FROM raw_marts.fact_appointment fa2
    WHERE fa2.patient_id = hp.patient_id
        AND fa2.appointment_date > hp.hygiene_date
        AND fa2.appointment_date > CURRENT_DATE
)
OR EXISTS (
    SELECT 1 
    FROM raw_marts.fact_appointment fa3
    WHERE fa3.patient_id = hp.patient_id
        AND fa3.next_appointment_id IS NOT NULL
)
```

---

### KPI 5: Recall Overdue % (Problem Metric)

**KPI Name**: Recall Overdue %  
**KPI Long Name**: % Active Patients Overdue Recall

**Description**:
Measures the percentage of active patients who are overdue for their recall appointments. This metric helps in identifying how well the dental office is managing its recall program and ensuring that patients are returning for necessary follow-up appointments.

**Objective**: 
- Monitor and improve patient return rates
- Minimize the number of overdue recall appointments
- Identify scheduling, compliance, or communication issues

**Calculation** (PBN Business Logic):
```sql
Recall Overdue % = (Number of Overdue Recall Patients / Total Number of Active Recall Patients) * 100
```

**Important Clarifications**:
- **Denominator**: Total number of **active patients who are ON the recall program** (not all active patients)
- **Numerator**: Number of **active patients who are overdue** for their recall appointments
- Active patients: Visited in past 18 months

**Example**:
- Total active recall patients: 200
- Overdue recall patients: 51
- Recall Overdue % = (51 / 200) * 100 = 25.5%

**Data Sources**:
- `int_recall_management` - `is_overdue` flag, `compliance_status = 'Overdue'`
- `dim_patient` - Active patient status
- `fact_appointment` - Last visit date (to determine active patients)

**SQL Logic** (PBN Approach):
```sql
WITH active_patients AS (
    -- Active patients: visited in past 18 months
    SELECT DISTINCT p.patient_id
    FROM raw_marts.dim_patient p
    INNER JOIN raw_marts.fact_appointment fa ON p.patient_id = fa.patient_id
    WHERE fa.appointment_date >= CURRENT_DATE - INTERVAL '18 months'
        AND p.patient_status IN ('Patient', 'Active')
),
active_recall_patients AS (
    -- Active patients who are ON the recall program
    SELECT DISTINCT ap.patient_id
    FROM active_patients ap
    INNER JOIN raw_intermediate.int_recall_management irm ON ap.patient_id = irm.patient_id
    WHERE irm.is_disabled = false
        AND irm.is_valid_recall = true
),
overdue_recall_patients AS (
    -- Active recall patients who are overdue
    SELECT DISTINCT arp.patient_id
    FROM active_recall_patients arp
    INNER JOIN raw_intermediate.int_recall_management irm ON arp.patient_id = irm.patient_id
    WHERE irm.is_overdue = true
        AND irm.is_disabled = false
        AND irm.is_valid_recall = true
)
SELECT 
    COALESCE(COUNT(DISTINCT orp.patient_id)::numeric, 0) 
        / NULLIF(COUNT(DISTINCT arp.patient_id)::numeric, 0) * 100 as recall_overdue_percent
FROM active_recall_patients arp
LEFT JOIN overdue_recall_patients orp ON arp.patient_id = orp.patient_id
```

---

### KPI 6: Not on Recall % (Problem Metric)

**KPI Name**: Not on Recall %  
**KPI Long Name**: Not on Recall %

**Description**:
Measures the percentage of patients who are not currently enrolled in any recall programs. Recall programs typically include regular reminders for dental check-ups, cleanings, or other routine dental care services. This KPI is crucial for understanding how many patients might be missing out on necessary follow-up care.

**Importance**: 
- Identifies patients who may not be receiving regular dental care
- Allows for targeted follow-up to improve patient health outcomes
- Should be monitored monthly for timely interventions

**Calculation** (PBN Business Logic):
```sql
Not on Recall % = (Number of Patients Not on Recall Program / Total Number of Patients) * 100
```

**Important Clarifications**:
- **Denominator**: **Total count of patients** in the dental office's database (not just active patients)
- **Numerator**: Count of patients who are **not enrolled in any recall programs**
- This KPI measures enrollment in recall programs across the entire patient base

**Example**:
- Total patients: 150
- Patients not on recall: 30
- Not on Recall % = (30 / 150) * 100 = 20%

**Data Sources**:
- `dim_patient` - All patients in the database
- `int_recall_management` - Patients with active recalls

**SQL Logic** (PBN Approach):
```sql
WITH all_patients AS (
    -- Total count of patients in the database
    SELECT DISTINCT p.patient_id
    FROM raw_marts.dim_patient p
    WHERE p.patient_status IN ('Patient', 'Active', 'Inactive')  -- Include all non-deleted patients
),
patients_with_recall AS (
    -- Patients who are enrolled in recall programs
    SELECT DISTINCT patient_id
    FROM raw_intermediate.int_recall_management
    WHERE is_disabled = false
        AND is_valid_recall = true
)
SELECT 
    COALESCE(COUNT(DISTINCT CASE WHEN pwr.patient_id IS NULL THEN ap.patient_id END)::numeric, 0)
        / NULLIF(COUNT(DISTINCT ap.patient_id)::numeric, 0) * 100 as not_on_recall_percent
FROM all_patients ap
LEFT JOIN patients_with_recall pwr ON ap.patient_id = pwr.patient_id
```

---

## Technical Implementation

### Backend Structure

#### 1. Service Layer: `api/services/hygiene_service.py` (NEW FILE)

**Function**: `get_hygiene_retention_summary()`

**Signature**:
```python
def get_hygiene_retention_summary(
    db: Session,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None
) -> dict:
```

**Returns**:
```python
{
    "recall_current_percent": 53.3,
    "hyg_pre_appointment_percent": 80.6,
    "hyg_patients_seen": 175,
    "hyg_pts_reappntd": 141,
    "recall_overdue_percent": 25.6,
    "not_on_recall_percent": 20.0
}
```

**Implementation Notes**:
- Combine all 6 KPI calculations into a single optimized query
- Use CTEs for readability and performance
- Handle NULL values gracefully (COALESCE to 0)
- Default date range: Last 12 months if not specified

#### 2. Pydantic Models: `api/models/hygiene.py` (NEW FILE)

**Model**: `HygieneRetentionSummary`

```python
class HygieneRetentionSummary(BaseModel):
    recall_current_percent: float  # Recall Current %
    hyg_pre_appointment_percent: float  # Hyg Pre-Appointment Any %
    hyg_patients_seen: int  # Hyg Patients Seen
    hyg_pts_reappntd: int  # Hyg Pts Re-appntd
    recall_overdue_percent: float  # Recall Overdue %
    not_on_recall_percent: float  # Not on Recall %
    
    class Config:
        from_attributes = True
```

#### 3. API Router: `api/routers/hygiene.py` (NEW FILE)

**Endpoint**: `GET /hygiene/retention-summary`

**Query Parameters**:
- `start_date` (optional): Start date for analysis (ISO format: YYYY-MM-DD)
- `end_date` (optional): End date for analysis (ISO format: YYYY-MM-DD)

**Response**: `HygieneRetentionSummary` model

**Error Handling**:
- Try-except blocks with logging
- HTTPException for database errors
- Return default values (0) on error

---

### Frontend Structure

#### 1. TypeScript Interfaces: `frontend/src/types/api.ts`

**Add to existing file**:
```typescript
export interface HygieneRetentionSummary {
    recall_current_percent: number;  // Recall Current %
    hyg_pre_appointment_percent: number;  // Hyg Pre-Appointment Any %
    hyg_patients_seen: number;  // Hyg Patients Seen
    hyg_pts_reappntd: number;  // Hyg Pts Re-appntd
    recall_overdue_percent: number;  // Recall Overdue %
    not_on_recall_percent: number;  // Not on Recall %
}
```

#### 2. API Service: `frontend/src/services/api.ts`

**Add to existing file**:
```typescript
export const hygieneApi = {
    getRetentionSummary: async (
        params: DateRange = {}
    ): Promise<ApiResponse<HygieneRetentionSummary>> => {
        return apiCall(() => api.get('/hygiene/retention-summary', { params }));
    },
};
```

#### 3. Page Component: `frontend/src/pages/HygieneRetention.tsx` (NEW FILE)

**Layout Structure** (following PBN design):

```
┌─────────────────────────────────────────────────────────┐
│ Hygiene Retention                    [Grid] [People] [...] │
├─────────────────────────────────────────────────────────┤
│                                                           │
│  ┌────────────────────┐    ┌────────────────────┐       │
│  │ Recall Current     │    │ Hyg Pre-appmt      │       │
│  │                    │    │                    │       │
│  │     53.3%          │    │     80.6%          │       │
│  │                    │    │                    │       │
│  │  [Progress Bar]    │    │  [Progress Bar]    │       │
│  └────────────────────┘    └────────────────────┘       │
│                                                           │
├─────────────────────────────────────────────────────────┤
│                                                           │
│  Metrics Table:                                          │
│  ┌─────────────────────────────────────────────┐        │
│  │ Hyg Pts Seen          │ 175                  │        │
│  ├─────────────────────────────────────────────┤        │
│  │ Hyg Pts Re-appntd     │ 141                  │        │
│  ├─────────────────────────────────────────────┤        │
│  │ Recall Overdue        │ 25.6%                │        │
│  ├─────────────────────────────────────────────┤        │
│  │ Not on Recall         │ 20%                  │        │
│  └─────────────────────────────────────────────┘        │
│                                                           │
└─────────────────────────────────────────────────────────┘
```

**Component Structure**:
- Main component: `HygieneRetention.tsx`
- Reusable components:
  - `KPICard.tsx` - Card with large number + progress bar
  - `MetricsTable.tsx` - Two-column metrics table

#### 4. Reusable Components

**KPICard Component** (`frontend/src/components/KPICard.tsx`):
```typescript
interface KPICardProps {
    title: string;
    value: number;
    isPercentage?: boolean;
    color?: string;
}

// Displays:
// - Title (gray text, left)
// - Large value (dark blue, bold)
// - Horizontal progress bar (teal fill, gray background)
```

**MetricsTable Component** (`frontend/src/components/MetricsTable.tsx`):
```typescript
interface MetricsTableProps {
    metrics: Array<{
        label: string;
        value: number | string;
        isPercentage?: boolean;
    }>;
}

// Displays:
// - Two-column layout (label | value)
// - Thin gray separators between rows
// - Right-aligned values
```

#### 5. Routing: `frontend/src/App.tsx` or router file

**Add route**:
```typescript
<Route path="/hygiene-retention" element={<HygieneRetention />} />
```

---

## Design Specifications

### Visual Design (Following PBN Patterns)

**Color Palette**:
- Primary metric text: `#1a237e` (dark blue)
- Progress bar fill: `#26a69a` (teal/turquoise)
- Progress bar background: `#e0e0e0` (light gray)
- Label text: `#757575` (gray)
- Separator lines: `#e0e0e0` (light gray)
- Card background: `#ffffff` (white)
- Page background: `#f5f5f5` (light gray)

**Typography**:
- Title: Bold, 24px, dark gray
- KPI values: Bold, 48px, dark blue
- Labels: Regular, 14px, gray
- Table values: Regular, 16px, dark blue

**Layout**:
- Card: White background, rounded corners (8px), shadow
- Padding: 24px inside cards
- Spacing: 16px between sections
- Progress bars: Height 8px, rounded corners (4px)

**Responsive Design**:
- Desktop: Two KPI cards side-by-side
- Tablet: Two KPI cards stacked
- Mobile: Single column, full width

---

## Data Quality Considerations

### Edge Cases to Handle

1. **No Active Patients**:
   - Return 0% for all percentage metrics
   - Return 0 for all count metrics
   - Avoid division by zero errors

2. **Missing Recall Data**:
   - Some patients may not have recall records
   - Treat as "Not on Recall" for that metric
   - Don't exclude from "Active Patients" count

3. **Future Appointments**:
   - `next_appointment_id` may point to past appointments
   - Check `appointment_date > CURRENT_DATE` for future appointments
   - Use `EXISTS` subquery for more reliable tracking

4. **Date Range Filters**:
   - Default to last 12 months if not specified
   - Validate date ranges (start_date <= end_date)
   - Handle timezone issues (use DATE type, not TIMESTAMP)

5. **Patient Status**:
   - Only count active patients (exclude archived, deleted, inactive)
   - Use `patient_status IN ('Patient', 'Active')` filter
   - Verify with business rules

---

## Testing Strategy

### Backend Testing

1. **Unit Tests** (if test framework available):
   - Test with empty database (should return 0s)
   - Test with sample data
   - Test date range filtering
   - Test NULL handling

2. **Manual Testing**:
   - Query database directly to verify calculations
   - Compare results with manual calculations
   - Test with different date ranges
   - Test edge cases (no data, all overdue, etc.)

### Frontend Testing

1. **Component Testing**:
   - Test KPI card rendering
   - Test progress bar calculation (percentage to width)
   - Test metrics table formatting
   - Test loading states
   - Test error states

2. **Integration Testing**:
   - Test API call and data flow
   - Test date range filtering
   - Test responsive design
   - Test with real data

---

## Implementation Checklist

### Backend Tasks

- [ ] Create `api/services/hygiene_service.py`
  - [ ] Implement `get_hygiene_retention_summary()` function
  - [ ] Write SQL query for all 6 KPIs
  - [ ] Add error handling and logging
  - [ ] Test with real data

- [ ] Create `api/models/hygiene.py`
  - [ ] Define `HygieneRetentionSummary` Pydantic model
  - [ ] Add validation rules
  - [ ] Add documentation

- [ ] Create `api/routers/hygiene.py`
  - [ ] Define `/hygiene/retention-summary` endpoint
  - [ ] Add query parameter validation
  - [ ] Add error handling
  - [ ] Register router in `main.py`

### Frontend Tasks

- [ ] Update `frontend/src/types/api.ts`
  - [ ] Add `HygieneRetentionSummary` interface

- [ ] Update `frontend/src/services/api.ts`
  - [ ] Add `hygieneApi.getRetentionSummary()` method

- [ ] Create `frontend/src/components/KPICard.tsx`
  - [ ] Implement card with title, value, progress bar
  - [ ] Add percentage formatting
  - [ ] Add styling (PBN design)

- [ ] Create `frontend/src/components/MetricsTable.tsx`
  - [ ] Implement two-column table
  - [ ] Add separator lines
  - [ ] Add value formatting (numbers, percentages)

- [ ] Create `frontend/src/pages/HygieneRetention.tsx`
  - [ ] Implement main page layout
  - [ ] Add API integration
  - [ ] Add loading states
  - [ ] Add error handling
  - [ ] Add date range filters (optional)

- [ ] Update routing
  - [ ] Add route to `App.tsx` or router config
  - [ ] Add navigation link (if applicable)

---

## Open Questions

1. **Date Range Default**:
   - Should default be last 30 days, 90 days, or 12 months?
   - **Recommendation**: Last 12 months (matches "active patients" definition)
   - **PBN Note**: Time range is specified for hygiene procedure analysis

2. **Active Patients Definition**:
   - Confirm 18-month lookback for active patients?
   - **Recommendation**: Yes, matches PBN definition (patients visited in past 18 months)

3. **Hygiene Procedure Identification**:
   - Use `fact_appointment.is_hygiene_appointment` flag, `hygienist_id`, or `int_procedure_complete.is_hygiene`?
   - **Recommendation**: Use combination - `is_hygiene_appointment = true` OR `hygienist_id IS NOT NULL` OR check procedures with `is_hygiene = true` for maximum coverage

4. **Pre-Appointment Tracking**:
   - Use `next_appointment_id` field, or check for any future appointment?
   - **Recommendation**: Check for any future appointment (any type) after hygiene date (more reliable per PBN logic)

5. **Recall Current Definition**:
   - What exactly defines "current on recall" - future due date, scheduled appointment, or completed appointment?
   - **Recommendation**: Use `int_recall_management.is_overdue = false` AND `is_disabled = false` AND `is_valid_recall = true` as primary indicator

6. **Not on Recall Denominator**:
   - Should denominator be ALL patients or just active patients?
   - **PBN Clarification**: ALL patients in database (confirmed in PBN definition: "Total Number of Patients: The total count of patients in the dental office's database")

7. **Additional Metrics**:
   - Include production metrics (production per hour, per patient)?
   - **Recommendation**: Keep simple for MVP, add later if needed

8. **Date Range Filters**:
   - Should frontend have date range selector?
   - **Recommendation**: Yes, add as optional enhancement

9. **Hygienist Breakdown**:
   - Show per-hygienist performance?
   - **Recommendation**: Add in Phase 2 (after MVP)

---

## Success Criteria

### MVP (Minimum Viable Product)

✅ All 6 KPIs calculated correctly  
✅ Dashboard displays all metrics  
✅ Matches PBN visual design  
✅ Responsive layout  
✅ Error handling in place  
✅ Loading states implemented  

### Phase 2 Enhancements (Future)

- Date range selector
- Per-hygienist breakdown table
- Trend charts (30/60/90 day views)
- Production metrics
- Export to CSV
- Drill-down to patient list

---

## Dependencies

### Existing Resources (Available)

- ✅ `mart_hygiene_retention` - Hygiene appointment data
- ✅ `int_recall_management` - Recall compliance data
- ✅ `fact_appointment` - Appointment data with `next_appointment_id`
- ✅ `dim_patient` - Patient demographics and status
- ✅ FastAPI backend infrastructure
- ✅ React + TypeScript frontend infrastructure
- ✅ Material-UI component library

### No New Dependencies Required

All required data models and infrastructure already exist.

---

## Timeline Estimate

**Total Effort**: 6-8 hours

**Breakdown**:
- Backend development: 3-4 hours
  - Service function: 2 hours
  - Models and router: 0.5 hours
  - Testing and debugging: 1-1.5 hours
- Frontend development: 3-4 hours
  - Components: 1.5 hours
  - Page integration: 1 hour
  - Styling and polish: 0.5-1 hour

**Recommended Approach**:
1. Build backend first (test with Postman/curl)
2. Build frontend components
3. Integrate and test
4. Polish and refine

---

## Next Steps

1. **Review this plan** with stakeholders
2. **Clarify open questions** (date ranges, definitions)
3. **Start with backend** implementation
4. **Test backend** with real data
5. **Build frontend** components
6. **Integrate and test** end-to-end
7. **Deploy and validate** with users

---

**Document Version**: 1.0  
**Created**: 2025-01-XX  
**Status**: Planning - Ready for Review  
**Owner**: Development Team

