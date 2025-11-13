{{
    config(
        materialized='table',
        schema='marts',
        unique_key=['procedure_date', 'provider_id', 'clinic_id'],
        on_schema_change='fail',
        indexes=[
            {'columns': ['procedure_date', 'provider_id', 'clinic_id']},
            {'columns': ['procedure_date']},
            {'columns': ['provider_id']},
            {'columns': ['clinic_id']}
        ]
    )
}}

/*
    Mart model for procedure-based treatment acceptance summary
    Part of System A: Fee Processing & Verification
    
    This model:
    1. Provides daily grain procedure acceptance metrics by provider and clinic
    2. Aggregates procedure-level acceptance data from int_procedure_acceptance
    3. Calculates key performance indicators (KPIs) for treatment acceptance
    4. Integrates with appointment data for patients_seen metric
    5. Supports dashboard visualizations for treatment acceptance analysis
    
    Business Logic:
    - Presented: Procedures with status 1 (Treatment Planned) OR status 6 (Ordered/Planned)
    - Accepted: Procedures with status 2 (Completed) OR status 1/6 with appointment scheduled
    - Same-Day Treatment: Procedures completed on the same day they were presented
    - Patients Seen: Unique patients with appointments (scheduled or completed) in date range
    - Patients with Exams: Unique patients with completed exam procedures in date range
    
    Key Metrics:
    - tx_acceptance_rate: (Tx Accepted / Tx Presented) * 100
    - patient_acceptance_rate: (Patients Accepted / Patients Presented) * 100
    - diagnosis_rate: (Patients with Exams AND Presented / Patients with Exams) * 100 (PBN definition)
    - same_day_treatment_rate: (Same-Day Treatment Amount / Tx Presented) * 100
    
    Data Sources:
    - int_procedure_acceptance: Procedure-level acceptance data
    - int_appointment_details: Appointment data for patients_seen metric
    
    Performance Considerations:
    - Table materialization for optimal query performance
    - Indexed on procedure_date, provider_id, and clinic_id for fast filtering
    - Daily grain allows for flexible aggregation (weekly, monthly) in dashboard
*/

-- 1. Aggregate procedure acceptance metrics by date + provider + clinic
WITH procedure_acceptance_daily AS (
    SELECT
        DATE(pa.procedure_date) AS procedure_date,
        COALESCE(pa.provider_id, 0) AS provider_id,
        COALESCE(pa.clinic_id, 0) AS clinic_id,
        
        -- Volume metrics
        -- Patients presented: Distinct patients with status 1/6 procedures
        COUNT(DISTINCT CASE WHEN pa.is_presented THEN pa.patient_id END) AS patients_presented,
        -- Patients accepted: Distinct patients with status 2 procedures OR status 1/6 with appointment
        COUNT(DISTINCT CASE WHEN pa.is_accepted THEN pa.patient_id END) AS patients_accepted,
        -- Procedures presented: Only status 1/6 (actually presented to patient)
        COUNT(DISTINCT CASE WHEN pa.is_presented THEN pa.procedure_id END) AS procedures_presented,
        -- Procedures accepted: Status 2 (completed) OR Status 1/6 with appointment scheduled
        -- Note: Status 2 procedures are always accepted, but may not have been "presented" on this date
        COUNT(DISTINCT CASE WHEN pa.is_accepted THEN pa.procedure_id END) AS procedures_accepted,
        
        -- Exam-specific metrics
        COUNT(DISTINCT CASE WHEN pa.is_exam_procedure AND pa.is_presented THEN pa.patient_id END) AS patients_with_exams_presented,
        COUNT(DISTINCT CASE WHEN pa.is_exam_procedure AND pa.is_accepted THEN pa.patient_id END) AS patients_with_exams_accepted,
        COUNT(DISTINCT CASE WHEN pa.is_exam_procedure AND pa.procedure_status = 2 THEN pa.patient_id END) AS patients_with_exams_completed,
        
        -- Financial metrics
        -- Tx presented: Only status 1/6 (actually presented to patient)
        SUM(CASE WHEN pa.is_presented THEN pa.procedure_fee ELSE 0 END) AS tx_presented_amount,
        -- Tx accepted: Status 2 (completed) OR Status 1/6 with appointment scheduled
        -- Note: Status 2 procedures contribute to accepted amount even if not "presented" on this date
        SUM(CASE WHEN pa.is_accepted THEN pa.procedure_fee ELSE 0 END) AS tx_accepted_amount,
        SUM(CASE WHEN pa.is_same_day_treatment THEN pa.procedure_fee ELSE 0 END) AS same_day_treatment_amount,
        
        -- Procedure status breakdown
        COUNT(DISTINCT CASE WHEN pa.procedure_status = 1 AND pa.is_presented THEN pa.procedure_id END) AS procedures_planned,
        COUNT(DISTINCT CASE WHEN pa.procedure_status = 6 AND pa.is_presented THEN pa.procedure_id END) AS procedures_ordered,
        COUNT(DISTINCT CASE WHEN pa.procedure_status = 2 THEN pa.procedure_id END) AS procedures_completed,
        COUNT(DISTINCT CASE WHEN pa.procedure_status IN (1, 6) AND pa.is_accepted AND pa.appointment_id IS NOT NULL THEN pa.procedure_id END) AS procedures_scheduled
        
    FROM {{ ref('int_procedure_acceptance') }} pa
    WHERE pa.procedure_date >= '2020-01-01'  -- Filter for reasonable date range
        AND pa.procedure_date <= CURRENT_DATE + INTERVAL '90 days'  -- Filter out far future dates
    GROUP BY 
        DATE(pa.procedure_date),
        COALESCE(pa.provider_id, 0),
        COALESCE(pa.clinic_id, 0)
),

-- 2. Patients seen (unique patients with appointments in date range)
patients_seen_daily AS (
    SELECT
        DATE(a.appointment_datetime) AS appointment_date,
        COALESCE(a.provider_id, 0) AS provider_id,
        COALESCE(a.clinic_id, 0) AS clinic_id,
        COUNT(DISTINCT a.patient_id) AS patients_seen
    FROM {{ ref('int_appointment_details') }} a
    WHERE a.appointment_status IN (1, 2)  -- 1 = Scheduled, 2 = Complete
        AND a.appointment_datetime IS NOT NULL
    GROUP BY 
        DATE(a.appointment_datetime),
        COALESCE(a.provider_id, 0),
        COALESCE(a.clinic_id, 0)
),

-- 3. Patients with exams (unique patients with completed exam procedures in date range)
patients_with_exams_daily AS (
    SELECT
        DATE(pa.procedure_date) AS procedure_date,
        COALESCE(pa.provider_id, 0) AS provider_id,
        COALESCE(pa.clinic_id, 0) AS clinic_id,
        COUNT(DISTINCT pa.patient_id) AS patients_with_exams
    FROM {{ ref('int_procedure_acceptance') }} pa
    WHERE pa.is_exam_procedure = TRUE
        AND pa.procedure_status = 2  -- Completed
        AND pa.procedure_date >= '2020-01-01'
        AND pa.procedure_date <= CURRENT_DATE + INTERVAL '90 days'  -- Filter out far future dates
    GROUP BY 
        DATE(pa.procedure_date),
        COALESCE(pa.provider_id, 0),
        COALESCE(pa.clinic_id, 0)
),

-- 4. Patients with exams who were also presented with treatment (PBN Diag % calculation)
-- PBN Definition: (Patients with Treatment Plans / Patients Receiving Exams) * 100
-- For procedure-based approach: (Patients with Exams AND Presented / Patients with Exams) * 100
-- Exam must be within 30 days before treatment presentation (matching PBN requirement)
-- Note: This is grouped by exam date, so each patient with an exam is counted on their exam date
-- if they were presented with treatment within 30 days after the exam
patients_with_exams_and_presented_daily AS (
    SELECT
        DATE(exam.procedure_date) AS procedure_date,
        COALESCE(exam.provider_id, 0) AS provider_id,
        COALESCE(exam.clinic_id, 0) AS clinic_id,
        COUNT(DISTINCT exam.patient_id) AS patients_with_exams_and_presented
    FROM {{ ref('int_procedure_acceptance') }} exam
    WHERE exam.is_exam_procedure = TRUE
        AND exam.procedure_status = 2  -- Completed exam
        AND exam.procedure_date >= '2020-01-01'
        AND exam.procedure_date <= CURRENT_DATE + INTERVAL '90 days'
        -- Check if this patient was presented with treatment within 30 days after the exam
        AND EXISTS (
            SELECT 1
            FROM {{ ref('int_procedure_acceptance') }} presented
            WHERE presented.patient_id = exam.patient_id
                -- Match by provider (exam provider should match presentation provider for PBN)
                AND presented.provider_id = exam.provider_id
                -- Match by clinic (exam clinic should match presentation clinic for PBN)
                AND presented.clinic_id = exam.clinic_id
                -- Presented procedure must be within 30 days after exam (matching PBN: "exam within 30 days of treatment plan creation")
                AND DATE(presented.procedure_date) >= DATE(exam.procedure_date)
                AND DATE(presented.procedure_date) <= DATE(exam.procedure_date) + INTERVAL '30 days'
                AND presented.is_presented = TRUE  -- Presented with treatment (status 1/6)
                AND presented.procedure_date >= '2020-01-01'
                AND presented.procedure_date <= CURRENT_DATE + INTERVAL '90 days'
        )
    GROUP BY 
        DATE(exam.procedure_date),
        COALESCE(exam.provider_id, 0),
        COALESCE(exam.clinic_id, 0)
),

-- 5. Combine all metrics
final AS (
    SELECT
        COALESCE(pad.procedure_date, psd.appointment_date, pwed.procedure_date) AS procedure_date,
        COALESCE(pad.provider_id, psd.provider_id, pwed.provider_id) AS provider_id,
        COALESCE(pad.clinic_id, psd.clinic_id, pwed.clinic_id) AS clinic_id,
        
        -- Volume metrics
        COALESCE(psd.patients_seen, 0) AS patients_seen,
        COALESCE(pwed.patients_with_exams, 0) AS patients_with_exams,
        COALESCE(pweapd.patients_with_exams_and_presented, 0) AS patients_with_exams_and_presented,
        COALESCE(pad.patients_presented, 0) AS patients_presented,
        COALESCE(pad.patients_accepted, 0) AS patients_accepted,
        COALESCE(pad.procedures_presented, 0) AS procedures_presented,
        COALESCE(pad.procedures_accepted, 0) AS procedures_accepted,
        
        -- Exam-specific metrics
        COALESCE(pad.patients_with_exams_presented, 0) AS patients_with_exams_presented,
        COALESCE(pad.patients_with_exams_accepted, 0) AS patients_with_exams_accepted,
        COALESCE(pad.patients_with_exams_completed, 0) AS patients_with_exams_completed,
        
        -- Financial metrics
        COALESCE(pad.tx_presented_amount, 0) AS tx_presented_amount,
        COALESCE(pad.tx_accepted_amount, 0) AS tx_accepted_amount,
        COALESCE(pad.same_day_treatment_amount, 0) AS same_day_treatment_amount,
        
        -- Procedure status breakdown
        COALESCE(pad.procedures_planned, 0) AS procedures_planned,
        COALESCE(pad.procedures_ordered, 0) AS procedures_ordered,
        COALESCE(pad.procedures_completed, 0) AS procedures_completed,
        COALESCE(pad.procedures_scheduled, 0) AS procedures_scheduled,
        
        -- Percentage metrics
        -- Tx Acceptance Rate: (Tx Accepted / Tx Presented) * 100
        -- Note: Can exceed 100% when status 2 procedures (completed) exist without corresponding status 1/6 procedures
        -- This happens when procedures are completed on the same day they're created, or when procedures were created/presented on a different date
        CASE
            WHEN COALESCE(pad.tx_presented_amount, 0) > 0
            THEN ROUND((COALESCE(pad.tx_accepted_amount, 0)::numeric / NULLIF(pad.tx_presented_amount, 0) * 100)::numeric, 2)
            WHEN COALESCE(pad.tx_accepted_amount, 0) > 0
            THEN NULL  -- Cannot calculate rate when no tx presented but tx accepted exists
            ELSE 0
        END AS tx_acceptance_rate,
        
        -- Patient Acceptance Rate: (Patients Accepted / Patients Presented) * 100
        -- Note: Can exceed 100% when patients have status 2 procedures (completed) without corresponding status 1/6 procedures
        -- This happens when procedures are completed on the same day they're created, or when procedures were created/presented on a different date
        CASE
            WHEN COALESCE(pad.patients_presented, 0) > 0
            THEN ROUND((COALESCE(pad.patients_accepted, 0)::numeric / NULLIF(pad.patients_presented, 0) * 100)::numeric, 2)
            WHEN COALESCE(pad.patients_accepted, 0) > 0
            THEN NULL  -- Cannot calculate rate when no patients presented but patients accepted exist
            ELSE 0
        END AS patient_acceptance_rate,
        
        -- Diagnosis Rate: (Patients with Exams AND Presented / Patients with Exams) * 100
        -- PBN Definition: (Patients with Treatment Plans / Patients Receiving Exams) * 100
        -- For procedure-based approach: (Patients with Exams AND Presented / Patients with Exams) * 100
        -- Exam must be within 30 days before treatment presentation (matching PBN requirement)
        -- Note: This is more accurate than (Patients Presented / Patients Seen) because it uses exams as denominator
        CASE
            WHEN COALESCE(pwed.patients_with_exams, 0) > 0
            THEN ROUND((COALESCE(pweapd.patients_with_exams_and_presented, 0)::numeric / NULLIF(pwed.patients_with_exams, 0) * 100)::numeric, 2)
            ELSE NULL  -- Cannot calculate diagnosis rate when no patients with exams
        END AS diagnosis_rate,
        
        -- Same-Day Treatment Rate: (Same-Day Treatment Amount / Tx Presented) * 100
        -- Note: Can exceed 100% when same-day treatment exists but no procedures were "presented" on that date
        -- This can happen when procedures are completed on the same day they're created without going through status 1/6
        CASE
            WHEN COALESCE(pad.tx_presented_amount, 0) > 0
            THEN ROUND((COALESCE(pad.same_day_treatment_amount, 0)::numeric / NULLIF(pad.tx_presented_amount, 0) * 100)::numeric, 2)
            WHEN COALESCE(pad.same_day_treatment_amount, 0) > 0
            THEN NULL  -- Cannot calculate rate when no tx presented but same-day treatment exists
            ELSE 0
        END AS same_day_treatment_rate,
        
        -- Procedure Acceptance Rate: (Procedures Accepted / Procedures Presented) * 100
        -- Note: Can exceed 100% when status 2 procedures (completed) exist without corresponding status 1/6 procedures
        -- This happens when procedures are completed on the same day they're created, or when procedures were created/presented on a different date
        CASE
            WHEN COALESCE(pad.procedures_presented, 0) > 0
            THEN ROUND((COALESCE(pad.procedures_accepted, 0)::numeric / NULLIF(pad.procedures_presented, 0) * 100)::numeric, 2)
            WHEN COALESCE(pad.procedures_accepted, 0) > 0
            THEN NULL  -- Cannot calculate rate when no procedures presented but procedures accepted exist
            ELSE 0
        END AS procedure_acceptance_rate,
        
        -- Standardized mart metadata
        {{ standardize_mart_metadata(preserve_source_metadata=false) }}
        
    FROM procedure_acceptance_daily pad
    FULL OUTER JOIN patients_seen_daily psd
        ON pad.procedure_date = psd.appointment_date
        AND pad.provider_id = psd.provider_id
        AND pad.clinic_id = psd.clinic_id
    FULL OUTER JOIN patients_with_exams_daily pwed
        ON COALESCE(pad.procedure_date, psd.appointment_date) = pwed.procedure_date
        AND COALESCE(pad.provider_id, psd.provider_id) = pwed.provider_id
        AND COALESCE(pad.clinic_id, psd.clinic_id) = pwed.clinic_id
    LEFT JOIN patients_with_exams_and_presented_daily pweapd
        ON COALESCE(pad.procedure_date, psd.appointment_date, pwed.procedure_date) = pweapd.procedure_date
        AND COALESCE(pad.provider_id, psd.provider_id, pwed.provider_id) = pweapd.provider_id
        AND COALESCE(pad.clinic_id, psd.clinic_id, pwed.clinic_id) = pweapd.clinic_id
)

-- Final select with DISTINCT to ensure uniqueness (in case joins create duplicates)
SELECT DISTINCT ON (procedure_date, provider_id, clinic_id)
    *
FROM final
WHERE procedure_date IS NOT NULL
    AND procedure_date <= CURRENT_DATE + INTERVAL '90 days'  -- Filter out far future dates
ORDER BY procedure_date DESC, provider_id, clinic_id, 
    -- Prefer rows with more data (non-zero values)
    CASE WHEN patients_seen > 0 THEN 1 ELSE 2 END,
    CASE WHEN patients_presented > 0 THEN 1 ELSE 2 END

