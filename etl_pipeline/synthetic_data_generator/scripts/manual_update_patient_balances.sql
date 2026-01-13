-- Manual Update of Patient Balances
-- Run this in DBeaver to update patient.BalTotal for existing data
-- This is a temporary fix until you regenerate data with the updated generator

-- Step 1: Calculate balances from procedures
WITH procedure_balances AS (
    SELECT 
        pl."PatNum",
        pl."ProcNum",
        pl."ProcFee" as fee,
        COALESCE(SUM(ps."SplitAmt") FILTER (
            WHERE ps."ProcNum" = pl."ProcNum" 
            AND EXISTS (
                SELECT 1 FROM raw.payment p 
                WHERE p."PayNum" = ps."PayNum" 
                AND p."PayType" IN (71, 69, 70)  -- Patient payment types
            )
        ), 0) as patient_paid,
        COALESCE(SUM(cp."InsPayAmt") FILTER (
            WHERE cp."ProcNum" = pl."ProcNum"
        ), 0) as insurance_paid,
        COALESCE(SUM(a."AdjAmt") FILTER (
            WHERE a."ProcNum" = pl."ProcNum"
        ), 0) as adjustments,
        pl."ProcFee" - 
            COALESCE(SUM(ps."SplitAmt") FILTER (
                WHERE ps."ProcNum" = pl."ProcNum" 
                AND EXISTS (
                    SELECT 1 FROM raw.payment p 
                    WHERE p."PayNum" = ps."PayNum" 
                    AND p."PayType" IN (71, 69, 70)
                )
            ), 0) - 
            COALESCE(SUM(cp."InsPayAmt") FILTER (
                WHERE cp."ProcNum" = pl."ProcNum"
            ), 0) - 
            COALESCE(SUM(a."AdjAmt") FILTER (
                WHERE a."ProcNum" = pl."ProcNum"
            ), 0) as calculated_balance
    FROM raw.procedurelog pl
    LEFT JOIN raw.paysplit ps ON pl."ProcNum" = ps."ProcNum"
    LEFT JOIN raw.claimproc cp ON pl."ProcNum" = cp."ProcNum"
    LEFT JOIN raw.adjustment a ON pl."ProcNum" = a."ProcNum"
    WHERE pl."ProcStatus" IN (2, 8)  -- Completed or In Progress
    GROUP BY pl."ProcNum", pl."PatNum", pl."ProcFee"
),
patient_totals AS (
    SELECT 
        "PatNum",
        SUM(calculated_balance) FILTER (WHERE calculated_balance > 0.01) as total_balance
    FROM procedure_balances
    GROUP BY "PatNum"
)
-- Step 2: Update patient table
UPDATE raw.patient p
SET 
    "BalTotal" = COALESCE(pt.total_balance, 0),
    "EstBalance" = COALESCE(pt.total_balance, 0)
FROM patient_totals pt
WHERE p."PatNum" = pt."PatNum";

-- Verify the update
SELECT 
    COUNT(*) as total_patients,
    COUNT(*) FILTER (WHERE "BalTotal" > 0) as patients_with_balance,
    SUM("BalTotal") FILTER (WHERE "BalTotal" > 0) as total_ar_balance
FROM raw.patient;
