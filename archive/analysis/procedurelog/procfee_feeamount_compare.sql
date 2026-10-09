-- Simpler alternative approach without requiring standard deviation
WITH procedure_fees AS (
    SELECT 
        procedure_id,
        code_id,
        procedure_fee,
        procedure_date,
        procedure_status,
        patient_id,
        provider_id,
        clinic_id
    FROM staging.stg_opendental__procedurelog
    WHERE procedure_fee IS NOT NULL
    AND code_id IS NOT NULL
),

fee_amounts AS (
    SELECT 
        procedure_code_id,
        fee_amount
    FROM staging.stg_opendental__fee
    WHERE fee_amount IS NOT NULL
),

fee_comparisons AS (
    SELECT
        pf.procedure_id,
        pf.code_id,
        pf.procedure_fee,
        pf.procedure_date,
        pf.procedure_status,
        fa.fee_amount,
        -- Calculate the difference and percentage difference
        (pf.procedure_fee - fa.fee_amount) AS fee_difference,
        CASE 
            WHEN fa.fee_amount = 0 THEN NULL
            ELSE ((pf.procedure_fee - fa.fee_amount) / fa.fee_amount) * 100 
        END AS percentage_difference
    FROM procedure_fees pf
    LEFT JOIN fee_amounts fa ON pf.code_id = fa.procedure_code_id
)

SELECT 
    code_id,
    COUNT(*) AS procedure_count,
    AVG(procedure_fee) AS avg_procedure_fee,
    AVG(fee_amount) AS avg_standard_fee,
    AVG(fee_difference) AS avg_fee_difference,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY fee_difference) AS median_fee_difference,
    MIN(fee_difference) AS min_fee_difference,
    MAX(fee_difference) AS max_fee_difference,
    -- Classify outliers as >25% difference
    COUNT(CASE WHEN ABS(percentage_difference) > 25 THEN 1 END) AS outlier_count,
    (COUNT(CASE WHEN ABS(percentage_difference) > 25 THEN 1 END)::FLOAT / COUNT(*) * 100) AS outlier_percentage
FROM fee_comparisons
GROUP BY code_id
ORDER BY outlier_percentage DESC, procedure_count DESC