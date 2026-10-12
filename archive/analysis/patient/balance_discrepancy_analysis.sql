{{
    config(
        materialized='table'
    )
}}

SELECT 
    CASE 
        WHEN ABS(estimated_balance - total_balance) <= 0.01 THEN 'No Difference'
        WHEN estimated_balance = 0 AND total_balance = 0 THEN 'Both Zero'
        WHEN estimated_balance < 0 AND total_balance = 0 THEN 'Negative Est, Zero Total'
        WHEN estimated_balance = 0 AND total_balance < 0 THEN 'Zero Est, Negative Total'
        WHEN estimated_balance > 0 AND total_balance = 0 THEN 'Positive Est, Zero Total'
        WHEN estimated_balance = 0 AND total_balance > 0 THEN 'Zero Est, Positive Total'
        ELSE 'Other Difference'
    END as difference_type,
    COUNT(*) as patient_count,
    ROUND(AVG(ABS(estimated_balance - total_balance)), 2) as avg_difference,
    MIN(ABS(estimated_balance - total_balance)) as min_difference,
    MAX(ABS(estimated_balance - total_balance)) as max_difference
FROM {{ ref('stg_opendental__patient') }}
GROUP BY 
    CASE 
        WHEN ABS(estimated_balance - total_balance) <= 0.01 THEN 'No Difference'
        WHEN estimated_balance = 0 AND total_balance = 0 THEN 'Both Zero'
        WHEN estimated_balance < 0 AND total_balance = 0 THEN 'Negative Est, Zero Total'
        WHEN estimated_balance = 0 AND total_balance < 0 THEN 'Zero Est, Negative Total'
        WHEN estimated_balance > 0 AND total_balance = 0 THEN 'Positive Est, Zero Total'
        WHEN estimated_balance = 0 AND total_balance > 0 THEN 'Zero Est, Positive Total'
        ELSE 'Other Difference'
    END
ORDER BY patient_count DESC; 