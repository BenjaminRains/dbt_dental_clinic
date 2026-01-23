-- Investigate provider_id NULL values in fact_claim
-- Purpose: Understand why 4,627 records have NULL provider_id
-- Run this in DBeaver against the analytics database (PostgreSQL)

-- Query 1: Summary of provider_id NULL patterns
SELECT 
    COUNT(*) as total_records,
    COUNT(CASE WHEN provider_id IS NULL THEN 1 END) as null_provider_count,
    COUNT(CASE WHEN provider_id IS NOT NULL THEN 1 END) as non_null_provider_count,
    ROUND(COUNT(CASE WHEN provider_id IS NULL THEN 1 END) * 100.0 / COUNT(*), 2) as pct_null
FROM marts.fact_claim
WHERE claim_date >= '2023-01-01';

-- Query 2: Check if NULL provider_id correlates with NULL procedure details
SELECT 
    CASE 
        WHEN provider_id IS NULL AND procedure_code IS NULL THEN 'Both NULL'
        WHEN provider_id IS NULL AND procedure_code IS NOT NULL THEN 'Provider NULL, Code exists'
        WHEN provider_id IS NOT NULL AND procedure_code IS NULL THEN 'Provider exists, Code NULL'
        ELSE 'Both exist'
    END as null_pattern,
    COUNT(*) as record_count
FROM marts.fact_claim
WHERE claim_date >= '2023-01-01'
GROUP BY 
    CASE 
        WHEN provider_id IS NULL AND procedure_code IS NULL THEN 'Both NULL'
        WHEN provider_id IS NULL AND procedure_code IS NOT NULL THEN 'Provider NULL, Code exists'
        WHEN provider_id IS NOT NULL AND procedure_code IS NULL THEN 'Provider exists, Code NULL'
        ELSE 'Both exist'
    END
ORDER BY record_count DESC;

-- Query 3: Check if NULL provider_id correlates with claim_id = 0 (pre-auth)
SELECT 
    CASE WHEN claim_id = 0 THEN 'Pre-auth (claim_id=0)' ELSE 'Regular Claims' END as claim_type,
    COUNT(*) as total_records,
    COUNT(CASE WHEN provider_id IS NULL THEN 1 END) as null_provider_count,
    COUNT(CASE WHEN provider_id IS NOT NULL THEN 1 END) as non_null_provider_count
FROM marts.fact_claim
WHERE claim_date >= '2023-01-01'
GROUP BY 
    CASE WHEN claim_id = 0 THEN 'Pre-auth (claim_id=0)' ELSE 'Regular Claims' END;

-- Query 4: Sample records with NULL provider_id to see patterns
SELECT 
    claim_id,
    procedure_id,
    claim_procedure_id,
    provider_id,
    procedure_code,
    procedure_description,
    claim_date,
    claim_status,
    claim_type
FROM marts.fact_claim
WHERE provider_id IS NULL
    AND claim_date >= '2023-01-01'
ORDER BY claim_date DESC
LIMIT 20;

-- Query 5: Check if procedures exist in procedurelog but provider_id is NULL
-- This would indicate procedures without assigned providers
SELECT 
    COUNT(DISTINCT fc.procedure_id) as procedures_with_null_provider,
    COUNT(*) as total_records_with_null_provider
FROM marts.fact_claim fc
WHERE fc.provider_id IS NULL
    AND fc.claim_date >= '2023-01-01'
    AND EXISTS (
        SELECT 1 
        FROM staging.stg_opendental__procedurelog pl
        WHERE pl.procedure_id = fc.procedure_id
    );

-- Query 6: Verify that NULL provider_id procedures don't exist in procedurelog
-- This confirms the root cause: pre-auth procedures haven't been completed yet
SELECT 
    COUNT(DISTINCT fc.procedure_id) as procedures_not_in_procedurelog,
    COUNT(*) as total_records_not_in_procedurelog,
    COUNT(DISTINCT fc.procedure_id) FILTER (WHERE fc.procedure_id = 0) as procedure_id_zero_count,
    COUNT(DISTINCT fc.procedure_id) FILTER (WHERE fc.procedure_id != 0) as procedure_id_nonzero_count
FROM marts.fact_claim fc
WHERE fc.provider_id IS NULL
    AND fc.claim_date >= '2023-01-01'
    AND NOT EXISTS (
        SELECT 1 
        FROM staging.stg_opendental__procedurelog pl
        WHERE pl.procedure_id = fc.procedure_id
    );

-- Query 7: Check if these procedures exist in treatment plans (proctp)
-- Pre-auth procedures might be planned procedures that haven't been completed
-- proctp links procedures to treatment plans and includes provider_id
SELECT 
    COUNT(DISTINCT fc.procedure_id) as procedures_in_treatplan,
    COUNT(*) as total_records_in_treatplan,
    COUNT(DISTINCT ptp.provider_id) as distinct_providers_in_treatplan
FROM marts.fact_claim fc
LEFT JOIN staging.stg_opendental__proctp ptp
    ON fc.procedure_id = ptp.procedure_id_orig
WHERE fc.provider_id IS NULL
    AND fc.claim_date >= '2023-01-01'
    AND ptp.procedure_id_orig IS NOT NULL;

-- Query 8: Sample records showing treatplan provider if available
-- This helps determine if we can get provider from treatment plans
-- proctp links procedures to treatment plans and includes provider_id
SELECT 
    fc.claim_id,
    fc.procedure_id,
    fc.claim_procedure_id,
    fc.provider_id as fact_claim_provider_id,
    ptp.provider_id as treatplan_provider_id,
    ptp.procedure_id_orig,
    fc.procedure_code,
    fc.claim_date,
    fc.claim_status,
    fc.claim_type
FROM marts.fact_claim fc
LEFT JOIN staging.stg_opendental__proctp ptp
    ON fc.procedure_id = ptp.procedure_id_orig
WHERE fc.provider_id IS NULL
    AND fc.claim_date >= '2023-01-01'
ORDER BY fc.claim_date DESC
LIMIT 20;

-- Query 9: Check source claimproc table for any provider information
-- Verify what data is available in the source for pre-auth claims
SELECT 
    COUNT(DISTINCT cp.procedure_id) as distinct_procedures_in_claimproc,
    COUNT(*) as total_claimproc_records_with_null_provider,
    COUNT(DISTINCT cp.procedure_id) FILTER (WHERE cp.procedure_id = 0) as procedure_id_zero_in_claimproc,
    COUNT(DISTINCT cp.procedure_id) FILTER (WHERE cp.procedure_id != 0) as procedure_id_nonzero_in_claimproc
FROM staging.stg_opendental__claimproc cp
WHERE cp.claim_id = 0
    AND cp.procedure_date >= '2023-01-01'
    AND NOT EXISTS (
        SELECT 1 
        FROM staging.stg_opendental__procedurelog pl
        WHERE pl.procedure_id = cp.procedure_id
    );

-- Query 10: Check if appointments exist for these pre-auth procedures
-- Scheduled appointments might have provider information
SELECT 
    COUNT(DISTINCT fc.procedure_id) as procedures_with_appointments,
    COUNT(*) as total_records_with_appointments,
    COUNT(DISTINCT a.provider_id) as distinct_providers_from_appointments
FROM marts.fact_claim fc
LEFT JOIN staging.stg_opendental__appointment a
    ON fc.patient_id = a.patient_id
    AND fc.claim_date::date = a.appointment_datetime::date
WHERE fc.provider_id IS NULL
    AND fc.claim_date >= '2023-01-01'
    AND a.appointment_id IS NOT NULL;

-- Query 11: Summary analysis - Can we populate provider_id from alternative sources?
-- This provides a comprehensive view of potential data sources
WITH null_provider_claims AS (
    SELECT DISTINCT
        fc.claim_id,
        fc.procedure_id,
        fc.patient_id,
        fc.claim_date
    FROM marts.fact_claim fc
    WHERE fc.provider_id IS NULL
        AND fc.claim_date >= '2023-01-01'
)
SELECT 
    'Total NULL provider records' as metric,
    COUNT(*) as count
FROM null_provider_claims
UNION ALL
SELECT 
    'Procedures in procedurelog' as metric,
    COUNT(DISTINCT npc.procedure_id) as count
FROM null_provider_claims npc
WHERE EXISTS (
    SELECT 1 FROM staging.stg_opendental__procedurelog pl
    WHERE pl.procedure_id = npc.procedure_id
)
UNION ALL
SELECT 
    'Procedures in treatplan' as metric,
    COUNT(DISTINCT npc.procedure_id) as count
FROM null_provider_claims npc
WHERE EXISTS (
    SELECT 1 FROM staging.stg_opendental__proctp ptp
    WHERE ptp.procedure_id_orig = npc.procedure_id
)
UNION ALL
SELECT 
    'Procedures with appointments' as metric,
    COUNT(DISTINCT npc.procedure_id) as count
FROM null_provider_claims npc
WHERE EXISTS (
    SELECT 1 FROM staging.stg_opendental__appointment a
    WHERE a.patient_id = npc.patient_id
    AND a.appointment_datetime::date = npc.claim_date::date
)
UNION ALL
SELECT 
    'Procedures with procedure_id = 0' as metric,
    COUNT(DISTINCT npc.procedure_id) as count
FROM null_provider_claims npc
WHERE npc.procedure_id = 0;
