-- Investigation Queries for Provider Status Category
-- Date: 2026-01-23
-- Purpose: Investigate provider_status_category to verify all provider_status values are properly mapped
-- Related: validation/VALIDATION_TEMPLATE.md

-- ============================================================================
-- Query 1: Provider Status Distribution in Source
-- ============================================================================

-- Query 1.1: Summary of provider_status values in source
SELECT 
    'Provider Status - Source Distribution' as investigation_type,
    provider_status,
    COUNT(*) as provider_count,
    COUNT(*) FILTER (WHERE termination_date IS NOT NULL) as has_termination_date,
    COUNT(*) FILTER (WHERE termination_date IS NULL) as no_termination_date
FROM staging.stg_opendental__provider
WHERE provider_id IS NOT NULL
GROUP BY provider_status
ORDER BY provider_status;

-- Query 1.2: Check for NULL provider_status values
SELECT 
    'Provider Status - NULL Check' as investigation_type,
    COUNT(*) as total_providers,
    COUNT(*) FILTER (WHERE provider_status IS NULL) as null_provider_status,
    COUNT(*) FILTER (WHERE provider_status IS NOT NULL) as non_null_provider_status
FROM staging.stg_opendental__provider
WHERE provider_id IS NOT NULL;

-- ============================================================================
-- Query 2: Provider Status Category Distribution
-- ============================================================================

-- Query 2.1: Summary of provider_status_category values
SELECT 
    'Provider Status Category - Summary' as investigation_type,
    provider_status_category,
    COUNT(*) as provider_count,
    COUNT(DISTINCT provider_id) as distinct_providers,
    MIN(provider_id) as min_provider_id,
    MAX(provider_id) as max_provider_id
FROM marts.dim_provider
GROUP BY provider_status_category
ORDER BY provider_count DESC;

-- Query 2.2: Breakdown by provider_status and termination_date
SELECT 
    'Provider Status Category - Breakdown' as analysis,
    provider_status_category,
    provider_status,
    CASE 
        WHEN termination_date IS NOT NULL THEN 'Has Termination Date'
        WHEN termination_date IS NULL THEN 'No Termination Date'
        ELSE 'Unknown'
    END as termination_date_status,
    COUNT(*) as provider_count
FROM marts.dim_provider
GROUP BY provider_status_category, provider_status,
    CASE 
        WHEN termination_date IS NOT NULL THEN 'Has Termination Date'
        WHEN termination_date IS NULL THEN 'No Termination Date'
        ELSE 'Unknown'
    END
ORDER BY provider_status_category, provider_status;

-- Query 2.3: Sample records for each provider_status_category
SELECT 
    'Provider Status Category - Sample Records' as analysis,
    provider_id,
    provider_status,
    provider_status_category,
    termination_date,
    provider_status_description,
    is_hidden
FROM marts.dim_provider
WHERE provider_status_category = 'Unknown'
ORDER BY provider_id
LIMIT 20;

-- ============================================================================
-- Query 3: Unknown Status Category Analysis
-- ============================================================================

-- Query 3.1: Detailed analysis of Unknown provider_status_category
SELECT 
    'Provider Status Category - Unknown Analysis' as investigation_type,
    COUNT(*) as total_unknown_records,
    COUNT(DISTINCT provider_id) as distinct_providers,
    COUNT(*) FILTER (WHERE provider_status IS NULL) as null_provider_status,
    COUNT(*) FILTER (WHERE provider_status IS NOT NULL) as non_null_provider_status,
    COUNT(*) FILTER (WHERE termination_date IS NOT NULL) as has_termination_date,
    COUNT(*) FILTER (WHERE termination_date IS NULL) as no_termination_date
FROM marts.dim_provider
WHERE provider_status_category = 'Unknown';

-- Query 3.2: Breakdown of Unknown by provider_status values
SELECT 
    'Provider Status Category - Unknown by Status' as analysis,
    provider_status,
    CASE 
        WHEN provider_status IS NULL THEN 'NULL'
        ELSE provider_status::text
    END as provider_status_text,
    COUNT(*) as provider_count,
    COUNT(*) FILTER (WHERE termination_date IS NOT NULL) as has_termination_date,
    COUNT(*) FILTER (WHERE termination_date IS NULL) as no_termination_date
FROM marts.dim_provider
WHERE provider_status_category = 'Unknown'
GROUP BY provider_status
ORDER BY provider_status;

-- Query 3.3: Sample Unknown records by provider_status
SELECT 
    'Provider Status Category - Unknown Samples' as analysis,
    provider_id,
    provider_status,
    provider_status_category,
    termination_date,
    provider_status_description,
    is_hidden,
    is_secondary,
    is_instructor
FROM marts.dim_provider
WHERE provider_status_category = 'Unknown'
ORDER BY provider_status NULLS LAST, provider_id
LIMIT 30;

-- ============================================================================
-- Query 4: Provider Status vs Status Category Comparison
-- ============================================================================

-- Query 4.1: Cross-tabulation of provider_status and provider_status_category
SELECT 
    'Provider Status - Category Cross Tab' as analysis,
    provider_status,
    provider_status_category,
    COUNT(*) as provider_count
FROM marts.dim_provider
GROUP BY provider_status, provider_status_category
ORDER BY provider_status NULLS LAST, provider_status_category;

-- Query 4.2: Check for unexpected mappings
SELECT 
    'Provider Status - Unexpected Mappings' as analysis,
    provider_status,
    provider_status_category,
    CASE 
        WHEN termination_date IS NOT NULL AND provider_status_category != 'Terminated' THEN 'Termination Date but not Terminated'
        WHEN termination_date IS NULL AND provider_status = 0 AND provider_status_category != 'Active' THEN 'Status 0 but not Active'
        WHEN termination_date IS NULL AND provider_status = 1 AND provider_status_category != 'Inactive' THEN 'Status 1 but not Inactive'
        ELSE 'Other'
    END as mapping_issue,
    COUNT(*) as provider_count
FROM marts.dim_provider
WHERE (
    (termination_date IS NOT NULL AND provider_status_category != 'Terminated')
    OR (termination_date IS NULL AND provider_status = 0 AND provider_status_category != 'Active')
    OR (termination_date IS NULL AND provider_status = 1 AND provider_status_category != 'Inactive')
)
GROUP BY provider_status, provider_status_category,
    CASE 
        WHEN termination_date IS NOT NULL AND provider_status_category != 'Terminated' THEN 'Termination Date but not Terminated'
        WHEN termination_date IS NULL AND provider_status = 0 AND provider_status_category != 'Active' THEN 'Status 0 but not Active'
        WHEN termination_date IS NULL AND provider_status = 1 AND provider_status_category != 'Inactive' THEN 'Status 1 but not Inactive'
        ELSE 'Other'
    END
ORDER BY provider_count DESC;

-- ============================================================================
-- Query 5: Provider Status Description Analysis
-- ============================================================================

-- Query 5.1: Check provider_status_description for Unknown records
SELECT 
    'Provider Status - Description for Unknown' as analysis,
    provider_status,
    provider_status_description,
    COUNT(*) as provider_count
FROM marts.dim_provider
WHERE provider_status_category = 'Unknown'
GROUP BY provider_status, provider_status_description
ORDER BY provider_count DESC;

-- Query 5.2: All provider_status values and their descriptions
SELECT 
    'Provider Status - All Values' as analysis,
    provider_status,
    provider_status_description,
    COUNT(*) as provider_count,
    COUNT(*) FILTER (WHERE provider_status_category = 'Unknown') as unknown_category_count,
    COUNT(*) FILTER (WHERE provider_status_category != 'Unknown') as known_category_count
FROM marts.dim_provider
GROUP BY provider_status, provider_status_description
ORDER BY provider_status NULLS LAST;

-- ============================================================================
-- Query 6: Summary Statistics
-- ============================================================================

-- Query 6.1: Overall summary
SELECT 
    'Provider Status Category - Summary Statistics' as analysis,
    COUNT(*) as total_providers,
    COUNT(*) FILTER (WHERE provider_status_category = 'Active') as active_count,
    COUNT(*) FILTER (WHERE provider_status_category = 'Inactive') as inactive_count,
    COUNT(*) FILTER (WHERE provider_status_category = 'Terminated') as terminated_count,
    COUNT(*) FILTER (WHERE provider_status_category = 'Unknown') as unknown_count,
    ROUND(100.0 * COUNT(*) FILTER (WHERE provider_status_category = 'Unknown') / COUNT(*), 2) as unknown_percentage
FROM marts.dim_provider;
