-- =============================================================================
-- Monitoring: Unclassified appointment rate (appointment_type_id = 0)
-- =============================================================================
-- Purpose: Track the rate of appointments with no type classification over time
--          to support TODO "Appointment Workflow Improvement - Type Classification"
-- Target:  Analytics DB (PostgreSQL) - opendental_analytics, schema public_staging
-- Run in:  DBeaver (or any PostgreSQL client)
-- Usage:   Run periodically; compare pct_unclassified month-over-month;
--          consider alerting if pct_unclassified exceeds a threshold (e.g. 50%)
-- =============================================================================

-- Adjust schema if your staging model lives elsewhere (e.g. public for dbt default)
-- Replace public_staging with the schema that contains stg_opendental__appointment

-- Monthly rate of unclassified appointments (type_id = 0)
SELECT
    date_trunc('month', appointment_datetime)::date AS appointment_month,
    COUNT(*) AS total_appointments,
    COUNT(*) FILTER (WHERE appointment_type_id = 0) AS unclassified_count,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE appointment_type_id = 0) / NULLIF(COUNT(*), 0),
        2
    ) AS pct_unclassified,
    -- Optional: flag months above threshold for alerting (e.g. 50%)
    CASE
        WHEN 100.0 * COUNT(*) FILTER (WHERE appointment_type_id = 0) / NULLIF(COUNT(*), 0) > 50
        THEN 'ALERT: above 50%'
        ELSE 'OK'
    END AS threshold_50pct
FROM public_staging.stg_opendental__appointment
WHERE appointment_datetime >= '2023-01-01'
  AND patient_id IS NOT NULL
GROUP BY date_trunc('month', appointment_datetime)
ORDER BY appointment_month DESC;

-- Optional: Single-number summary for "current" period (last full month)
-- Uncomment to run as a quick health check
/*
SELECT
    COUNT(*) AS total,
    COUNT(*) FILTER (WHERE appointment_type_id = 0) AS unclassified,
    ROUND(100.0 * COUNT(*) FILTER (WHERE appointment_type_id = 0) / NULLIF(COUNT(*), 0), 2) AS pct_unclassified
FROM public_staging.stg_opendental__appointment
WHERE appointment_datetime >= date_trunc('month', current_date - interval '1 month')
  AND appointment_datetime < date_trunc('month', current_date)
  AND patient_id IS NOT NULL;
*/
