-- KPI investigation: Production by Procedure (2026-06-10)
-- Run in DBeaver against PostgreSQL opendental_analytics (analytics_user).
--
-- Golden: golden/od_daily_production_by_procedure_06102026_06102026.csv
-- OD report total: $15,239.00 (28 procedure codes)
--
-- OD report uses DateComplete + ProcStatus=2. Compare both date fields below.

-- =============================================================================
-- Query 1: Date field comparison (totals)
-- =============================================================================
SELECT
    'procdate_status_2_4' AS layer,
    count(*) AS procedure_row_count,
    round(sum(pl.procedure_fee)::numeric, 2) AS total_fees
FROM staging.stg_opendental__procedurelog pl
WHERE pl.procedure_date::date = DATE '2026-06-10'
  AND pl.procedure_status IN (2, 4)
UNION ALL
SELECT
    'datecomplete_status_2',
    count(*),
    round(sum(pl.procedure_fee)::numeric, 2)
FROM staging.stg_opendental__procedurelog pl
WHERE pl.date_complete::date = DATE '2026-06-10'
  AND pl.procedure_status = 2
UNION ALL
SELECT
    'mart_fact_procedure',
    count(*),
    round(sum(fp.actual_fee)::numeric, 2)
FROM marts.fact_procedure fp
WHERE fp.date_id = DATE '2026-06-10';

-- =============================================================================
-- Query 2: Staging by code — OD-aligned (DateComplete, status 2)
-- =============================================================================
SELECT
    trim(pc.procedure_code) AS procedure_code,
    pc.description,
    count(*) AS quantity,
    round(sum(pl.procedure_fee)::numeric, 2) AS total_fees,
    round(avg(pl.procedure_fee)::numeric, 2) AS avg_fee
FROM staging.stg_opendental__procedurelog pl
INNER JOIN staging.stg_opendental__procedurecode pc
    ON pl.procedure_code_id = pc.procedure_code_id
WHERE pl.date_complete::date = DATE '2026-06-10'
  AND pl.procedure_status = 2
GROUP BY 1, 2
ORDER BY total_fees DESC;

-- =============================================================================
-- Query 3: Codes in OD golden but missing on DateComplete (status 2)
--     — check if they exist on ProcDate instead
-- =============================================================================
WITH od_codes AS (
    SELECT unnest(ARRAY[
        'D0120','D0140','D0150','D0171','D0210','D0220','D0230','D0272','D0274',
        'D0330','D0350','D0801','D1110','D1120','D1206','D4346','D4267','D4910',
        'D5214','D2799','D7140','D7210','D7953','09902','D2919','D9230','D9986','D9987'
    ]) AS procedure_code
),
complete AS (
    SELECT trim(pc.procedure_code) AS procedure_code, count(*) AS qty
    FROM staging.stg_opendental__procedurelog pl
    INNER JOIN staging.stg_opendental__procedurecode pc
        ON pl.procedure_code_id = pc.procedure_code_id
    WHERE pl.date_complete::date = DATE '2026-06-10'
      AND pl.procedure_status = 2
    GROUP BY 1
),
procdate AS (
    SELECT trim(pc.procedure_code) AS procedure_code, count(*) AS qty,
           min(pl.procedure_date::date) AS min_proc_date,
           max(pl.procedure_date::date) AS max_proc_date
    FROM staging.stg_opendental__procedurelog pl
    INNER JOIN staging.stg_opendental__procedurecode pc
        ON pl.procedure_code_id = pc.procedure_code_id
    WHERE pl.procedure_date::date = DATE '2026-06-10'
      AND pl.procedure_status IN (2, 4)
    GROUP BY 1
)
SELECT
    o.procedure_code,
    c.qty AS qty_on_date_complete,
    pd.qty AS qty_on_proc_date,
    pd.min_proc_date,
    pd.max_proc_date
FROM od_codes o
LEFT JOIN complete c ON c.procedure_code = o.procedure_code
LEFT JOIN procdate pd ON pd.procedure_code = o.procedure_code
WHERE c.procedure_code IS NULL OR c.qty IS DISTINCT FROM pd.qty
ORDER BY o.procedure_code;

-- =============================================================================
-- Query 4: Status sweep on 2026-06-10 (ProcDate OR DateComplete)
-- =============================================================================
SELECT
    pl.procedure_status,
    count(*) AS row_count,
    round(sum(pl.procedure_fee)::numeric, 2) AS total_fees
FROM staging.stg_opendental__procedurelog pl
WHERE pl.date_complete::date = DATE '2026-06-10'
   OR pl.procedure_date::date = DATE '2026-06-10'
GROUP BY 1
ORDER BY total_fees DESC;

-- =============================================================================
-- Query 5: Find warehouse dates near OD golden total ($15,239) in June 2026
-- =============================================================================
SELECT
    pl.date_complete::date AS production_date,
    count(*) AS row_count,
    round(sum(pl.procedure_fee)::numeric, 2) AS total_fees
FROM staging.stg_opendental__procedurelog pl
WHERE pl.procedure_status = 2
  AND pl.date_complete::date BETWEEN DATE '2026-06-01' AND DATE '2026-06-30'
GROUP BY 1
HAVING round(sum(pl.procedure_fee)::numeric, 2) BETWEEN 14000 AND 16000
ORDER BY production_date;

-- =============================================================================
-- Query 7: Find ANY date with complete production near OD golden ($15,239 ± $500)
-- =============================================================================
SELECT
    pl.date_complete::date AS production_date,
    count(*) AS row_count,
    round(sum(pl.procedure_fee)::numeric, 2) AS total_fees
FROM staging.stg_opendental__procedurelog pl
WHERE pl.procedure_status = 2
  AND pl.date_complete::date >= DATE '2025-01-01'
GROUP BY 1
HAVING round(sum(pl.procedure_fee)::numeric, 2) BETWEEN 14700 AND 15800
ORDER BY production_date DESC
LIMIT 20;

-- =============================================================================
-- Query 8: Incremental sync check — raw.procedurelog vs staging (status 2, 2026-06-10)
-- =============================================================================
SELECT
    'raw_status_2' AS layer,
    count(*) AS row_count,
    round(sum(pl."ProcFee")::numeric, 2) AS total_fees
FROM raw.procedurelog pl
WHERE pl."ProcStatus" = 2
  AND (pl."DateComplete"::date = DATE '2026-06-10'
    OR pl."ProcDate"::date = DATE '2026-06-10')
UNION ALL
SELECT
    'staging_status_2',
    count(*),
    round(sum(pl.procedure_fee)::numeric, 2)
FROM staging.stg_opendental__procedurelog pl
WHERE pl.procedure_status = 2
  AND (pl.date_complete::date = DATE '2026-06-10'
    OR pl.procedure_date::date = DATE '2026-06-10');

-- =============================================================================
-- Query 9: Stale ProcStatus? — status 1 rows on golden date (candidates for missed TP→Complete)
-- =============================================================================
SELECT
    trim(pc.procedure_code) AS procedure_code,
    count(*) AS qty_status_1,
    round(sum(pl.procedure_fee)::numeric, 2) AS fees_status_1
FROM staging.stg_opendental__procedurelog pl
INNER JOIN staging.stg_opendental__procedurecode pc
    ON pl.procedure_code_id = pc.procedure_code_id
WHERE pl.procedure_status = 1
  AND (pl.date_complete::date = DATE '2026-06-10'
    OR pl.procedure_date::date = DATE '2026-06-10')
GROUP BY 1
ORDER BY fees_status_1 DESC;
