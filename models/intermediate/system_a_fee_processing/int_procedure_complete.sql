{{ config(
    materialized='incremental',
    unique_key='procedure_id',
    schema='intermediate'
) }}

-- First, get the procedure logs
WITH ProcedureLog AS (
    SELECT *
    FROM {{ ref('stg_opendental__procedurelog') }}
    {% if is_incremental() %}
        WHERE date_timestamp > (select max(date_timestamp) from {{ this }})
    {% endif %}
),

-- Get the procedure codes
ProcedureCodes AS (
    SELECT 
        procedure_code_id,
        procedure_code,
        description,
        abbreviated_description,
        is_hygiene_flag,
        treatment_area,
        is_prosthetic_flag,
        is_multi_visit_flag
    FROM {{ ref('stg_opendental__procedurecode') }}
),

-- Get procedure notes
ProcedureNotes AS (
    SELECT 
        procedure_id,
        STRING_AGG(note, ' | ' ORDER BY entry_timestamp) AS procedure_notes,
        COUNT(*) AS note_count,
        MAX(entry_timestamp) AS last_note_timestamp
    FROM {{ ref('stg_opendental__procnote') }}
    GROUP BY procedure_id
),

-- Get fee schedules
FeeSchedules AS (
    SELECT
        fee_schedule_id,
        fee_schedule_description,
        fee_schedule_type_id,
        is_hidden
    FROM {{ ref('stg_opendental__feesched') }}
),

-- Standard fees with ranking to get the most relevant fee per procedure code
StandardFees AS (
    SELECT
        fee_id,
        procedure_code_id,
        fee_schedule_id,
        clinic_id,
        provider_id,
        fee_amount AS standard_fee,
        created_at,
        ROW_NUMBER() OVER (
            PARTITION BY procedure_code_id, clinic_id
            ORDER BY created_at DESC
        ) AS fee_rank
    FROM {{ ref('stg_opendental__fee') }}
),

-- Create fee statistics separately to avoid duplication
FeeStats AS (
    SELECT
        procedure_code_id,
        COUNT(DISTINCT fee_id) AS available_fee_options,
        MIN(fee_amount) AS min_available_fee,
        MAX(fee_amount) AS max_available_fee,
        AVG(fee_amount) AS avg_fee_amount
    FROM {{ ref('stg_opendental__fee') }}
    GROUP BY procedure_code_id
),

-- Map procedure status to a descriptive string
ProcedureStatusMap AS (
    SELECT 1 AS procedure_status, 'Treatment Plan' AS procedure_status_desc 
    UNION ALL SELECT 2, 'Complete'
    UNION ALL SELECT 3, 'Existing Current'
    UNION ALL SELECT 4, 'Existing Other'
    UNION ALL SELECT 5, 'Referred'
    UNION ALL SELECT 6, 'Deleted'
    UNION ALL SELECT 7, 'Condition'
    UNION ALL SELECT 8, 'EHR Planned'
    UNION ALL SELECT 9, 'Draft'
),

-- Combine everything
ProcedureComplete AS (
    SELECT
        pl.procedure_id,
        pl.patient_id,
        pl.provider_id,
        pl.clinic_id,
        pl.code_id AS procedure_code_id,
        pl.procedure_date,
        pl.procedure_status,
        ps.procedure_status_desc,
        pl.procedure_fee,
        pl.tooth_number,
        pl.surface,
        
        -- Procedure code information
        pc.procedure_code,
        pc.description AS procedure_description,
        pc.abbreviated_description,
        pc.is_hygiene_flag,
        pc.treatment_area,
        pc.is_prosthetic_flag,
        pc.is_multi_visit_flag,
        
        -- Fee information
        sf.fee_id AS standard_fee_id,
        sf.fee_schedule_id,
        sf.standard_fee,
        fs.fee_schedule_description,
        fs.fee_schedule_type_id,
        
        -- Fee statistics
        fstat.available_fee_options,
        fstat.min_available_fee,
        fstat.max_available_fee,
        fstat.avg_fee_amount,
        
        -- Fee validation flags
        CASE 
            WHEN sf.standard_fee IS NULL THEN FALSE
            ELSE TRUE 
        END AS has_standard_fee,
        
        CASE 
            WHEN ABS(COALESCE(pl.procedure_fee, 0) - COALESCE(sf.standard_fee, 0)) < 0.01 THEN TRUE
            ELSE FALSE 
        END AS fee_matches_standard,
        
        -- Procedure notes
        pn.procedure_notes,
        pn.note_count,
        pn.last_note_timestamp,
        
        -- Metadata and timestamps
        pl.date_timestamp,
        pl._airbyte_loaded_at,
        current_timestamp AS _loaded_at
        
    FROM ProcedureLog pl
    LEFT JOIN ProcedureCodes pc 
        ON pl.code_id = pc.procedure_code_id
    LEFT JOIN StandardFees sf
        ON pl.code_id = sf.procedure_code_id
        AND pl.clinic_id = sf.clinic_id
        AND sf.fee_rank = 1  -- Only get the most recent fee
    LEFT JOIN FeeStats fstat
        ON pl.code_id = fstat.procedure_code_id
    LEFT JOIN FeeSchedules fs
        ON sf.fee_schedule_id = fs.fee_schedule_id
    LEFT JOIN ProcedureNotes pn
        ON pl.procedure_id = pn.procedure_id
    LEFT JOIN ProcedureStatusMap ps
        ON pl.procedure_status = ps.procedure_status
)

SELECT * FROM ProcedureComplete