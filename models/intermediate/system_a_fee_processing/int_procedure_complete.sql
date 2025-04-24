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

-- Get definitions for various coded values
Definitions AS (
    SELECT
        definition_id,
        category_id,
        item_name,
        item_value,
        item_order,
        item_color
    FROM {{ ref('stg_opendental__definition') }}
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

-- Combine everything
ProcedureComplete AS (
    SELECT
        pl.procedure_id,
        pl.patient_id,
        pl.provider_id,
        pl.clinic_id,
        pl.procedure_code_id,
        pl.procedure_date,
        pl.procedure_status,
        def_status.item_name as procedure_status_desc,
        pl.procedure_fee,
        pl.discount as procedure_discount,
        pl.tooth_number,
        pl.surface,
        pl.old_code,
        
        -- Procedure code information
        pc.procedure_code,
        pc.description AS procedure_description,
        pc.abbreviated_description,
        pc.is_hygiene_flag,
        pc.treatment_area,
        def_treatment.item_name as treatment_area_desc,
        pc.is_prosthetic_flag,
        pc.is_multi_visit_flag,
        
        -- Fee information
        sf.fee_id AS standard_fee_id,
        sf.fee_schedule_id,
        sf.standard_fee,
        fs.fee_schedule_description,
        fs.fee_schedule_type_id,
        def_fee_type.item_name as fee_schedule_type_desc,
        
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
        ON pl.procedure_code_id = pc.procedure_code_id
    LEFT JOIN StandardFees sf
        ON pl.procedure_code_id = sf.procedure_code_id
        AND pl.clinic_id = sf.clinic_id
        AND sf.fee_rank = 1  -- Only get the most recent fee
    LEFT JOIN FeeStats fstat
        ON pl.procedure_code_id = fstat.procedure_code_id
    LEFT JOIN FeeSchedules fs
        ON sf.fee_schedule_id = fs.fee_schedule_id
    LEFT JOIN ProcedureNotes pn
        ON pl.procedure_id = pn.procedure_id
    -- Join with definitions for various coded values
    LEFT JOIN Definitions def_status
        ON def_status.category_id = 4  -- Assuming category_id 4 is for procedure status
        AND def_status.item_value = pl.procedure_status::text
    LEFT JOIN Definitions def_treatment
        ON def_treatment.category_id = 5  -- Assuming category_id 5 is for treatment areas
        AND def_treatment.item_value = pc.treatment_area::text
    LEFT JOIN Definitions def_fee_type
        ON def_fee_type.category_id = 6  -- Assuming category_id 6 is for fee schedule types
        AND def_fee_type.item_value = fs.fee_schedule_type_id::text
)

SELECT * FROM ProcedureComplete