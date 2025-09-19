{{ config(
    materialized='incremental',
    schema='intermediate',
    unique_key='procedure_id',
    on_schema_change='append_new_columns',
    incremental_strategy='merge',
    indexes=[
        {'columns': ['procedure_id'], 'unique': true},
        {'columns': ['patient_id']},
        {'columns': ['_updated_at']}
    ]) }}

/*
    Intermediate model for procedure_complete
    Part of System A: Fee Processing & Verification
    
    This model:
    1. Consolidates procedure data with complete fee information and validation
    2. Enriches procedures with associated notes, definitions, and fee schedules
    3. Provides fee validation flags and statistics for financial analysis
    
    Business Logic Features:
    - Fee Validation: Compares actual procedure fees against standard fee schedules
    - Fee Statistics: Aggregates min, max, and average fees by procedure code
    - Definition Lookups: Resolves coded values to human-readable descriptions
    - Note Aggregation: Consolidates all procedure notes with timestamps
    
    Data Quality Notes:
    - Fee matching uses 0.01 tolerance for decimal precision issues
    - Definition category IDs are assumed based on OpenDental standards (4=status, 5=treatment_area, 6=fee_type)
    - Missing standard fees are flagged for follow-up fee schedule configuration
    
    Performance Considerations:
    - Incremental materialization based on date_timestamp for efficient updates
    - ROW_NUMBER() window function for fee ranking to get most recent fees
    - Separate fee statistics CTE to avoid duplication in main query
*/

-- 1. Source data retrieval
with source_procedure_log as (
    select *
    from {{ ref('stg_opendental__procedurelog') }}
    {% if is_incremental() %}
        where _updated_at > (select max(_updated_at) from {{ this }})
    {% endif %}
),

-- 2. Lookup/reference data
source_procedure_codes as (
    SELECT 
        procedure_code_id,
        procedure_code,
        description,
        abbreviated_description,
        is_hygiene,
        treatment_area,
        is_prosthetic,
        is_multi_visit
    FROM {{ ref('stg_opendental__procedurecode') }}
),

source_procedure_notes as (
    select *
    from {{ ref('stg_opendental__procnote') }}
),

source_fee_schedules as (
    SELECT
        fee_schedule_id,
        fee_schedule_description,
        fee_schedule_type_id,
        is_hidden
    FROM {{ ref('stg_opendental__feesched') }}
),

source_definitions as (
    SELECT
        definition_id,
        category_id,
        item_name,
        item_value,
        item_order,
        item_color
    FROM {{ ref('stg_opendental__definition') }}
),

-- 3. Calculation/aggregation CTEs
standard_fees_ranked as (
    SELECT
        fee_id,
        procedure_code_id,
        fee_schedule_id,
        clinic_id,
        provider_id,
        fee_amount AS standard_fee,
        _created_at,
        ROW_NUMBER() OVER (
            PARTITION BY procedure_code_id, clinic_id
            ORDER BY _created_at DESC
        ) AS fee_rank
    FROM {{ ref('stg_opendental__fee') }}
),

fee_statistics as (
    SELECT
        procedure_code_id,
        COUNT(DISTINCT fee_id) AS available_fee_options,
        MIN(fee_amount) AS min_available_fee,
        MAX(fee_amount) AS max_available_fee,
        AVG(fee_amount) AS avg_fee_amount
    FROM {{ ref('stg_opendental__fee') }}
    GROUP BY procedure_code_id
),

-- 4. Business logic transformation
procedure_notes_aggregated as (
    SELECT 
        procedure_id,
        STRING_AGG(note, ' | ' ORDER BY entry_timestamp) AS procedure_notes,
        COUNT(*) AS note_count,
        MAX(entry_timestamp) AS last_note_timestamp
    FROM {{ ref('stg_opendental__procnote') }}
    GROUP BY procedure_id
),

-- 5. Integration CTE (joins everything together)
procedure_complete_integrated as (
    SELECT
        pl.procedure_id,
        pl.patient_id,
        pl.provider_id,
        pl.clinic_id,
        pl.procedure_code_id,
        pl.appointment_id,
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
        pc.is_hygiene,
        pc.treatment_area,
        def_treatment.item_name as treatment_area_desc,
        pc.is_prosthetic,
        pc.is_multi_visit,
        
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
        
        -- Metadata fields (standardized pattern)
        {{ standardize_intermediate_metadata(primary_source_alias='pl') }}
        
    from source_procedure_log pl
    left join source_procedure_codes pc 
        on pl.procedure_code_id = pc.procedure_code_id
    left join standard_fees_ranked sf
        on pl.procedure_code_id = sf.procedure_code_id
        and pl.clinic_id = sf.clinic_id
        and sf.fee_rank = 1  -- Only get the most recent fee
    left join fee_statistics fstat
        on pl.procedure_code_id = fstat.procedure_code_id
    left join source_fee_schedules fs
        on sf.fee_schedule_id = fs.fee_schedule_id
    left join procedure_notes_aggregated pn
        on pl.procedure_id = pn.procedure_id
    -- Join with definitions for various coded values
    left join source_definitions def_status
        on def_status.category_id = 4  -- Assuming category_id 4 is for procedure status
        and def_status.item_value = pl.procedure_status::text
    left join source_definitions def_treatment
        on def_treatment.category_id = 5  -- Assuming category_id 5 is for treatment areas
        and def_treatment.item_value = pc.treatment_area::text
    left join source_definitions def_fee_type
        on def_fee_type.category_id = 6  -- Assuming category_id 6 is for fee schedule types
        and def_fee_type.item_value = fs.fee_schedule_type_id::text
)

select * from procedure_complete_integrated
