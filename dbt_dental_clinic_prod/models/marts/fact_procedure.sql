{{ config(
    materialized='table',
    indexes=[
        {'columns': ['procedure_id'], 'unique': true},
        {'columns': ['date_id']},
        {'columns': ['patient_id']},
        {'columns': ['provider_id']},
        {'columns': ['procedure_type_id']}
    ]
) }}

/*
    Fact table for procedure execution details and metrics
    Part of the marts layer - provides business-ready procedure facts
    
    This model:
    1. Creates a comprehensive fact table for procedure analysis
    2. Links procedures to dimension tables via surrogate keys
    3. Provides key metrics for financial and operational analysis
    4. Includes procedure timing and fee information
    
    Business Logic Features:
    - Procedure Execution Facts: Core procedure details with financial metrics
    - Fee Analysis: Actual vs standard fees with variance calculations
    - Timing Metrics: Actual treatment duration from appointment data
    - Quality Flags: Data quality indicators for downstream analysis
    
    Data Sources:
    - int_procedure_complete: Complete procedure data with fee information
    - fact_appointment: Appointment timing data for treatment duration
    - dim_date: Date dimension for temporal analysis
    - dim_patient: Patient dimension for demographic analysis
    - dim_provider: Provider dimension for performance analysis
    - dim_procedure: Procedure dimension for procedure type analysis
    
    Performance Considerations:
    - Table materialization for optimal query performance
    - Indexed on key foreign key columns
    - Includes only essential metrics to keep table size manageable
*/

with procedure_data as (
    select
        procedure_id,
        patient_id,
        provider_id,
        procedure_code_id as procedure_type_id,
        procedure_date,
        procedure_fee as actual_fee,
        standard_fee,
        procedure_time,
        procedure_time_end,
        procedure_status,
        clinic_id,
        appointment_id,
        -- Include metadata fields for downstream use
        _loaded_at,
        _created_at,
        _updated_at,
        _created_by
    from {{ ref('int_procedure_complete') }}
    where procedure_id is not null
        and procedure_date is not null
        and patient_id is not null
        and provider_id is not null
),

-- Join with appointment data to get actual treatment duration
procedure_with_appointment_timing as (
    select
        p.*,
        round(a.treatment_time_minutes, 2) as duration_minutes,
        a.date_time_seated as treatment_start_time,
        a.date_time_dismissed as treatment_end_time,
        a.appointment_id,
        a.appointment_datetime
    from procedure_data p
    left join {{ ref('fact_appointment') }} a
        on p.appointment_id = a.appointment_id
),

-- Join with dimension tables to get surrogate keys
final as (
    select
        -- Primary key
        p.procedure_id,
        
        -- Foreign keys to dimension tables
        d.date_id,
        p.patient_id,
        p.provider_id,
        p.procedure_type_id,
        
        -- Financial metrics
        p.actual_fee,
        p.standard_fee,
        
        -- Operational metrics
        p.duration_minutes,
        
        -- Standardized metadata
        {{ standardize_mart_metadata(
            primary_source_alias='p',
            source_metadata_fields=['_loaded_at', '_created_at', '_updated_at', '_created_by']
        ) }}
        
    from procedure_with_appointment_timing p
    left join {{ ref('dim_date') }} d
        on p.procedure_date = d.date_day
    where p.procedure_status in (2, 4)  -- 2=Completed, 4=Existing Prior
)

select * from final
