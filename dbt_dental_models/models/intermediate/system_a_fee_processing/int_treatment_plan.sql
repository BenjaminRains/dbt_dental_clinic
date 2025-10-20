{{ config(
    materialized='table',
    schema='intermediate',
    unique_key='treatment_plan_id',
    on_schema_change='fail',
    indexes=[
        {'columns': ['treatment_plan_id'], 'unique': true},
        {'columns': ['patient_id']},
        {'columns': ['treatment_plan_date']},
        {'columns': ['treatment_plan_status']},
        {'columns': ['_updated_at']}
    ],
    tags=['foundation', 'weekly']) }}

/*
    Intermediate model for treatment plan management
    Part of System A: Treatment Planning and Fee Processing
    
    This model:
    1. Combines treatment plans with attached procedures and procedure details
    2. Calculates total treatment plan values and completion tracking
    3. Tracks acceptance and completion status with timeline analysis
    4. Provides comprehensive treatment plan metrics for revenue analysis
    5. Identifies delays and provides recovery potential assessment
    
    Business Logic Features:
    - Treatment plan value calculation: Total planned, completed, and remaining amounts
    - Completion tracking: Percentage complete, procedure counts
    - Timeline analysis: Days to first procedure, days since last activity
    - Acceptance tracking: Patient signature, practice signature, fully accepted flags
    - Status categorization: Not started, in progress, completed
    - Delay assessment: Current, Recent, Delayed, Very Delayed
    - Provider assignment: Primary provider and clinic from procedures
    
    Data Sources:
    - stg_opendental__treatplan: Treatment plan headers
    - stg_opendental__treatplanattach: Procedure attachments to treatment plans
    - stg_opendental__procedurelog: Actual procedure details and fees
    - int_procedure_catalog: Procedure code information and categorization
    
    Data Quality Notes:
    - Treatment plans without attached procedures have NULL amounts and counts
    - Provider and clinic determined from most recent attached procedure
    - Completion percentage calculated only when procedures exist
    - Timeline analysis handles plans with no procedures started
    - Acceptance flags based on signature timestamps
    
    Performance Considerations:
    - Table materialization (treatment plans change infrequently)
    - Indexed on treatment_plan_id, patient_id, status, and date
    - Weekly refresh cycle appropriate for treatment plan updates
    - Procedure aggregations pre-calculated for mart efficiency
*/

-- 1. Source treatment plan headers
with treatment_plan_base as (
    select
        treatment_plan_id,
        patient_id,
        treatment_plan_date,
        treatment_plan_status,
        heading,
        note,
        entry_user_id,
        presenter_user_id,
        signature,
        signature_practice,
        responsible_party_id,
        signed_timestamp as date_time_signed_patient,
        practice_signed_timestamp as date_time_signed_practice,
        mobile_app_device_id,
        _loaded_at,
        _created_at,
        _updated_at
    from {{ ref('stg_opendental__treatplan') }}
),

-- 2. Treatment plan procedure attachments
treatment_plan_procedures as (
    select
        tpa.treatplan_id as treatment_plan_id,
        tpa.procedure_id,
        tpa.priority,
        tpa.treatplan_attach_id as treatment_plan_attach_id
    from {{ ref('stg_opendental__treatplanattach') }} tpa
),

-- 3. Procedure details from procedurelog
procedure_details as (
    select
        pl.procedure_id,
        pl.procedure_code_id,
        pl.procedure_date,
        pl.procedure_status,
        pl.patient_id,
        pl.provider_id,
        pl.clinic_id,
        pl.procedure_fee,
        pl.tooth_number,
        pl.tooth_range,
        pc.procedure_code,
        pc.procedure_description,
        pc.procedure_category
    from {{ ref('stg_opendental__procedurelog') }} pl
    left join {{ ref('int_procedure_catalog') }} pc
        on pl.procedure_code_id = pc.procedure_code_id
),

-- 4. Aggregate procedure information per treatment plan
treatment_plan_aggregated as (
    select
        tpp.treatment_plan_id,
        count(distinct tpp.procedure_id) as procedure_count,
        sum(pd.procedure_fee) as total_planned_amount,
        array_agg(distinct pd.procedure_code order by pd.procedure_code) 
            filter (where pd.procedure_code is not null) as procedure_codes,
        array_agg(distinct pd.procedure_category order by pd.procedure_category)
            filter (where pd.procedure_category is not null) as procedure_categories,
        count(distinct case when pd.procedure_status = 2 then pd.procedure_id end) as completed_procedure_count,
        sum(case when pd.procedure_status = 2 then pd.procedure_fee else 0 end) as completed_amount,
        min(pd.procedure_date) as earliest_procedure_date,
        max(pd.procedure_date) as latest_procedure_date,
        max(pd.provider_id) as primary_provider_id,  -- Most recent provider
        max(pd.clinic_id) as primary_clinic_id       -- Most recent clinic
    from treatment_plan_procedures tpp
    left join procedure_details pd
        on tpp.procedure_id = pd.procedure_id
    group by tpp.treatment_plan_id
),

-- 5. Business logic enhancement
treatment_plan_enhanced as (
    select
        tpb.treatment_plan_id,
        tpb.patient_id,
        tpb.treatment_plan_date,
        tpb.treatment_plan_status,
        tpb.heading,
        tpb.note,
        tpb.entry_user_id,
        tpb.presenter_user_id,
        tpb.signature,
        tpb.signature_practice,
        tpb.responsible_party_id,
        tpb.date_time_signed_patient,
        tpb.date_time_signed_practice,
        tpb.mobile_app_device_id,
        
        -- Aggregated procedure information
        coalesce(tpa.procedure_count, 0) as procedure_count,
        tpa.total_planned_amount,
        tpa.procedure_codes,
        tpa.procedure_categories,
        coalesce(tpa.completed_procedure_count, 0) as completed_procedure_count,
        coalesce(tpa.completed_amount, 0) as completed_amount,
        tpa.earliest_procedure_date,
        tpa.latest_procedure_date,
        tpa.primary_provider_id,
        tpa.primary_clinic_id,
        
        -- Status descriptions
        case tpb.treatment_plan_status
            when 0 then 'Active'
            when 1 then 'Inactive'
            when 2 then 'Saved'
            else 'Unknown'
        end as treatment_plan_status_description,
        
        -- Completion percentage
        case 
            when coalesce(tpa.procedure_count, 0) > 0 
            then round(coalesce(tpa.completed_procedure_count, 0)::numeric / tpa.procedure_count * 100, 2)
            else 0
        end as completion_percentage,
        
        -- Amount remaining
        coalesce(tpa.total_planned_amount, 0) - coalesce(tpa.completed_amount, 0) as remaining_amount,
        
        -- Timeline analysis
        case
            when tpb.treatment_plan_date is null then null
            when tpa.earliest_procedure_date is null 
            then current_date - tpb.treatment_plan_date
            else tpa.earliest_procedure_date - tpb.treatment_plan_date
        end as days_to_first_procedure,
        
        case
            when tpa.latest_procedure_date is not null
            then current_date - tpa.latest_procedure_date
            else current_date - tpb.treatment_plan_date
        end as days_since_last_activity,
        
        -- Acceptance flags
        case
            when tpb.date_time_signed_patient is not null then true
            else false
        end as is_patient_signed,
        
        case
            when tpb.date_time_signed_practice is not null then true
            else false
        end as is_practice_signed,
        
        case
            when tpb.date_time_signed_patient is not null 
            and tpb.date_time_signed_practice is not null
            then true
            else false
        end as is_fully_accepted,
        
        -- Status flags
        case
            when tpb.treatment_plan_status = 0 
            and coalesce(tpa.completed_procedure_count, 0) = 0
            then true
            else false
        end as is_not_started,
        
        case
            when coalesce(tpa.completed_procedure_count, 0) > 0
            and coalesce(tpa.completed_procedure_count, 0) < coalesce(tpa.procedure_count, 0)
            then true
            else false
        end as is_in_progress,
        
        case
            when coalesce(tpa.completed_procedure_count, 0) = coalesce(tpa.procedure_count, 0)
            and coalesce(tpa.procedure_count, 0) > 0
            then true
            else false
        end as is_completed,
        
        -- Delay categorization
        case
            when current_date - tpb.treatment_plan_date > 180 then 'Very Delayed'
            when current_date - tpb.treatment_plan_date > 90 then 'Delayed'
            when current_date - tpb.treatment_plan_date > 30 then 'Recent'
            else 'Current'
        end as timeline_status,
        
        -- Recovery potential for revenue lost analysis
        case
            when tpb.treatment_plan_status != 0 then 'None'  -- Inactive/Saved
            when current_date - tpb.treatment_plan_date > 180 then 'Low'
            when current_date - tpb.treatment_plan_date > 90 then 'Medium'
            when coalesce(tpa.procedure_count, 0) > 0 then 'High'
            else 'Medium'
        end as recovery_potential
        
    from treatment_plan_base tpb
    left join treatment_plan_aggregated tpa
        on tpb.treatment_plan_id = tpa.treatment_plan_id
),

-- 6. Final selection with standardized metadata
final as (
    select
        tpe.*,
        -- Standardized intermediate metadata
        {{ standardize_intermediate_metadata(
            primary_source_alias='tpb',
            source_metadata_fields=['_loaded_at', '_created_at', '_updated_at']
        ) }}
    from treatment_plan_enhanced tpe
    inner join treatment_plan_base tpb
        on tpe.treatment_plan_id = tpb.treatment_plan_id
)

select * from final

