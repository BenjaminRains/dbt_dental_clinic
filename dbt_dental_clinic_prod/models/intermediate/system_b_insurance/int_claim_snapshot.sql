{{ config(        materialized='table',
        
        unique_key='claim_snapshot_id',
        on_schema_change='fail',
        indexes=[
            {'columns': ['claim_snapshot_id'], 'unique': true},
            {'columns': ['claim_id']},
            {'columns': ['patient_id']},
            {'columns': ['entry_timestamp']},
            {'columns': ['_updated_at']}
        ]) }}

/*
    Intermediate model for claim snapshots
    Part of System B: Insurance & Claims Processing
    
    This model:
    1. Tracks claim states at various points in time
    2. Provides historical context for claim changes
    3. Integrates with int_claim_details for comprehensive claim history
    4. Supports insurance payment tracking and analysis
    
    Business Logic Features:
    - Snapshot Triggers: Captures key claim lifecycle events (INITIAL, RESUBMIT, PAYMENT, ADJUSTMENT, DENIAL, APPEAL)
    - Variance Analysis: Compares estimated vs actual payments and write-offs
    - Temporal Analysis: Calculates days between snapshot and payment events
    - Data Deduplication: Ensures unique claim_snapshot_id values
    
    Data Quality Notes:
    - Deduplication applied to handle multiple claim procedures per snapshot
    - Payment variance calculations handle null values appropriately
    - Temporal calculations account for data quality issues (payment before snapshot)
    
    Performance Considerations:
    - Table materialization for complex joins and calculations
    - Indexed on claim_id, patient_id, and temporal fields
    - Row number partitioning for efficient deduplication
*/

-- 1. Source CTEs (multiple sources)
with source_claim_snapshots as (
    select * from {{ ref('stg_opendental__claimsnapshot') }}
),

source_claim_procedures as (
    select
        claim_procedure_id,
        claim_id,
        procedure_id,
        patient_id,
        plan_id,
        insurance_payment_amount,
        write_off,
        allowed_override,
        status,
        claim_adjustment_reason_codes
    from {{ ref('stg_opendental__claimproc') }}
),

source_claim_tracking as (
    select
        claim_id,
        claim_tracking_id, 
        tracking_type,
        entry_timestamp,
        tracking_note
    from {{ ref('int_claim_tracking') }}
),

source_claim_details as (
    select
        claim_id,
        patient_id,
        procedure_id,
        procedure_code,
        billed_amount,
        allowed_amount,
        claim_status,
        claim_type
    from {{ ref('int_claim_details') }}
),

source_claim_payments as (
    select
        claim_id,
        procedure_id,
        paid_amount,
        check_date,
        write_off_amount
    from {{ ref('int_claim_payments') }}
),

-- 2. Calculation/Aggregation CTEs
claim_tracking_ranked as (
    select
        claim_id,
        claim_tracking_id, 
        tracking_type,
        entry_timestamp,
        tracking_note,
        row_number() over(
            partition by claim_id, date_trunc('day', entry_timestamp)
            order by entry_timestamp desc
        ) as tracking_rank
    from source_claim_tracking
),

claim_payments_ranked as (
    select
        claim_id,
        procedure_id,
        paid_amount,
        check_date,
        write_off_amount,
        row_number() over(
            partition by claim_id, procedure_id 
            order by check_date desc
        ) as payment_rank
    from source_claim_payments
),

most_recent_payments as (
    select
        claim_id,
        procedure_id,
        paid_amount,
        check_date,
        write_off_amount
    from claim_payments_ranked
    where payment_rank = 1
),

-- 3. Business Logic CTEs
snapshot_validation as (
    select
        cs.claim_snapshot_id,
        cs.claim_procedure_id,
        cs.claim_type,
        cs.write_off_amount,
        cs.insurance_payment_estimate,
        cs.fee_amount,
        cs.entry_timestamp,
        cs.snapshot_trigger,
        cp.claim_id,
        cp.procedure_id,
        cp.patient_id,
        cp.plan_id,
        row_number() over(
            partition by cs.claim_snapshot_id 
            order by cp.claim_id
        ) as snapshot_rank
    from source_claim_snapshots cs
    inner join source_claim_procedures cp 
        on cs.claim_procedure_id = cp.claim_procedure_id
),

snapshot_deduplication as (
    select
        claim_snapshot_id,
        claim_procedure_id,
        claim_type,
        write_off_amount,
        insurance_payment_estimate,
        fee_amount,
        entry_timestamp,
        snapshot_trigger,
        claim_id,
        procedure_id,
        patient_id,
        plan_id
    from snapshot_validation
    where snapshot_rank = 1
),

-- 4. Integration CTE (joins everything together)
snapshot_integration as (
    select
        -- Primary identification
        vs.claim_snapshot_id,
        vs.claim_procedure_id,
        
        -- Foreign keys
        vs.claim_id,
        vs.procedure_id,
        vs.patient_id,
        vs.plan_id,
        
        -- Tracking integration
        ct.claim_tracking_id,
        ct.tracking_type,
        ct.tracking_note,
        
        -- Procedure information
        cd.procedure_code,
        coalesce(cd.claim_type, vs.claim_type) as claim_type,
        cd.claim_status,
        
        -- Snapshot details
        vs.claim_type as snapshot_claim_type,
        vs.write_off_amount as estimated_write_off,
        vs.insurance_payment_estimate,
        vs.fee_amount,
        vs.entry_timestamp,
        vs.snapshot_trigger,
        
        -- Actual claim procedure details
        cp.insurance_payment_amount as actual_payment_amount,
        cp.write_off as actual_write_off,
        cp.allowed_override as actual_allowed_amount,
        cp.status as claim_procedure_status,
        cp.claim_adjustment_reason_codes,
        
        -- Payment information
        mrp.paid_amount as most_recent_payment,
        mrp.check_date as most_recent_payment_date,
        
        -- Calculated variance analysis
        (cp.insurance_payment_amount - vs.insurance_payment_estimate) as payment_variance,
        (cp.write_off - vs.write_off_amount) as write_off_variance,
        
        -- Temporal calculations
        case 
            when mrp.check_date is null then null
            when mrp.check_date < vs.entry_timestamp then 0
            else extract(epoch from (mrp.check_date - vs.entry_timestamp))/86400 
        end as days_to_payment
    from snapshot_deduplication vs
    left join source_claim_procedures cp
        on vs.claim_procedure_id = cp.claim_procedure_id
    left join claim_tracking_ranked ct
        on vs.claim_id = ct.claim_id
        and date_trunc('day', vs.entry_timestamp) = date_trunc('day', ct.entry_timestamp)
        and ct.tracking_rank = 1
    left join source_claim_details cd
        on vs.claim_id = cd.claim_id
        and vs.procedure_id = cd.procedure_id
    left join most_recent_payments mrp
        on vs.claim_id = mrp.claim_id
        and vs.procedure_id = mrp.procedure_id
),

-- 5. Final filtering/validation
final as (
    select
        -- Primary identification
        claim_snapshot_id,
        claim_procedure_id,
        
        -- Foreign keys
        claim_id,
        procedure_id,
        patient_id,
        plan_id,
        
        -- Tracking information
        claim_tracking_id,
        tracking_type,
        tracking_note,
        
        -- Procedure information
        procedure_code,
        claim_type,
        claim_status,
        
        -- Snapshot details
        snapshot_claim_type,
        estimated_write_off,
        insurance_payment_estimate,
        fee_amount,
        entry_timestamp,
        snapshot_trigger,
        
        -- Actual amounts
        actual_payment_amount,
        actual_write_off,
        actual_allowed_amount,
        claim_procedure_status,
        claim_adjustment_reason_codes,
        
        -- Payment information
        most_recent_payment,
        most_recent_payment_date,
        
        -- Variance analysis
        payment_variance,
        write_off_variance,
        
        -- Temporal analysis
        days_to_payment,
        
        -- Generate metadata for this intermediate model
        current_timestamp as _loaded_at,
        entry_timestamp as _created_at,
        entry_timestamp as _updated_at,
        0 as _created_by,
        current_timestamp as _transformed_at
    from snapshot_integration
    where claim_snapshot_id is not null
)

select * from final
