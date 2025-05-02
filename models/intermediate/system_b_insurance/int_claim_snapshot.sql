{{
    config(
        materialized='table',
        schema='intermediate',
        unique_key='claim_snapshot_id',
        cluster_by=['claim_id', 'entry_timestamp']
    )
}}

/*
    Intermediate model for claim snapshots
    Part of System B: Insurance
    
    This model:
    1. Tracks claim states at various points in time
    2. Provides historical context for claim changes
    3. Integrates with int_claim_details for comprehensive claim history
    4. Supports insurance payment tracking and analysis
    5. Links to claim tracking events for full audit trail
    6. Enables variance analysis between estimated and actual payments
    
    Snapshot triggers include:
    - INITIAL: First submission of claim
    - RESUBMIT: When a claim is resubmitted
    - PAYMENT: When payment is received
    - ADJUSTMENT: When claim is adjusted
    - DENIAL: When claim is denied
    - APPEAL: When claim is appealed
*/

with Source as (
    select * from {{ ref('stg_opendental__claimsnapshot') }}
),

ClaimProc as (
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

ClaimTracking as (
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
    from {{ ref('int_claim_tracking') }}
),

ClaimDetails as (
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

-- Get most recent payment for each claim/procedure
ClaimPayments as (
    select
        claim_id,
        procedure_id,
        paid_amount,
        check_date,
        write_off_amount,
        row_number() over(partition by claim_id, procedure_id 
                          order by check_date desc) as payment_rank
    from {{ ref('int_claim_payments') }}
),

MostRecentPayment as (
    select *
    from ClaimPayments
    where payment_rank = 1
),

-- Only include snapshots that have a valid claim procedure ID with complete data
ValidSnapshots as (
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
        cp.plan_id
    from Source cs
    inner join ClaimProc cp on cs.claim_procedure_id = cp.claim_procedure_id
),

-- Deduplicate snapshots to ensure unique claim_snapshot_id values
DedupedSnapshots as (
    select *
    from ValidSnapshots
    qualify row_number() over(partition by claim_snapshot_id order by claim_id) = 1
),

Final as (
    select
        -- Primary Key
        vs.claim_snapshot_id,
        
        -- Foreign Keys
        vs.claim_id,
        vs.procedure_id,
        vs.patient_id,
        vs.plan_id,
        
        -- Link to Tracking
        ct.claim_tracking_id,
        ct.tracking_type,
        ct.tracking_note,
        
        -- Procedure Info from Claim Details
        cd.procedure_code,
        coalesce(cd.claim_type, vs.claim_type) as claim_type, -- Use snapshot claim_type if cd claim_type is null
        cd.claim_status,
        
        -- Claim Details at Snapshot Time
        vs.claim_type as snapshot_claim_type,
        vs.write_off_amount as estimated_write_off,
        vs.insurance_payment_estimate,
        vs.fee_amount,
        vs.entry_timestamp,
        
        -- Actual Payment Details
        cp.insurance_payment_amount as actual_payment_amount,
        cp.write_off as actual_write_off,
        cp.allowed_override as actual_allowed_amount,
        cp.status as claim_procedure_status,
        cp.claim_adjustment_reason_codes,
        
        -- Most Recent Payment
        mrp.paid_amount as most_recent_payment,
        mrp.check_date as most_recent_payment_date,
        
        -- Variance Analysis
        (cp.insurance_payment_amount - vs.insurance_payment_estimate) as payment_variance,
        (cp.write_off - vs.write_off_amount) as write_off_variance,
        
        -- Snapshot Metadata
        vs.snapshot_trigger,
        
        -- Temporal Fields for Time-Series Analysis
        CASE 
            WHEN mrp.check_date IS NULL THEN NULL  -- No payment yet, so days_to_payment is undefined
            WHEN mrp.check_date < vs.entry_timestamp THEN 0  -- Payment date before snapshot (data issue)
            ELSE EXTRACT(EPOCH FROM (mrp.check_date - vs.entry_timestamp))/86400 
        END as days_to_payment,
        
        -- Meta Fields
        vs.entry_timestamp as created_at,
        vs.entry_timestamp as updated_at
    from DedupedSnapshots vs
    left join ClaimProc cp
        on vs.claim_procedure_id = cp.claim_procedure_id
    left join ClaimTracking ct
        on vs.claim_id = ct.claim_id
        and date_trunc('day', vs.entry_timestamp) = date_trunc('day', ct.entry_timestamp)
        and ct.tracking_rank = 1
    left join ClaimDetails cd
        on vs.claim_id = cd.claim_id
        and vs.procedure_id = cd.procedure_id
    left join MostRecentPayment mrp
        on vs.claim_id = mrp.claim_id
        and vs.procedure_id = mrp.procedure_id
)

select * from Final