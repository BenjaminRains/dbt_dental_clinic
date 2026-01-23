{{
    config(
        materialized='table',
        schema='marts',
        unique_key=['claim_id', 'procedure_id', 'claim_procedure_id'],
        on_schema_change='fail',
        indexes=[
            {'columns': ['claim_id', 'procedure_id', 'claim_procedure_id'], 'unique': true},
            {'columns': ['patient_id']},
            {'columns': ['claim_date']},
            {'columns': ['insurance_plan_id']},
            {'columns': ['_updated_at']}
        ]
    )
}}

/*
    Fact table for claim transactions and procedures.
    Part of System A: Financial Management
    
    This model:
    1. Provides comprehensive claim-level transaction tracking for insurance billing
    2. Enables revenue cycle management and payment analysis
    3. Supports procedure-level financial reporting and reconciliation
    
    Business Logic Features:
    - Individual claim transaction tracking with procedure-level granularity
    - Payment processing and EOB documentation integration
    - Claim status monitoring across multiple states (submitted, paid, denied, etc.)
    - Financial reconciliation with billing amounts, payments, and write-offs
    
    Key Metrics:
    - Billed amounts vs. paid amounts for collection analysis
    - Payment timing analysis for revenue cycle optimization
    - Write-off tracking for financial performance monitoring
    - EOB documentation completeness for audit compliance
    
    Data Quality Notes:
    - Some claims may have missing payment information (handled with left joins)
    - EOB attachments are optional and may be null for some claims
    - Claim status fields may have different granularity across source systems
    
    Performance Considerations:
    - Indexed on primary keys and common query dimensions (patient_id, provider_id, claim_date)
    - Uses efficient joins with proper key matching
    - Materialized as table for query performance
    
    Dependencies:
    - int_claim_details: Primary claim transaction data
    - int_claim_payments: Payment processing information
    - int_claim_snapshot: Historical claim status snapshots
    - int_claim_tracking: Claim processing workflow tracking
    - int_adjustments: Write-off and adjustment data (for adjustment_write_off_amount field)
    - int_insurance_eob_attachments: EOB documentation metadata
*/

-- 1. Source data retrieval
with source_claims as (
    select * from {{ ref('int_claim_details') }}
),

-- 2. Related dimension lookups
claim_payments as (
    select * from {{ ref('int_claim_payments') }}
),

claim_snapshots as (
    select * from {{ ref('int_claim_snapshot') }}
),

claim_tracking as (
    select * from {{ ref('int_claim_tracking') }}
),

-- Aggregate write-offs from adjustments table by procedure_id
-- Write-offs are tracked in int_adjustments, not in claimproc.WriteOff
procedure_write_offs as (
    select
        procedure_id,
        -- Sum all insurance write-off adjustments for this procedure
        -- adjustment_amount is negative for write-offs, so we use ABS to get positive amount
        coalesce(sum(abs(adjustment_amount)), 0.0) as adjustment_write_off_amount,
        count(*) as write_off_count
    from {{ ref('int_adjustments') }}
    where adjustment_category = 'insurance_writeoff'
        and procedure_id is not null
    group by procedure_id
),

-- 3. Business logic and calculations
claims_calculated as (
    select
        row_number() over (
            partition by sc.claim_id, sc.procedure_id, sc.claim_procedure_id
            order by sc.claim_date desc, cp.check_date desc, cs.entry_timestamp desc
        ) as rn,
        -- Primary Key
        sc.claim_id,
        sc.procedure_id,
        sc.claim_procedure_id,

        -- Foreign Keys
        sc.patient_id,
        sc.insurance_plan_id,
        sc.carrier_id,
        sc.subscriber_id,
        sc.provider_id,

        -- Date Fields
        sc.claim_date,
        cp.check_date,
        cs.entry_timestamp as snapshot_date,
        ct.entry_timestamp as tracking_date,

        -- Claim Status and Type
        sc.claim_status,
        sc.claim_type,
        sc.claim_procedure_status,
        cs.claim_status as snapshot_status,
        ct.tracking_type as tracking_status,

        -- Procedure Details
        sc.procedure_code,
        sc.code_prefix,
        sc.procedure_description,
        sc.abbreviated_description,
        sc.procedure_time,
        sc.procedure_category_id,
        sc.treatment_area,
        sc.is_prosthetic,
        sc.is_hygiene,
        sc.base_units,
        sc.is_radiology,
        sc.no_bill_insurance,
        sc.default_claim_note,
        sc.medical_code,
        sc.diagnostic_codes,

        -- Financial Information
        sc.billed_amount,
        sc.allowed_amount,
        sc.paid_amount,
        sc.write_off_amount,  -- Legacy field from claimproc.WriteOff (typically $0.00)
        coalesce(pwo.adjustment_write_off_amount, 0.0) as adjustment_write_off_amount,  -- Write-offs from adjustments table
        sc.patient_responsibility,
        cp.check_amount,
        cp.payment_type_id,
        cp.is_partial,

        -- EOB Documentation
        cp.eob_attachment_count,
        cp.eob_attachment_ids,
        cp.eob_attachment_file_names,

        -- Insurance Plan Details
        sc.plan_type,
        sc.group_number,
        sc.group_name,
        sc.verification_date,
        sc.benefit_details,
        sc.verification_status,
        sc.effective_date,
        sc.termination_date,

        -- Calculated Fields and Business Logic
        case 
            when sc.paid_amount > 0 then 'Paid'
            when sc.claim_status = 'Denied' then 'Denied'
            when sc.claim_status = 'Submitted' then 'Pending'
            when sc.claim_status = 'Rejected' then 'Rejected'
            else 'Unknown'
        end as payment_status_category,

        case 
            when sc.billed_amount > 0 then 'Billable'
            when sc.no_bill_insurance = true then 'Non-Billable'
            else 'Unknown'
        end as billing_status_category,

        case 
            when cp.check_date is not null and sc.claim_date is not null 
                then cp.check_date - sc.claim_date
            else null
        end as payment_days_from_claim,

        case 
            when coalesce(pwo.adjustment_write_off_amount, 0.0) > 0 then 'Write-off'
            when sc.patient_responsibility > 0 then 'Patient Balance'
            when sc.paid_amount = sc.billed_amount then 'Fully Paid'
            else 'Partial Payment'
        end as payment_completion_status,

        case 
            when cp.eob_attachment_count > 0 then 'Documented'
            when cp.check_amount > 0 then 'Payment Without EOB'
            else 'No Documentation'
        end as eob_documentation_status,

        -- Metadata
        {{ standardize_mart_metadata(
            primary_source_alias='sc',
            source_metadata_fields=['_loaded_at', '_created_at', '_updated_at', '_created_by']
        ) }}

    from source_claims sc
    left join claim_payments cp
        on sc.claim_id = cp.claim_id
        and sc.procedure_id = cp.procedure_id
        and sc.claim_procedure_id = cp.claim_procedure_id
    left join claim_snapshots cs
        on sc.claim_id = cs.claim_id
        and sc.procedure_id = cs.procedure_id
        and sc.claim_procedure_id = cs.claim_procedure_id
    left join claim_tracking ct
        on sc.claim_id = ct.claim_id
    left join procedure_write_offs pwo
        on sc.procedure_id = pwo.procedure_id
    -- EOB attachment data is now included in claim_payments CTE
),

-- 4. Final validation and deduplication
final as (
    select * from claims_calculated
    where rn = 1  -- Keep only the first record for each unique combination
      and claim_id is not null
      and procedure_id is not null
      and claim_procedure_id is not null
)

select * from final