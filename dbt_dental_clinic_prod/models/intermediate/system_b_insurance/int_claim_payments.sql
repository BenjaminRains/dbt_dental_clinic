{{ config(        materialized='table',
        
        unique_key='claim_payment_detail_id',
        on_schema_change='fail',
        indexes=[
            {'columns': ['claim_payment_detail_id'], 'unique': true},
            {'columns': ['patient_id']},
            {'columns': ['claim_id']},
            {'columns': ['_updated_at']}
        ]) }}

/*
    Intermediate model for claim_payments
    Part of System B: Insurance & Claims Processing
    
    This model:
    1. Consolidates claim payment details with procedure-level breakdowns
    2. Integrates EOB attachment information for documentation tracking
    3. Provides comprehensive payment reconciliation data
    
    Business Logic Features:
    - Payment allocation: Tracks how insurance payments are allocated across procedures
    - EOB integration: Links explanation of benefits attachments to payments
    - Deduplication: Ensures one record per claim payment detail
    
    Data Quality Notes:
    - Filters to claim payments with valid claim_payment_id
    - Deduplicates using composite key with payment amount priority
    - Includes EOB attachments from 2023+ to match active payment period
    
    Performance Considerations:
    - Uses table materialization due to complex joins and aggregations
    - Indexed on primary key and common lookup fields
*/

with source_claim as (
    select
        claim_id,
        patient_id
    from {{ ref('stg_opendental__claim') }}
),

source_claim_proc as (
    select
        claim_id,
        procedure_id,
        claim_procedure_id,
        claim_payment_id,
        fee_billed as billed_amount,
        allowed_override as allowed_amount,
        insurance_payment_amount as paid_amount,
        write_off,
        copay_amount as patient_responsibility
    from {{ ref('stg_opendental__claimproc') }}
    where claim_payment_id is not null
),

source_claim_payment as (
    select
        claim_payment_id,
        check_amount,
        check_date,
        payment_type_id,
        is_partial
    from {{ ref('stg_opendental__claimpayment') }}
),

eob_attachments_summary as (
    select
        claim_payment_id,
        count(eob_attach_id) as eob_attachment_count,
        array_agg(eob_attach_id) as eob_attachment_ids,
        array_agg(file_name) as eob_attachment_file_names
    from {{ ref('stg_opendental__eobattach') }}
    where created_at >= '2023-01-01' -- Filter to match claim payment date range
    group by claim_payment_id
),

claim_payment_details as (
    select
        c.patient_id,
        cp.claim_id,
        cp.procedure_id,
        cp.claim_procedure_id,
        cp.claim_payment_id,
        cp.billed_amount,
        cp.allowed_amount,
        cp.paid_amount,
        cp.write_off,
        cp.patient_responsibility,
        row_number() over(
            partition by cp.claim_id, cp.procedure_id, cp.claim_procedure_id, cp.claim_payment_id
            order by cp.paid_amount desc
        ) as rn
    from source_claim_proc cp
    inner join source_claim c
        on cp.claim_id = c.claim_id
),

claim_payment_enhanced as (
    select
        -- Generate surrogate key for unique identification
        {{ dbt_utils.generate_surrogate_key(['claim_id', 'procedure_id', 'claim_procedure_id', 'claim_payment_id']) }} as claim_payment_detail_id,
        
        -- Primary identifiers
        dc.claim_id,
        dc.procedure_id,
        dc.claim_procedure_id,
        dc.claim_payment_id,
        dc.patient_id, -- Include patient_id in the output

        -- Financial Information
        dc.billed_amount,
        dc.allowed_amount,
        dc.paid_amount,
        dc.write_off as write_off_amount,
        dc.patient_responsibility,

        -- Payment Details
        cpy.check_amount,
        cpy.check_date,
        cpy.payment_type_id,
        cpy.is_partial,

        -- EOB Attachment Information
        coalesce(eob.eob_attachment_count, 0) as eob_attachment_count,
        eob.eob_attachment_ids,
        eob.eob_attachment_file_names,
        
        -- Meta Fields
        cpy.check_date as created_at,
        cpy.check_date as updated_at

    from claim_payment_details dc
    left join source_claim_payment cpy
        on dc.claim_payment_id = cpy.claim_payment_id
    left join eob_attachments_summary eob
        on dc.claim_payment_id = eob.claim_payment_id
    where dc.rn = 1
),

final as (
    select
        *,
        -- Metadata fields
        current_timestamp as _extracted_at,
        created_at as _created_at,
        coalesce(updated_at, created_at) as _updated_at,
        current_timestamp as _transformed_at
    from claim_payment_enhanced
)

select * from final
