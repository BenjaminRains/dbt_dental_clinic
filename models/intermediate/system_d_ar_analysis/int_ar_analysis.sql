{{ config(
    materialized='table',
    schema='intermediate',
    unique_key='patient_id'
) }}

with PatientBalances as (
    select
        patient_id,
        total_balance as total_ar_balance,
        balance_0_30_days,
        balance_31_60_days,
        balance_61_90_days,
        balance_over_90_days,
        insurance_estimate as estimated_insurance_ar,
        payment_plan_due as payment_plan_balance,
        patient_status
    from {{ ref('stg_opendental__patient') }}
),

ProcedureFinancials as (
    select
        patient_id,
        sum(procedure_fee) as total_procedure_fees,
        sum(discount) as total_procedure_discounts,
        sum(discount_plan_amount) as total_discount_plan_amount,
        sum(tax_amount) as total_procedure_tax,
        count(distinct case 
            when procedure_status = 2 then procedure_id 
        end) as completed_procedures_count,
        count(distinct case 
            when procedure_status = 1 then procedure_id 
        end) as treatment_planned_procedures_count,
        count(distinct case 
            when procedure_status = 6 then procedure_id 
        end) as ordered_planned_procedures_count,
        count(distinct case 
            when procedure_status = 8 then procedure_id 
        end) as in_progress_procedures_count,
        count(distinct case 
            when is_locked = 1 then procedure_id 
        end) as locked_procedures_count
    from {{ ref('stg_opendental__procedurelog') }}
    group by 1
),

InsuranceAR as (
    select
        cp.patient_id,
        count(distinct case 
            when c.claim_status not in ('P', 'R') 
            then c.claim_id 
        end) as pending_claims_count,
        sum(case 
            when c.claim_status not in ('P', 'R') 
            then c.claim_fee 
            else 0 
        end) as pending_claims_amount,
        count(distinct case 
            when c.claim_status = 'D' 
            then c.claim_id 
        end) as denied_claims_count,
        sum(case 
            when c.claim_status = 'D' 
            then c.claim_fee 
            else 0 
        end) as denied_claims_amount,
        c.claim_status
    from {{ ref('stg_opendental__claimproc') }} cp
    left join {{ ref('stg_opendental__claim') }} c
        on cp.claim_id = c.claim_id
    group by 1, c.claim_status
),

PaymentSummary as (
    select
        patient_id,
        max(payment_date) as last_payment_date,
        sum(payment_amount) as total_payment_amount,
        sum(merchant_fee) as total_merchant_fees,
        count(distinct case 
            when is_split_flag = true 
            then payment_id 
        end) as split_payment_count,
        count(distinct case 
            when is_recurring_cc_flag = true 
            then payment_id 
        end) as recurring_cc_payment_count,
        count(distinct case 
            when is_cc_completed_flag = true 
            then payment_id 
        end) as completed_cc_payment_count,
        count(distinct case 
            when payment_status = 1 
            then payment_id 
        end) as completed_payment_count,
        count(distinct case 
            when payment_status = 2 
            then payment_id 
        end) as voided_payment_count,
        count(distinct case 
            when payment_source = 1 
            then payment_id 
        end) as cash_payment_count,
        count(distinct case 
            when payment_source = 2 
            then payment_id 
        end) as check_payment_count,
        count(distinct case 
            when payment_source = 3 
            then payment_id 
        end) as credit_card_payment_count,
        count(distinct case 
            when payment_source = 4 
            then payment_id 
        end) as patient_insurance_payment_count
    from {{ ref('stg_opendental__payment') }}
    group by 1
),

PaymentActivity as (
    select
        ps.patient_id,
        ps.last_payment_date,
        ps.total_payment_amount,
        ps.total_merchant_fees,
        ps.split_payment_count,
        ps.recurring_cc_payment_count,
        ps.completed_cc_payment_count,
        ps.completed_payment_count,
        ps.voided_payment_count,
        ps.cash_payment_count,
        ps.check_payment_count,
        ps.credit_card_payment_count,
        ps.patient_insurance_payment_count,
        sum(case 
            when ps2.is_discount_flag = false 
            then ps2.split_amount 
            else 0 
        end) as total_payments,
        sum(case 
            when ps2.is_discount_flag = true 
            then ps2.split_amount 
            else 0 
        end) as total_discounts,
        count(distinct case 
            when ps2.payplan_id is not null 
            then ps2.payplan_id 
        end) as active_payment_plans,
        sum(case 
            when ps2.payplan_id is not null 
            then ps2.split_amount 
            else 0 
        end) as payment_plan_payments,
        count(distinct case 
            when ps2.unearned_type is not null 
            then ps2.paysplit_id 
        end) as unearned_income_count,
        sum(case 
            when ps2.unearned_type is not null 
            then ps2.split_amount 
            else 0 
        end) as unearned_income_amount
    from PaymentSummary ps
    left join {{ ref('stg_opendental__paysplit') }} ps2
        on ps.patient_id = ps2.patient_id
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
),

InsurancePayments as (
    select
        c.patient_id,
        max(cp.check_date) as last_insurance_payment_date,
        sum(cp.check_amount) as total_insurance_payments,
        count(distinct cp.claim_payment_id) as insurance_payment_count
    from {{ ref('stg_opendental__claimpayment') }} cp
    left join {{ ref('stg_opendental__claim') }} c
        on cp.claim_payment_id = c.claim_id
    group by 1
),

StatementActivity as (
    select
        patient_id,
        max(date_sent) as last_statement_date,
        count(*) as statement_count,
        sum(balance_total) as total_statement_balance,
        sum(insurance_estimate) as total_statement_insurance_estimate,
        count(distinct case when is_receipt = 1 then statement_id end) as receipt_count,
        count(distinct case when is_invoice = 1 then statement_id end) as invoice_count
    from {{ ref('stg_opendental__statement') }}
    where is_sent = true
    group by 1
),

Adjustments as (
    select
        patient_id,
        max(adjustment_category) as adjustment_category,
        sum(case 
            when adjustment_amount < 0 
            then abs(adjustment_amount) 
            else 0 
        end) as write_off_amount,
        sum(case 
            when adjustment_amount > 0 
            then adjustment_amount 
            else 0 
        end) as credit_amount,
        count(distinct case 
            when adjustment_category = 'insurance_writeoff' 
            then adjustment_id 
        end) as insurance_writeoff_count,
        sum(case 
            when adjustment_category = 'insurance_writeoff' 
            then abs(adjustment_amount) 
            else 0 
        end) as insurance_writeoff_amount,
        count(distinct case 
            when is_procedure_adjustment = 1 
            then adjustment_id 
        end) as procedure_adjustment_count,
        count(distinct case 
            when is_retroactive_adjustment = 1 
            then adjustment_id 
        end) as retroactive_adjustment_count,
        count(distinct case 
            when adjustment_category = 'provider_discount' 
            then adjustment_id 
        end) as provider_discount_count,
        sum(case 
            when adjustment_category = 'provider_discount' 
            then abs(adjustment_amount) 
            else 0 
        end) as provider_discount_amount
    from {{ ref('stg_opendental__adjustment') }}
    group by 1
),

ClaimTracking as (
    select
        c.patient_id,
        count(distinct case 
            when ct.tracking_type = 'S' 
            then ct.claim_id 
        end) as submitted_claims_count,
        count(distinct case 
            when ct.tracking_type = 'R' 
            then ct.claim_id 
        end) as received_claims_count,
        count(distinct case 
            when ct.tracking_type = 'D' 
            then ct.claim_id 
        end) as tracking_denied_claims_count,
        min(case 
            when ct.tracking_type = 'S' 
            then ct.entry_timestamp 
        end) as first_claim_submission_date,
        max(case 
            when ct.tracking_type = 'S' 
            then ct.entry_timestamp 
        end) as last_claim_submission_date,
        min(case 
            when ct.tracking_type = 'R' 
            then ct.entry_timestamp 
        end) as first_claim_received_date,
        max(case 
            when ct.tracking_type = 'R' 
            then ct.entry_timestamp 
        end) as last_claim_received_date,
        min(case 
            when ct.tracking_type = 'D' 
            then ct.entry_timestamp 
        end) as first_claim_denied_date,
        max(case 
            when ct.tracking_type = 'D' 
            then ct.entry_timestamp 
        end) as last_claim_denied_date
    from {{ ref('stg_opendental__claimtracking') }} ct
    left join {{ ref('stg_opendental__claim') }} c
        on ct.claim_id = c.claim_id
    group by 1
),

-- Add definition lookups
Definitions as (
    select
        definition_id,
        category_id,
        item_name,
        item_value,
        item_order,
        item_color
    from {{ ref('stg_opendental__definition') }}
),

-- Join definitions for descriptive values
EnrichedData as (
    select
        pb.*, -- Patient balance metrics including total AR and aging buckets
        pf.*, -- Procedure financial metrics including completed and planned procedures
        ia.*, -- Insurance AR metrics including pending and denied claims
        pa.*, -- Payment activity metrics including payment counts and amounts
        ip.*, -- Insurance payment metrics including payment dates and amounts
        sa.*, -- Statement activity metrics including statement counts and dates
        adj.*, -- Adjustment metrics including write-offs and credits
        ct.*, -- Claim tracking metrics including submission and denial dates
        -- Patient Status Description
        def_patient.item_name as patient_status_description,
        -- Claim Status Description
        def_claim.item_name as claim_status_description,
        -- Adjustment Type Descriptions
        def_adj.item_name as adjustment_type_description,
        current_timestamp as created_at,
        current_timestamp as updated_at
    from PatientBalances pb
    left join ProcedureFinancials pf
        on pb.patient_id = pf.patient_id
    left join InsuranceAR ia
        on pb.patient_id = ia.patient_id
    left join PaymentActivity pa
        on pb.patient_id = pa.patient_id
    left join InsurancePayments ip
        on pb.patient_id = ip.patient_id
    left join StatementActivity sa
        on pb.patient_id = sa.patient_id
    left join Adjustments adj
        on pb.patient_id = adj.patient_id
    left join ClaimTracking ct
        on pb.patient_id = ct.patient_id
    -- Join with definitions for descriptions
    left join Definitions def_patient
        on def_patient.category_id = 1 
        and def_patient.item_value = pb.patient_status::text
    left join Definitions def_claim
        on def_claim.category_id = 2 
        and def_claim.item_value = ia.claim_status::text
    left join Definitions def_adj
        on def_adj.category_id = 3 
        and def_adj.item_value = adj.adjustment_category::text
)

select * from EnrichedData