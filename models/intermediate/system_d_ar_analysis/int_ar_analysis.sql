{{ config(
    materialized='table',
    schema='intermediate'
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
        payment_plan_due as payment_plan_balance
    from {{ ref('stg_opendental__patient') }}
),

ProcedureFinancials as (
    select
        patient_id,
        sum(procedure_fee) as total_procedure_fees,
        sum(discount) as total_procedure_discounts,
        sum(discount_plan_amount) as total_discount_plan_amount,
        sum(tax_amount) as total_procedure_tax,
        count(distinct case when procedure_status = 2 then procedure_id end) as completed_procedures_count,
        count(distinct case when procedure_status = 1 then procedure_id end) as treatment_planned_procedures_count,
        count(distinct case when procedure_status = 6 then procedure_id end) as ordered_planned_procedures_count,
        count(distinct case when procedure_status = 8 then procedure_id end) as in_progress_procedures_count,
        count(distinct case when is_locked = 1 then procedure_id end) as locked_procedures_count
    from {{ ref('stg_opendental__procedurelog') }}
    group by 1
),

InsuranceAR as (
    select
        cp.patient_id,
        count(distinct case when c.claim_status not in ('P', 'R') then c.claim_id end) as pending_claims_count,
        sum(case when c.claim_status not in ('P', 'R') then c.claim_fee else 0 end) as pending_claims_amount,
        count(distinct case when c.claim_status = 'D' then c.claim_id end) as denied_claims_count,
        sum(case when c.claim_status = 'D' then c.claim_fee else 0 end) as denied_claims_amount
    from {{ ref('stg_opendental__claimproc') }} cp
    left join {{ ref('stg_opendental__claim') }} c
        on cp.claim_id = c.claim_id
    group by 1
),

PaymentSummary as (
    select
        patient_id,
        max(payment_date) as last_payment_date,
        sum(payment_amount) as total_payment_amount,
        sum(merchant_fee) as total_merchant_fees,
        count(distinct case when is_split_flag = true then payment_id end) as split_payment_count,
        count(distinct case when is_recurring_cc_flag = true then payment_id end) as recurring_cc_payment_count,
        count(distinct case when is_cc_completed_flag = true then payment_id end) as completed_cc_payment_count,
        count(distinct case when payment_status = 1 then payment_id end) as completed_payment_count,
        count(distinct case when payment_status = 2 then payment_id end) as voided_payment_count,
        count(distinct case when payment_source = 1 then payment_id end) as cash_payment_count,
        count(distinct case when payment_source = 2 then payment_id end) as check_payment_count,
        count(distinct case when payment_source = 3 then payment_id end) as credit_card_payment_count,
        count(distinct case when payment_source = 4 then payment_id end) as patient_insurance_payment_count
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
        sum(case when ps2.is_discount_flag = false then ps2.split_amount else 0 end) as total_payments,
        sum(case when ps2.is_discount_flag = true then ps2.split_amount else 0 end) as total_discounts,
        count(distinct case when ps2.payplan_id is not null then ps2.payplan_id end) as active_payment_plans,
        sum(case when ps2.payplan_id is not null then ps2.split_amount else 0 end) as payment_plan_payments,
        count(distinct case when ps2.unearned_type is not null then ps2.paysplit_id end) as unearned_income_count,
        sum(case when ps2.unearned_type is not null then ps2.split_amount else 0 end) as unearned_income_amount
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
        sum(case when adjustment_amount < 0 then abs(adjustment_amount) else 0 end) as write_off_amount,
        sum(case when adjustment_amount > 0 then adjustment_amount else 0 end) as credit_amount,
        count(distinct case when adjustment_category = 'insurance_writeoff' then adjustment_id end) as insurance_writeoff_count,
        sum(case when adjustment_category = 'insurance_writeoff' then abs(adjustment_amount) else 0 end) as insurance_writeoff_amount,
        count(distinct case when is_procedure_adjustment = 1 then adjustment_id end) as procedure_adjustment_count,
        count(distinct case when is_retroactive_adjustment = 1 then adjustment_id end) as retroactive_adjustment_count,
        count(distinct case when adjustment_category = 'provider_discount' then adjustment_id end) as provider_discount_count,
        sum(case when adjustment_category = 'provider_discount' then abs(adjustment_amount) else 0 end) as provider_discount_amount
    from {{ ref('stg_opendental__adjustment') }}
    group by 1
),

ClaimTracking as (
    select
        c.patient_id,
        count(distinct case when ct.tracking_type = 'S' then ct.claim_id end) as submitted_claims_count,
        count(distinct case when ct.tracking_type = 'R' then ct.claim_id end) as received_claims_count,
        count(distinct case when ct.tracking_type = 'D' then ct.claim_id end) as tracking_denied_claims_count,
        min(case when ct.tracking_type = 'S' then ct.entry_timestamp end) as first_claim_submission_date,
        max(case when ct.tracking_type = 'S' then ct.entry_timestamp end) as last_claim_submission_date,
        min(case when ct.tracking_type = 'R' then ct.entry_timestamp end) as first_claim_received_date,
        max(case when ct.tracking_type = 'R' then ct.entry_timestamp end) as last_claim_received_date,
        min(case when ct.tracking_type = 'D' then ct.entry_timestamp end) as first_claim_denied_date,
        max(case when ct.tracking_type = 'D' then ct.entry_timestamp end) as last_claim_denied_date
    from {{ ref('stg_opendental__claimtracking') }} ct
    left join {{ ref('stg_opendental__claim') }} c
        on ct.claim_id = c.claim_id
    group by 1
)

select
    pb.patient_id,
    pb.total_ar_balance,
    pb.balance_0_30_days,
    pb.balance_31_60_days,
    pb.balance_61_90_days,
    pb.balance_over_90_days,
    pb.estimated_insurance_ar,
    pb.payment_plan_balance,
    
    -- Procedure Financials
    pf.total_procedure_fees,
    pf.total_procedure_discounts,
    pf.total_discount_plan_amount,
    pf.total_procedure_tax,
    pf.completed_procedures_count,
    pf.treatment_planned_procedures_count,
    pf.ordered_planned_procedures_count,
    pf.in_progress_procedures_count,
    pf.locked_procedures_count,
    
    -- Insurance AR
    ia.pending_claims_count,
    ia.pending_claims_amount,
    ia.denied_claims_count,
    ia.denied_claims_amount,
    
    -- Payment Activity
    pa.last_payment_date,
    pa.total_payment_amount,
    pa.total_merchant_fees,
    pa.split_payment_count,
    pa.recurring_cc_payment_count,
    pa.completed_cc_payment_count,
    pa.completed_payment_count,
    pa.voided_payment_count,
    pa.cash_payment_count,
    pa.check_payment_count,
    pa.credit_card_payment_count,
    pa.patient_insurance_payment_count,
    pa.total_payments,
    pa.total_discounts,
    pa.active_payment_plans,
    pa.payment_plan_payments,
    pa.unearned_income_count,
    pa.unearned_income_amount,
    
    -- Insurance Payments
    ip.last_insurance_payment_date,
    ip.total_insurance_payments,
    ip.insurance_payment_count,
    
    -- Statements
    sa.last_statement_date,
    sa.statement_count,
    sa.total_statement_balance,
    sa.total_statement_insurance_estimate,
    sa.receipt_count,
    sa.invoice_count,
    
    -- Adjustments
    adj.write_off_amount,
    adj.credit_amount,
    adj.insurance_writeoff_count,
    adj.insurance_writeoff_amount,
    adj.procedure_adjustment_count,
    adj.retroactive_adjustment_count,
    adj.provider_discount_count,
    adj.provider_discount_amount,
    
    -- Claim Tracking
    ct.submitted_claims_count,
    ct.received_claims_count,
    ct.tracking_denied_claims_count,
    ct.first_claim_submission_date,
    ct.last_claim_submission_date,
    ct.first_claim_received_date,
    ct.last_claim_received_date,
    ct.first_claim_denied_date,
    ct.last_claim_denied_date,
    
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