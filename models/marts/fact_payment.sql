{{
    config(
        materialized='table',
        schema='marts',
        unique_key='payment_id'
    )
}}

/*
Fact table for payment transactions and financial activity.
This model captures all payment-related activities including patient payments,
insurance payments, adjustments, and refunds for comprehensive financial analysis.

Key features:
- Complete payment transaction tracking
- Payment method and source analysis
- Split payments and allocations
- Adjustment and write-off tracking
- Revenue cycle performance
*/

with PaymentBase as (
    select * from {{ ref('stg_opendental__payment') }}
),

PaymentSplits as (
    select 
        payment_id,
        count(*) as split_count,
        sum(split_amount) as total_split_amount,
        array_agg(distinct provider_id::text) as split_provider_ids,
        array_agg(distinct procedure_id::text) as split_procedure_ids,
        array_agg(distinct patient_id::text) as split_patient_ids
    from {{ ref('stg_opendental__paysplit') }}
    group by payment_id
),

/*
TODO - PAYMENT PLAN CHARGE INTEGRATION:
The PaymentPlans CTE has been temporarily removed because stg_opendental__payplancharge
staging model does not exist. A new approach needs to be developed for incorporating
payment plan charge information into the payment fact table.

Potential approaches:
1. Create stg_opendental__payplancharge staging model from payplancharge source table
2. Integrate payment plan data through the existing paysplit model (PayPlanChargeNum)
3. Create a dedicated payment plan fact table that references payments
4. Use payplan source table to track payment plan relationships

When implementing payment plan charge integration, restore the following structure:
- payment_id linkage to connect charges to payments
- payment_plan_id for plan identification
- payment_plan_charge_id for specific charge tracking
- Consider charge amounts, due dates, and payment allocations
*/

Final as (
    select
        -- Primary Key
        pb.payment_id,

        -- Foreign Keys
        pb.patient_id,
        pb.provider_id,
        pb.clinic_id,
        pb.payment_type_id,
        pb.deposit_id,
        null as payment_plan_id,  -- TODO: Restore when payplancharge integration is available

        -- Date and Time
        pb.payment_date,
        pb.date_entry as entry_date,
        pb.receipt_date,
        extract(year from pb.payment_date) as payment_year,
        extract(month from pb.payment_date) as payment_month,
        extract(quarter from pb.payment_date) as payment_quarter,
        extract(dow from pb.payment_date) as payment_day_of_week,

        -- Payment Details
        case pb.payment_type
            when 0 then 'Patient'
            when 1 then 'Insurance'
            when 2 then 'Partial'
            when 3 then 'PrePayment'
            when 4 then 'Adjustment'
            when 5 then 'Refund'
            else 'Unknown'
        end as payment_type,

        case pb.payment_method
            when 0 then 'Check'
            when 1 then 'Cash'
            when 2 then 'Credit Card'
            when 3 then 'Electronic'
            when 4 then 'Online'
            when 5 then 'Auto'
            else 'Other'
        end as payment_method,

        pb.payment_amount,
        pb.payment_note,
        pb.check_number,
        pb.bank_branch,
        pb.is_recurring,
        pb.external_id,

        -- Payment Source Information
        case pb.payment_source
            when 0 then 'User'
            when 1 then 'Practice'
            when 2 then 'Insurance'
            when 3 then 'Online'
            when 4 then 'Kiosk'
            else 'Unknown'
        end as payment_source,

        -- Processing Information
        pb.processing_status,
        pb.receipt_number,
        pb.external_reference,
        pb.payment_software,

        -- Split Information
        ps.split_count,
        ps.total_split_amount,
        ps.split_provider_ids,
        ps.split_procedure_ids,
        ps.split_patient_ids,
        
        -- Validation flags
        case when pb.payment_amount = ps.total_split_amount then true else false end as splits_match_payment,
        case when ps.split_count > 1 then true else false end as has_multiple_splits,

        -- Financial Categorization
        case 
            when pb.payment_amount > 0 then 'Income'
            when pb.payment_amount < 0 then 'Refund'
            else 'Zero'
        end as payment_direction,

        case 
            when pb.payment_amount between 0 and 50 then 'Small'
            when pb.payment_amount between 50 and 200 then 'Medium'
            when pb.payment_amount between 200 and 1000 then 'Large'
            when pb.payment_amount > 1000 then 'Very Large'
            else 'Negative'
        end as payment_size_category,

        -- Timing Analysis
        case 
            when pb.payment_date = pb.date_entry then 'Same Day'
            when pb.payment_date < pb.date_entry then 'Backdated'
            when pb.payment_date > pb.date_entry then 'Future Dated'
        end as payment_timing,

        -- Boolean Flags
        case when pb.payment_type = 1 then true else false end as is_insurance_payment,
        case when pb.payment_type = 0 then true else false end as is_patient_payment,
        case when pb.payment_type = 4 then true else false end as is_adjustment,
        case when pb.payment_type = 5 then true else false end as is_refund,
        case when pb.payment_amount = 0 then true else false end as is_zero_payment,
        case when pb.is_recurring then true else false end as is_recurring_payment,

        -- Metadata
        pb._created_at,
        pb._updated_at,
        current_timestamp as _loaded_at

    from PaymentBase pb
    left join PaymentSplits ps
        on pb.payment_id = ps.payment_id
    -- TODO: Restore when payplancharge integration is available:
    -- left join PaymentPlans pp
    --     on pb.payment_id = pp.payment_id
)

select * from Final
