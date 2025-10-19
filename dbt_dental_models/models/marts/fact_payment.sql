{{
    config(
        materialized='table',
        schema='marts',
        unique_key='payment_id',
        on_schema_change='fail',
        indexes=[
            {'columns': ['payment_id'], 'unique': true},
            {'columns': ['patient_id']},
            {'columns': ['provider_id']},
            {'columns': ['payment_date']},
            {'columns': ['_updated_at']}
        ]
    )
}}

/*
Fact table for payment transactions and financial activity.
Part of System 2: Revenue Cycle Management

This model:
1. Captures all payment-related activities including patient payments, insurance payments, adjustments, and refunds
2. Provides comprehensive financial analysis capabilities for revenue cycle management
3. Enables payment method and source analysis for operational insights

Business Logic Features:
- Complete payment transaction tracking with split payment allocation
- Payment method and source categorization for operational analysis
- Financial categorization (Income/Refund/Zero) and size classification
- Timing analysis for payment processing efficiency
- Validation flags for data quality assurance

Key Metrics:
- Payment amounts and directions (Income vs Refund)
- Payment timing analysis (Same Day, Backdated, Future Dated)
- Split payment validation and allocation tracking
- Payment method distribution and source tracking

Data Quality Notes:
- Payment plan functionality not used by clinic (fields set to null)
- Split amounts validated against payment amounts with mismatch flag
- Zero payments identified and flagged for analysis

Performance Considerations:
- Indexed on payment_id (unique), patient_id, provider_id, payment_date
- Payment splits aggregated to avoid N+1 query patterns
- Date extractions performed at mart level for analytical queries

Dependencies:
- int_payment_split: Intermediate model with payment split categorization, validation, and payment header fields
*/

-- 1. Source data retrieval from intermediate layer
with source_payment_splits as (
    select * from {{ ref('int_payment_split') }}
),

-- 2. Payment splits aggregation from intermediate
payment_splits as (
    select 
        payment_id,
        patient_id,
        payment_date,
        payment_type_id,
        payment_notes,
        payment_status,
        process_status,
        is_split,
        is_recurring_cc,
        merchant_fee,
        is_cc_completed,
        recurring_charge_date,
        
        -- Aggregated split information
        count(*) as split_count,
        sum(split_amount) as total_split_amount,
        array_agg(distinct provider_id::text order by provider_id::text) filter (where provider_id is not null) as split_provider_ids,
        array_agg(distinct procedure_id::text order by procedure_id::text) filter (where procedure_id is not null) as split_procedure_ids,
        array_agg(distinct patient_id::text order by patient_id::text) as split_patient_ids,
        
        -- Split type aggregations
        count(*) filter (where split_type = 'DISCOUNT') as discount_split_count,
        count(*) filter (where split_type = 'UNEARNED_REVENUE') as unearned_split_count,
        count(*) filter (where split_type = 'TREATMENT_PLAN_PREPAYMENT') as treatment_plan_split_count,
        count(*) filter (where split_type = 'INCOME_TRANSFER') as transfer_split_count,
        
        -- Metadata
        max(_loaded_at) as _loaded_at,
        max(_created_at) as _created_at,
        max(_updated_at) as _updated_at,
        max(_created_by) as _created_by
        
    from source_payment_splits
    group by 
        payment_id,
        patient_id,
        payment_date,
        payment_type_id,
        payment_notes,
        payment_status,
        process_status,
        is_split,
        is_recurring_cc,
        merchant_fee,
        is_cc_completed,
        recurring_charge_date
),


-- 4. Business logic and calculations
payment_calculated as (

    select
        -- Primary key
        ps.payment_id,

        -- Foreign keys
        ps.patient_id,
        null as provider_id,  -- Provider not directly associated with payment header
        ps.clinic_id,
        ps.payment_type_id,
        ps.deposit_id,
        null as payment_plan_id,  -- Not used by clinic

        -- Date and time
        ps.payment_date,
        ps.payment_entry_date as entry_date,
        null as receipt_date,  -- Not available in source
        extract(year from ps.payment_date) as payment_year,
        extract(month from ps.payment_date) as payment_month,
        extract(quarter from ps.payment_date) as payment_quarter,
        extract(dow from ps.payment_date) as payment_day_of_week,

        -- Payment details
        case ps.payment_type_id
            when 0 then 'Patient'
            when 1 then 'Insurance'
            when 2 then 'Partial'
            when 3 then 'PrePayment'
            when 4 then 'Adjustment'
            when 5 then 'Refund'
            else 'Unknown'
        end as payment_type,

        'Unknown' as payment_method,  -- Payment method not available in source

        ps.payment_amount,
        ps.payment_notes as payment_note,
        ps.check_number,
        ps.bank_branch,
        ps.is_recurring_cc as is_recurring,
        ps.payment_external_id as external_id,

        -- Payment source information
        case when ps.payment_source then 'Practice' else 'External' end as payment_source,

        -- Processing information
        ps.process_status as processing_status,
        null as receipt_number,  -- Not available in source
        null as external_reference,  -- Not available in source
        null as payment_software,  -- Not available in source

        -- Split information from intermediate aggregation
        ps.split_count,
        ps.total_split_amount,
        ps.split_provider_ids,
        ps.split_procedure_ids,
        ps.split_patient_ids,
        
        -- Validation flags
        case when ps.payment_amount = ps.total_split_amount then true else false end as splits_match_payment,
        case when ps.split_count > 1 then true else false end as has_multiple_splits,

        -- Financial categorization
        case 
            when ps.payment_amount > 0 then 'Income'
            when ps.payment_amount < 0 then 'Refund'
            else 'Zero'
        end as payment_direction,

        case 
            when ps.payment_amount between 0 and 50 then 'Small'
            when ps.payment_amount between 50 and 200 then 'Medium'
            when ps.payment_amount between 200 and 1000 then 'Large'
            when ps.payment_amount > 1000 then 'Very Large'
            else 'Negative'
        end as payment_size_category,

        -- Timing analysis
        case 
            when ps.payment_date = ps.payment_entry_date then 'Same Day'
            when ps.payment_date < ps.payment_entry_date then 'Backdated'
            when ps.payment_date > ps.payment_entry_date then 'Future Dated'
        end as payment_timing,

        -- Boolean flags
        case when ps.payment_type_id = 1 then true else false end as is_insurance_payment,
        case when ps.payment_type_id = 0 then true else false end as is_patient_payment,
        case when ps.payment_type_id = 4 then true else false end as is_adjustment,
        case when ps.payment_type_id = 5 then true else false end as is_refund,
        case when ps.payment_amount = 0 then true else false end as is_zero_payment,
        case when ps.is_recurring_cc then true else false end as is_recurring_payment,

        -- Metadata (using intermediate metadata)
        {{ standardize_mart_metadata(
            primary_source_alias='ps',
            source_metadata_fields=['_loaded_at', '_created_at', '_updated_at', '_created_by']
        ) }}

    from payment_splits ps
),

-- 4. Final validation
final as (
    select * from payment_calculated
    -- No additional filtering required - all payment records are valid
)

select * from final