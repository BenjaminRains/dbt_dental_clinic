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
- stg_opendental__payment: Primary source for payment transaction data
- stg_opendental__paysplit: Payment allocation and split information
*/

-- 1. Source data retrieval
with source_payment as (
    select * from {{ ref('stg_opendental__payment') }}
),

-- 2. Payment splits aggregation
payment_splits as (
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

-- 3. Business logic and calculations
payment_calculated as (

    select
        -- Primary key
        sp.payment_id,

        -- Foreign keys
        sp.patient_id,
        null as provider_id,  -- Provider not directly associated with payment
        sp.clinic_id,
        sp.payment_type_id,
        sp.deposit_id,
        null as payment_plan_id,  -- Not used by clinic

        -- Date and time
        sp.payment_date,
        sp.entry_date,
        null as receipt_date,  -- Not available in source
        extract(year from sp.payment_date) as payment_year,
        extract(month from sp.payment_date) as payment_month,
        extract(quarter from sp.payment_date) as payment_quarter,
        extract(dow from sp.payment_date) as payment_day_of_week,

        -- Payment details
        case sp.payment_type_id
            when 0 then 'Patient'
            when 1 then 'Insurance'
            when 2 then 'Partial'
            when 3 then 'PrePayment'
            when 4 then 'Adjustment'
            when 5 then 'Refund'
            else 'Unknown'
        end as payment_type,

        'Unknown' as payment_method,  -- Payment method not available in source

        sp.payment_amount,
        sp.payment_notes as payment_note,
        sp.check_number,
        sp.bank_branch,
        sp.is_recurring_cc as is_recurring,
        sp.external_id,

        -- Payment source information
        case when sp.payment_source then 'Practice' else 'External' end as payment_source,

        -- Processing information
        sp.process_status as processing_status,
        null as receipt_number,  -- Not available in source
        null as external_reference,  -- Not available in source
        null as payment_software,  -- Not available in source

        -- Split information
        ps.split_count,
        ps.total_split_amount,
        ps.split_provider_ids,
        ps.split_procedure_ids,
        ps.split_patient_ids,
        
        -- Validation flags
        case when sp.payment_amount = ps.total_split_amount then true else false end as splits_match_payment,
        case when ps.split_count > 1 then true else false end as has_multiple_splits,

        -- Financial categorization
        case 
            when sp.payment_amount > 0 then 'Income'
            when sp.payment_amount < 0 then 'Refund'
            else 'Zero'
        end as payment_direction,

        case 
            when sp.payment_amount between 0 and 50 then 'Small'
            when sp.payment_amount between 50 and 200 then 'Medium'
            when sp.payment_amount between 200 and 1000 then 'Large'
            when sp.payment_amount > 1000 then 'Very Large'
            else 'Negative'
        end as payment_size_category,

        -- Timing analysis
        case 
            when sp.payment_date = sp.entry_date then 'Same Day'
            when sp.payment_date < sp.entry_date then 'Backdated'
            when sp.payment_date > sp.entry_date then 'Future Dated'
        end as payment_timing,

        -- Boolean flags
        case when sp.payment_type_id = 1 then true else false end as is_insurance_payment,
        case when sp.payment_type_id = 0 then true else false end as is_patient_payment,
        case when sp.payment_type_id = 4 then true else false end as is_adjustment,
        case when sp.payment_type_id = 5 then true else false end as is_refund,
        case when sp.payment_amount = 0 then true else false end as is_zero_payment,
        case when sp.is_recurring_cc then true else false end as is_recurring_payment,

        -- Metadata
        {{ standardize_mart_metadata(
            primary_source_alias='sp',
            source_metadata_fields=['_loaded_at', '_created_at', '_updated_at', '_created_by']
        ) }}

    from source_payment sp
    left join payment_splits ps
        on sp.payment_id = ps.payment_id
),

-- 4. Final validation
final as (
    select * from payment_calculated
    -- No additional filtering required - all payment records are valid
)

select * from final