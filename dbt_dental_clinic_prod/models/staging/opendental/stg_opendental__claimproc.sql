{{ config(
    materialized='incremental',
    unique_key='claim_procedure_id',
    
) }}

with source_data as (
    select * from {{ source('opendental', 'claimproc') }}
    where {{ clean_opendental_date('"ProcDate"') }} >= '2023-01-01'
    {% if is_incremental() %}
        and {{ clean_opendental_date('"SecDateTEdit"') }} > (select max(_updated_at) from {{ this }})
    {% endif %}
),

renamed_columns as (
    select
        -- Primary Key
        {{ transform_id_columns([
            {'source': '"ClaimProcNum"', 'target': 'claim_procedure_id'}
        ]) }},
        
        -- Foreign Keys
        {{ transform_id_columns([
            {'source': '"ProcNum"', 'target': 'procedure_id'},
            {'source': '"ClaimNum"', 'target': 'claim_id'},
            {'source': '"PatNum"', 'target': 'patient_id'},
            {'source': '"ProvNum"', 'target': 'provider_id'},
            {'source': '"PlanNum"', 'target': 'plan_id'},
            {'source': '"ClaimPaymentNum"', 'target': 'claim_payment_id'},
            {'source': '"ClinicNum"', 'target': 'clinic_id'},
            {'source': '"InsSubNum"', 'target': 'insurance_subscriber_id'},
            {'source': '"PayPlanNum"', 'target': 'payment_plan_id'},
            {'source': '"ClaimPaymentTracking"', 'target': 'claim_payment_tracking_id'}
        ]) }},
        
        -- Date Fields
        {{ clean_opendental_date('"DateCP"') }} as claim_procedure_date,
        {{ clean_opendental_date('"ProcDate"') }} as procedure_date,
        {{ clean_opendental_date('"DateEntry"') }} as entry_date,
        {{ clean_opendental_date('"DateSuppReceived"') }} as supplemental_received_date,
        {{ clean_opendental_date('"DateInsFinalized"') }} as insurance_finalized_date,
        
        -- Monetary Amounts
        "FeeBilled" as fee_billed,
        "InsPayEst" as insurance_payment_estimate,
        "InsPayAmt" as insurance_payment_amount,
        "DedApplied" as deductible_applied,
        "WriteOff" as write_off,
        "AllowedOverride" as allowed_override,
        "CopayAmt" as copay_amount,
        "PaidOtherIns" as paid_other_insurance,
        "BaseEst" as base_estimate,
        "CopayOverride" as copay_override,
        "DedEst" as deductible_estimate,
        "DedEstOverride" as deductible_estimate_override,
        "InsEstTotal" as insurance_estimate_total,
        "InsEstTotalOverride" as insurance_estimate_total_override,
        "PaidOtherInsOverride" as paid_other_insurance_override,
        "WriteOffEst" as write_off_estimate,
        "WriteOffEstOverride" as write_off_estimate_override,
        
        -- Boolean Fields
        {{ convert_opendental_boolean('"IsTransfer"') }} as is_transfer,
        {{ convert_opendental_boolean('"IsOverpay"') }} as is_overpay,
        {{ convert_opendental_boolean('"NoBillIns"') }} as no_bill_insurance,
        
        -- Integer/Status Fields (cast to smallint to match DDL)
        "Status"::smallint as status,
        "Percentage"::smallint as percentage,
        "PercentOverride"::smallint as percentage_override,
        "LineNumber"::smallint as line_number,
        "PaymentRow" as payment_row,
        
        -- Character Fields
        "Remarks" as remarks,
        "CodeSent" as code_sent,
        "EstimateNote" as estimate_note,
        "ClaimAdjReasonCodes" as claim_adjustment_reason_codes,
        "SecurityHash" as security_hash,
        
        -- Metadata columns
        {{ standardize_metadata_columns(
            created_at_column='"DateEntry"',
            updated_at_column='"SecDateTEdit"',
            created_by_column='"SecUserNumEntry"'
        ) }}
        
    from source_data
)

select * from renamed_columns
