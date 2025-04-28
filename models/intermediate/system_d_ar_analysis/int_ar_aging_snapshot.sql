{{ config(
    materialized='incremental',
    schema='intermediate',
    unique_key=['snapshot_date', 'patient_id']
) }}

/*
    Intermediate model for AR aging snapshots
    Part of System D: AR Analysis
    
    This model:
    1. Creates snapshots of AR aging over time
    2. Tracks changes in aging buckets
    3. Monitors collection efficiency
    4. Builds on int_ar_balance for consistency
    5. Uses shared calculations for payment activity
*/

WITH CurrentSnapshot AS (
    SELECT
        CURRENT_DATE AS snapshot_date,
        patient_id,
        SUM(current_balance) AS total_ar_balance,
        SUM(CASE WHEN aging_bucket = '0-30' THEN current_balance ELSE 0 END) AS balance_0_30_days,
        SUM(CASE WHEN aging_bucket = '31-60' THEN current_balance ELSE 0 END) AS balance_31_60_days,
        SUM(CASE WHEN aging_bucket = '61-90' THEN current_balance ELSE 0 END) AS balance_61_90_days,
        SUM(CASE WHEN aging_bucket = '90+' THEN current_balance ELSE 0 END) AS balance_over_90_days,
        SUM(insurance_pending_amount) AS estimated_insurance_ar,
        SUM(CASE WHEN responsible_party = 'PATIENT' THEN current_balance ELSE 0 END) AS patient_responsibility,
        SUM(CASE WHEN responsible_party = 'INSURANCE' THEN current_balance ELSE 0 END) AS insurance_responsibility,
        COUNT(DISTINCT CASE WHEN current_balance > 0 THEN procedure_id END) AS open_procedures_count,
        COUNT(DISTINCT CASE WHEN claim_id IS NOT NULL THEN claim_id END) AS active_claims_count
    FROM {{ ref('int_ar_balance') }}
    GROUP BY 1, 2
),

PaymentActivity AS (
    SELECT
        patient_id,
        SUM(CASE 
            WHEN payment_date >= CURRENT_DATE - INTERVAL '30 days' 
            THEN payment_amount 
            ELSE 0 
        END) AS payments_30_days,
        SUM(CASE 
            WHEN payment_date >= CURRENT_DATE - INTERVAL '60 days' 
            THEN payment_amount 
            ELSE 0 
        END) AS payments_60_days,
        SUM(CASE 
            WHEN payment_date >= CURRENT_DATE - INTERVAL '90 days' 
            THEN payment_amount 
            ELSE 0 
        END) AS payments_90_days
    FROM {{ ref('int_ar_shared_calculations') }}
    WHERE payment_date IS NOT NULL
    GROUP BY 1
),

CollectionEfficiency AS (
    SELECT
        cs.patient_id,
        CASE 
            WHEN cs.balance_0_30_days + cs.balance_31_60_days + cs.balance_61_90_days + cs.balance_over_90_days > 0
            THEN pa.payments_30_days / NULLIF(cs.balance_0_30_days + cs.balance_31_60_days + cs.balance_61_90_days + cs.balance_over_90_days, 0)
            ELSE 0
        END AS collection_efficiency_30_days,
        CASE 
            WHEN cs.balance_0_30_days + cs.balance_31_60_days + cs.balance_61_90_days + cs.balance_over_90_days > 0
            THEN pa.payments_60_days / NULLIF(cs.balance_0_30_days + cs.balance_31_60_days + cs.balance_61_90_days + cs.balance_over_90_days, 0)
            ELSE 0
        END AS collection_efficiency_60_days,
        CASE 
            WHEN cs.balance_0_30_days + cs.balance_31_60_days + cs.balance_61_90_days + cs.balance_over_90_days > 0
            THEN pa.payments_90_days / NULLIF(cs.balance_0_30_days + cs.balance_31_60_days + cs.balance_61_90_days + cs.balance_over_90_days, 0)
            ELSE 0
        END AS collection_efficiency_90_days
    FROM CurrentSnapshot cs
    LEFT JOIN PaymentActivity pa
        ON cs.patient_id = pa.patient_id
)

SELECT
    cs.snapshot_date,
    cs.patient_id,
    cs.total_ar_balance,
    cs.balance_0_30_days,
    cs.balance_31_60_days,
    cs.balance_61_90_days,
    cs.balance_over_90_days,
    cs.estimated_insurance_ar,
    cs.patient_responsibility,
    cs.insurance_responsibility,
    cs.open_procedures_count,
    cs.active_claims_count,
    
    -- Collection efficiency
    ce.collection_efficiency_30_days,
    ce.collection_efficiency_60_days,
    ce.collection_efficiency_90_days,
    
    -- Metadata
    CURRENT_TIMESTAMP AS model_created_at,
    CURRENT_TIMESTAMP AS model_updated_at
    
FROM CurrentSnapshot cs
LEFT JOIN CollectionEfficiency ce
    ON cs.patient_id = ce.patient_id