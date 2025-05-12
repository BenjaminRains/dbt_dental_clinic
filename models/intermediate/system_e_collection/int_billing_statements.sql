{{ config(
    materialized='incremental',
    schema='intermediate',
    unique_key='billing_statement_id'
) }}

/*
    ======================================================
    Note on key strategy:
    - Using an MD5 hash of statement_id as billing_statement_id
    - This ensures stable IDs during incremental processing
    - Avoids problems that can occur with ROW_NUMBER() in incrementals
    - Maintains consistent surrogate keys for downstream models
    ======================================================
*/

/*
    Intermediate model for billing statements
    Part of System E: Collections
    
    This model:
    1. Consolidates statement data with related production items
    2. Tracks statement effectiveness for collections
    3. Integrates with collection campaigns
    4. Enhances AR analysis with statement history
*/

WITH StatementBase AS (
    SELECT
        s.statement_id,
        s.patient_id,
        s.date_sent,
        s.date_range_from,
        s.date_range_to,
        s.balance_total,
        s.insurance_estimate,
        s.mode,
        CASE
            WHEN s.mode = 0 THEN 'Not Sent'
            WHEN s.mode = 1 THEN 'Mail'
            WHEN s.mode = 2 THEN 'Email'
            WHEN s.mode = 3 THEN 'Both Email & Mail'
            WHEN s.mode = 4 THEN 'SMS'
            WHEN s.mode = 5 THEN 'Email & SMS'
            WHEN s.mode = 6 THEN 'Mail & SMS'
            WHEN s.mode = 7 THEN 'All Methods'
            ELSE 'Unknown'
        END AS delivery_method,
        s.is_sent,
        s.is_receipt,
        s.is_invoice,
        s.statement_type,
        s.sms_send_status,
        s.date_timestamp,
        s.note,
        s.email_subject,
        s.email_body,
        s.super_family_id
    FROM {{ ref('stg_opendental__statement') }} s
    WHERE s.date_sent >= CURRENT_DATE - INTERVAL '18 months'
    {% if is_incremental() %}
        AND s.date_sent > (SELECT MAX(date_sent) FROM {{ this }})
    {% endif %}
),

PatientInfo AS (
    SELECT
        patient_id,
        preferred_name,
        total_ar_balance AS total_balance,
        balance_0_30_days,
        balance_31_60_days,
        balance_61_90_days,
        balance_over_90_days
    FROM {{ ref('int_ar_analysis') }}
),

StatementItems AS (
    SELECT
        statement_id,
        COUNT(DISTINCT statement_prod_id) AS item_count,
        COUNT(DISTINCT CASE WHEN prod_type = 1 THEN fkey END) AS procedure_count,
        COUNT(DISTINCT CASE WHEN prod_type = 2 THEN fkey END) AS adjustment_count,
        COUNT(DISTINCT CASE WHEN prod_type = 3 THEN fkey END) AS payment_count,
        COUNT(DISTINCT CASE WHEN prod_type = 4 THEN fkey END) AS misc_count
    FROM {{ ref('stg_opendental__statementprod') }}
    GROUP BY statement_id
),

-- Join to collection campaigns if this statement is for a patient in a collection campaign
CollectionCampaignMatch AS (
    SELECT
        sb.statement_id,
        cc.campaign_id,
        cc.campaign_name,
        cc.campaign_status,
        CASE 
            WHEN sb.date_sent > cc.start_date THEN TRUE
            ELSE FALSE
        END AS sent_during_campaign
    FROM StatementBase sb
    INNER JOIN {{ ref('int_collection_tasks') }} ct
        ON sb.patient_id = ct.patient_id
    INNER JOIN {{ ref('int_collection_campaigns') }} cc
        ON cc.campaign_id = ct.campaign_id
),

-- Identify if payments were made after statement was sent
PaymentActivity AS (
    SELECT
        s.statement_id,
        s.patient_id,
        s.date_sent,
        -- Payment within 7 days
        SUM(CASE 
            WHEN p.payment_date BETWEEN s.date_sent AND s.date_sent + INTERVAL '7 days'
            THEN p.payment_amount
            ELSE 0
        END) AS payment_amount_7days,
        -- Payment within 14 days
        SUM(CASE 
            WHEN p.payment_date BETWEEN s.date_sent AND s.date_sent + INTERVAL '14 days'
            THEN p.payment_amount
            ELSE 0
        END) AS payment_amount_14days,
        -- Payment within 30 days
        SUM(CASE 
            WHEN p.payment_date BETWEEN s.date_sent AND s.date_sent + INTERVAL '30 days'
            THEN p.payment_amount
            ELSE 0
        END) AS payment_amount_30days,
        -- Count of payments
        COUNT(DISTINCT CASE 
            WHEN p.payment_date > s.date_sent AND p.payment_date <= s.date_sent + INTERVAL '30 days'
            THEN p.payment_id
        END) AS payment_count_30days
    FROM StatementBase s
    LEFT JOIN {{ ref('stg_opendental__payment') }} p
        ON s.patient_id = p.patient_id
        AND p.payment_date >= s.date_sent
    GROUP BY s.statement_id, s.patient_id, s.date_sent
),

-- Flag for if this is a collection statement (part of collection effort)
CollectionFlag AS (
    SELECT
        s.statement_id,
        CASE
            WHEN p.balance_61_90_days > 0 OR p.balance_over_90_days > 0 THEN TRUE
            WHEN ccm.campaign_id IS NOT NULL THEN TRUE
            WHEN s.note LIKE '%overdue%' OR s.note LIKE '%past due%' OR s.note LIKE '%collection%' THEN TRUE
            WHEN s.email_subject LIKE '%overdue%' OR s.email_subject LIKE '%past due%' OR s.email_subject LIKE '%collection%' THEN TRUE
            ELSE FALSE
        END AS is_collection_statement,
        CASE
            WHEN pa.payment_amount_30days > 0 THEN TRUE
            ELSE FALSE
        END AS resulted_in_payment,
        CASE
            WHEN pa.payment_amount_30days >= s.balance_total * 0.9 THEN 'full_payment'
            WHEN pa.payment_amount_30days > 0 THEN 'partial_payment'
            ELSE 'no_payment'
        END AS payment_result
    FROM StatementBase s
    LEFT JOIN PatientInfo p
        ON s.patient_id = p.patient_id
    LEFT JOIN CollectionCampaignMatch ccm
        ON s.statement_id = ccm.statement_id
    LEFT JOIN PaymentActivity pa
        ON s.statement_id = pa.statement_id
)

-- Final selection
SELECT
    -- Create a stable hash-based unique ID for this model
    MD5(CAST(sb.statement_id AS VARCHAR)) AS billing_statement_id,
    sb.statement_id,
    sb.patient_id,
    sb.date_sent,
    sb.date_range_from,
    sb.date_range_to,
    sb.balance_total,
    sb.insurance_estimate,
    sb.mode,
    sb.delivery_method,
    sb.is_sent,
    sb.is_receipt,
    sb.is_invoice,
    sb.statement_type,
    sb.sms_send_status,
    
    -- Patient AR information
    pi.preferred_name AS patient_name,
    pi.total_balance,
    pi.balance_0_30_days,
    pi.balance_31_60_days,
    pi.balance_61_90_days,
    pi.balance_over_90_days,
    
    -- Statement items
    COALESCE(si.item_count, 0) AS item_count,
    COALESCE(si.procedure_count, 0) AS procedure_count,
    COALESCE(si.adjustment_count, 0) AS adjustment_count,
    COALESCE(si.payment_count, 0) AS payment_count,
    COALESCE(si.misc_count, 0) AS misc_count,
    
    -- Collection campaign information
    ccm.campaign_id,
    ccm.campaign_name,
    ccm.campaign_status,
    ccm.sent_during_campaign,
    
    -- Payment response
    COALESCE(pa.payment_amount_7days, 0) AS payment_amount_7days,
    COALESCE(pa.payment_amount_14days, 0) AS payment_amount_14days,
    COALESCE(pa.payment_amount_30days, 0) AS payment_amount_30days,
    COALESCE(pa.payment_count_30days, 0) AS payment_count_30days,
    
    -- Payment metrics
    CASE
        WHEN sb.balance_total > 0 
        THEN ROUND(COALESCE(pa.payment_amount_30days, 0)::numeric / sb.balance_total::numeric, 2)
        ELSE 0
    END AS payment_ratio_30days,
    
    -- Collection flags
    cf.is_collection_statement,
    cf.resulted_in_payment,
    cf.payment_result,
    
    -- Metadata
    CURRENT_TIMESTAMP AS model_created_at,
    CURRENT_TIMESTAMP AS model_updated_at
FROM StatementBase sb
LEFT JOIN PatientInfo pi
    ON sb.patient_id = pi.patient_id
LEFT JOIN StatementItems si
    ON sb.statement_id = si.statement_id
LEFT JOIN CollectionCampaignMatch ccm
    ON sb.statement_id = ccm.statement_id
LEFT JOIN PaymentActivity pa
    ON sb.statement_id = pa.statement_id
LEFT JOIN CollectionFlag cf
    ON sb.statement_id = cf.statement_id