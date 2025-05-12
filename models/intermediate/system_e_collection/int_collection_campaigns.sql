{{ config(
    materialized='table',
    schema='intermediate',
    unique_key='campaign_id'
) }}

/*
    Intermediate model for collection campaigns
    Part of System E: Collections

    This model:
    1. Creates and tracks collection campaigns
    2. Defines target criteria for collections
    3. Monitors collection performance by campaign
    4. Builds on AR analysis models for accurate targeting

    ======================================================
    FUTURE ENHANCEMENT NOTE:

    Currently, the clinic doesn't actively use campaigns, but this model
    provides the foundation for campaign-based collections when needed.

    When expanding campaign functionality in the future, consider:

    1. Moving campaign definitions to a seed file (CSV in seeds/ directory)
       instead of hardcoding in SQL. This would allow:
       - Non-technical users to manage campaign definitions
       - Easier campaign additions without code changes
       - Better separation of reference data from analytics logic

    2. Separating the model into:
       - A reference model for campaign definitions
       - A metrics model for campaign performance

    3. Adding a UI or admin interface for campaign management

    Example seed file structure (campaigns.csv):
    campaign_id,campaign_name,description,start_date,end_date,...

    Then reference it in models using {{ ref('campaigns') }}
    ======================================================
*/

WITH CampaignDefinitions AS (
    -- Campaigns with dates aligned to historical task data
    SELECT
        1 AS campaign_id,
        'Patient Collections' AS campaign_name,
        'Standard collection campaign targeting overdue patient balances' AS campaign_description,
        '2023-01-01'::date AS start_date,
        '2024-12-31'::date AS end_date,
        100.00 AS target_ar_balance_min,
        NULL AS target_ar_balance_max,
        30 AS target_aging_min,
        NULL AS target_aging_max,
        'high' AS priority,
        'active' AS campaign_status,
        57 AS assigned_user_id  -- Based on common user_id in tasks (adjust if needed)
    
    UNION ALL
    
    SELECT
        2 AS campaign_id,
        'Insurance Follow-up' AS campaign_name,
        'Campaign focused on following up with insurance claims and verifications' AS campaign_description,
        '2023-01-01'::date AS start_date,
        '2024-12-31'::date AS end_date,
        0.00 AS target_ar_balance_min,
        NULL AS target_ar_balance_max,
        0 AS target_aging_min,
        NULL AS target_aging_max,
        'medium' AS priority,
        'active' AS campaign_status,
        46 AS assigned_user_id  -- Based on common user_id in tasks
    
    UNION ALL
    
    SELECT
        3 AS campaign_id,
        'Treatment Plan Payment Collection' AS campaign_name,
        'Campaign for collecting payments related to treatment plans' AS campaign_description,
        '2023-01-01'::date AS start_date,
        '2024-12-31'::date AS end_date,
        200.00 AS target_ar_balance_min,
        NULL AS target_ar_balance_max,
        0 AS target_aging_min,
        NULL AS target_aging_max,
        'medium' AS priority,
        'active' AS campaign_status,
        49 AS assigned_user_id  -- Based on common user_id in tasks
),

CampaignAccounts AS (
    -- Match patients to campaigns based on criteria
    SELECT
        cd.campaign_id,
        ar.patient_id,
        ar.total_ar_balance,
        ar.balance_0_30_days,
        ar.balance_31_60_days,
        ar.balance_61_90_days,
        ar.balance_over_90_days,
        ar.patient_responsibility,
        ar.insurance_responsibility,
        ar.pending_claims_count,
        ar.denied_claims_count
    FROM {{ ref('int_ar_analysis') }} ar
    CROSS JOIN CampaignDefinitions cd
    WHERE (
        -- Match balance criteria with explicit casting using larger precision
        (ar.total_ar_balance >= CAST(cd.target_ar_balance_min AS NUMERIC) OR cd.target_ar_balance_min IS NULL)
        AND (ar.total_ar_balance <= CAST(COALESCE(cd.target_ar_balance_max, '99999.99') AS NUMERIC))
        
        -- Match aging criteria based on campaign
        AND (
            (cd.campaign_id = 1 AND ar.balance_over_90_days > 0)
            OR (cd.campaign_id = 2 AND ar.balance_61_90_days > 0)
            OR (cd.campaign_id = 3 AND ar.denied_claims_count > 0)
        )
    )
    AND cd.campaign_status IN ('active', 'planned')
),

CampaignMetrics AS (
    -- Calculate metrics for each campaign
    SELECT
        campaign_id,
        COUNT(DISTINCT patient_id) AS total_accounts,
        COALESCE(SUM(total_ar_balance), 0)::NUMERIC AS total_ar_amount,
        0.00::NUMERIC AS collected_amount, -- This would be updated with actual data
        0.00::NUMERIC AS collection_rate  -- This would be calculated from actual data
    FROM CampaignAccounts
    GROUP BY campaign_id
)

-- Final selection
SELECT
    cd.campaign_id,
    cd.campaign_name,
    cd.campaign_description,
    cd.start_date,
    cd.end_date,
    cd.target_ar_balance_min,
    cd.target_ar_balance_max,
    cd.target_aging_min,
    cd.target_aging_max,
    cd.priority,
    cd.campaign_status,
    cd.assigned_user_id,
    
    -- Campaign metrics
    COALESCE(cm.total_accounts, 0) AS total_accounts,
    COALESCE(cm.total_ar_amount, 0.00)::NUMERIC AS total_ar_amount,
    COALESCE(cm.collected_amount, 0.00)::NUMERIC AS collected_amount,
    CASE 
        WHEN COALESCE(cm.total_ar_amount, 0.00) > 0 
        THEN COALESCE(cm.collected_amount, 0.00)::NUMERIC / NULLIF(cm.total_ar_amount, 0)::NUMERIC
        ELSE 0.00::NUMERIC
    END AS collection_rate,
    
    -- Metadata fields
    CURRENT_TIMESTAMP AS created_at,
    CURRENT_TIMESTAMP AS updated_at,
    CURRENT_TIMESTAMP AS model_created_at,
    CURRENT_TIMESTAMP AS model_updated_at
FROM CampaignDefinitions cd
LEFT JOIN CampaignMetrics cm
    ON cd.campaign_id = cm.campaign_id