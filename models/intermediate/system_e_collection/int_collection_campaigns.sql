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
*/

WITH CampaignDefinitions AS (
    -- This would typically come from a seed file or external source
    -- For demonstration, defining sample campaigns here
    SELECT
        1 AS campaign_id,
        'Overdue 90+ High Balance' AS campaign_name,
        'Collection campaign targeting accounts with balances over $1000 that are 90+ days overdue' AS campaign_description,
        '2025-01-01'::date AS start_date,
        '2025-03-31'::date AS end_date,
        1000.00 AS target_ar_balance_min,
        NULL AS target_ar_balance_max,
        90 AS target_aging_min,
        NULL AS target_aging_max,
        'high' AS priority,
        'active' AS campaign_status,
        15 AS assigned_user_id  -- Example user ID
    
    UNION ALL
    
    SELECT
        2 AS campaign_id,
        'Overdue 60-90 Medium Balance' AS campaign_name,
        'Collection campaign targeting accounts with balances between $500-$1000 that are 60-90 days overdue' AS campaign_description,
        '2025-01-15'::date AS start_date,
        '2025-03-15'::date AS end_date,
        500.00 AS target_ar_balance_min,
        1000.00 AS target_ar_balance_max,
        60 AS target_aging_min,
        90 AS target_aging_max,
        'medium' AS priority,
        'planned' AS campaign_status,
        18 AS assigned_user_id  -- Example user ID
    
    UNION ALL
    
    SELECT
        3 AS campaign_id,
        'Insurance Rejected Claims' AS campaign_name,
        'Collection campaign targeting accounts with rejected insurance claims' AS campaign_description,
        '2025-02-01'::date AS start_date,
        '2025-04-30'::date AS end_date,
        200.00 AS target_ar_balance_min,
        NULL AS target_ar_balance_max,
        30 AS target_aging_min,
        NULL AS target_aging_max,
        'high' AS priority,
        'planned' AS campaign_status,
        22 AS assigned_user_id  -- Example user ID
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
        -- Match balance criteria
        (ar.total_ar_balance >= cd.target_ar_balance_min OR cd.target_ar_balance_min IS NULL)
        AND (ar.total_ar_balance <= cd.target_ar_balance_max OR cd.target_ar_balance_max IS NULL)
        
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
        SUM(total_ar_balance) AS total_ar_amount,
        0.00 AS collected_amount, -- This would be updated with actual data
        0.00 AS collection_rate  -- This would be calculated from actual data
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
    COALESCE(cm.total_ar_amount, 0.00) AS total_ar_amount,
    COALESCE(cm.collected_amount, 0.00) AS collected_amount,
    CASE 
        WHEN COALESCE(cm.total_ar_amount, 0.00) > 0 
        THEN COALESCE(cm.collected_amount, 0.00) / cm.total_ar_amount 
        ELSE 0.00 
    END AS collection_rate,
    
    -- Metadata fields
    CURRENT_TIMESTAMP AS created_at,
    CURRENT_TIMESTAMP AS updated_at,
    CURRENT_TIMESTAMP AS model_created_at,
    CURRENT_TIMESTAMP AS model_updated_at
FROM CampaignDefinitions cd
LEFT JOIN CampaignMetrics cm
    ON cd.campaign_id = cm.campaign_id