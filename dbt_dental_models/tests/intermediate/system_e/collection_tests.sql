-- Test that collection campaigns have valid dates
SELECT
    campaign_id,
    campaign_name,
    start_date,
    end_date
FROM {{ ref('int_collection_campaigns') }}
WHERE 
    start_date > end_date
    OR start_date IS NULL
    OR end_date IS NULL;

-- Test that collection tasks have valid patient IDs
SELECT
    collection_task_id,
    campaign_id,
    patient_id
FROM {{ ref('int_collection_tasks') }}
WHERE patient_id IS NULL;

-- Test that collection communications have valid links
SELECT
    collection_communication_id,
    commlog_id,
    patient_id,
    collection_task_id
FROM {{ ref('int_collection_communication') }}
WHERE 
    commlog_id IS NULL
    OR patient_id IS NULL;

-- Test for collection metrics validity
SELECT
    metric_id,
    snapshot_date,
    collection_rate,
    payment_fulfillment_rate,
    payment_punctuality_rate
FROM {{ ref('int_collection_metrics') }}
WHERE 
    collection_rate < 0 OR collection_rate > 1
    OR payment_fulfillment_rate < 0 OR payment_fulfillment_rate > 1
    OR payment_punctuality_rate < 0 OR payment_punctuality_rate > 1;

-- Test for consistent campaign metrics across tables
WITH CampaignCounts AS (
    SELECT
        cc.campaign_id,
        COUNT(DISTINCT ct.collection_task_id) AS task_count_from_tasks,
        COUNT(DISTINCT com.collection_communication_id) AS comm_count_from_comms,
        COUNT(DISTINCT cm.metric_id) AS metric_count_from_metrics
    FROM {{ ref('int_collection_campaigns') }} cc
    LEFT JOIN {{ ref('int_collection_tasks') }} ct
        ON cc.campaign_id = ct.campaign_id
    LEFT JOIN {{ ref('int_collection_communication') }} com
        ON ct.collection_task_id = com.collection_task_id
    LEFT JOIN {{ ref('int_collection_metrics') }} cm
        ON cc.campaign_id = cm.campaign_id
        AND cm.metric_level = 'campaign'
    GROUP BY cc.campaign_id
)
SELECT
    campaign_id,
    task_count_from_tasks,
    comm_count_from_comms,
    metric_count_from_metrics
FROM CampaignCounts
WHERE metric_count_from_metrics = 0;