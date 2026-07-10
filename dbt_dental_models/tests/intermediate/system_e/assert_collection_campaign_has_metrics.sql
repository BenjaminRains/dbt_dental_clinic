-- Failing rows: campaigns with no campaign-level metrics
WITH campaign_counts AS (
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
FROM campaign_counts
WHERE metric_count_from_metrics = 0
