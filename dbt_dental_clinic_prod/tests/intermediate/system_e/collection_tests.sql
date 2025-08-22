-- Test that collection campaigns have valid dates and metadata
SELECT
    'invalid_campaign_dates' as test_type,
    campaign_id,
    campaign_name,
    start_date,
    end_date,
    NULL as additional_info
FROM {{ ref('int_collection_campaigns') }}
WHERE 
    start_date > end_date
    OR start_date IS NULL
    OR end_date IS NULL

UNION ALL

-- Test that collection tasks have valid patient IDs
SELECT
    'invalid_task_patient' as test_type,
    collection_task_id as campaign_id,
    'Collection Task' as campaign_name,
    NULL as start_date,
    NULL as end_date,
    'Missing patient_id' as additional_info
FROM {{ ref('int_collection_tasks') }}
WHERE patient_id IS NULL

UNION ALL

-- Test that collection communications have valid links
SELECT
    'invalid_communication_links' as test_type,
    collection_communication_id as campaign_id,
    'Collection Communication' as campaign_name,
    NULL as start_date,
    NULL as end_date,
    CASE 
        WHEN commlog_id IS NULL THEN 'Missing commlog_id'
        WHEN patient_id IS NULL THEN 'Missing patient_id'
        ELSE 'Unknown validation error'
    END as additional_info
FROM {{ ref('int_collection_communication') }}
WHERE 
    commlog_id IS NULL
    OR patient_id IS NULL

UNION ALL

-- Test for collection metrics validity
SELECT
    'invalid_metrics' as test_type,
    metric_id as campaign_id,
    'Collection Metric' as campaign_name,
    NULL as start_date,
    NULL as end_date,
    CASE 
        WHEN collection_rate < 0 OR collection_rate > 1 THEN 'Invalid collection_rate: ' || collection_rate::text
        WHEN payment_fulfillment_rate < 0 OR payment_fulfillment_rate > 1 THEN 'Invalid payment_fulfillment_rate: ' || payment_fulfillment_rate::text
        WHEN payment_punctuality_rate < 0 OR payment_punctuality_rate > 1 THEN 'Invalid payment_punctuality_rate: ' || payment_punctuality_rate::text
        ELSE 'Unknown metric issue'
    END as additional_info
FROM {{ ref('int_collection_metrics') }}
WHERE 
    collection_rate < 0 OR collection_rate > 1
    OR payment_fulfillment_rate < 0 OR payment_fulfillment_rate > 1
    OR payment_punctuality_rate < 0 OR payment_punctuality_rate > 1

UNION ALL

-- Test for consistent campaign metrics across tables
SELECT
    'inconsistent_campaign_metrics' as test_type,
    cc.campaign_id,
    cc.campaign_name,
    NULL as start_date,
    NULL as end_date,
    'Campaign has no metrics recorded' as additional_info
FROM {{ ref('int_collection_campaigns') }} cc
LEFT JOIN {{ ref('int_collection_metrics') }} cm
    ON cc.campaign_id = cm.campaign_id
    AND cm.metric_level = 'campaign'
WHERE cm.metric_id IS NULL

UNION ALL

-- Test for invalid task relationships (communications with non-existent tasks)
SELECT
    'invalid_task_relationship' as test_type,
    cc.collection_communication_id as campaign_id,
    'Collection Communication' as campaign_name,
    NULL as start_date,
    NULL as end_date,
    'Communication has task_id but no matching task' as additional_info
FROM {{ ref('int_collection_communication') }} cc
LEFT JOIN {{ ref('int_collection_tasks') }} ct
    ON cc.collection_task_id = ct.collection_task_id
WHERE cc.collection_task_id IS NOT NULL 
  AND ct.collection_task_id IS NULL