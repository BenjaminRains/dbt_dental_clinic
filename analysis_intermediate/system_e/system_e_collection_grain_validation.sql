-- System E Collection Models - Grain and Deduplication Validation Queries
-- Run these queries in DBeaver to validate grain and check for duplicates

-- ============== int_billing_statements ==============
-- Check for duplicates in int_billing_statements
SELECT 
    statement_id, 
    COUNT(*) as count
FROM public_intermediate.int_billing_statements
GROUP BY statement_id
HAVING COUNT(*) > 1;

-- Validate that billing_statement_id is unique
SELECT 
    billing_statement_id, 
    COUNT(*) as count
FROM public_intermediate.int_billing_statements
GROUP BY billing_statement_id
HAVING COUNT(*) > 1;

-- ============== int_statement_metrics ==============
-- Check for duplicates in int_statement_metrics based on grain
SELECT 
    snapshot_date, 
    metric_type, 
    delivery_method, 
    campaign_id, 
    COUNT(*) as count
FROM public_intermediate.int_statement_metrics
GROUP BY snapshot_date, metric_type, delivery_method, campaign_id
HAVING COUNT(*) > 1;

-- Validate that statement_metric_id is unique
SELECT 
    statement_metric_id, 
    COUNT(*) as count
FROM public_intermediate.int_statement_metrics
GROUP BY statement_metric_id
HAVING COUNT(*) > 1;

-- ============== int_collection_campaigns ==============
-- Check for duplicates in int_collection_campaigns
SELECT 
    campaign_id, 
    COUNT(*) as count
FROM public_intermediate.int_collection_campaigns
GROUP BY campaign_id
HAVING COUNT(*) > 1;

-- Check for duplicate campaign names (non-PK but should be unique)
SELECT 
    campaign_name, 
    COUNT(*) as count
FROM public_intermediate.int_collection_campaigns
GROUP BY campaign_name
HAVING COUNT(*) > 1;

-- ============== int_collection_communication ==============
-- Check for duplicates in int_collection_communication
SELECT 
    collection_communication_id, 
    COUNT(*) as count
FROM public_intermediate.int_collection_communication
GROUP BY collection_communication_id
HAVING COUNT(*) > 1;

-- Check for duplicates based on underlying source data
SELECT 
    commlog_id, 
    COUNT(*) as count
FROM public_intermediate.int_collection_communication
GROUP BY commlog_id
HAVING COUNT(*) > 1;

-- ============== int_collection_tasks ==============
-- Check for duplicates in int_collection_tasks
SELECT 
    collection_task_id, 
    COUNT(*) as count
FROM public_intermediate.int_collection_tasks
GROUP BY collection_task_id
HAVING COUNT(*) > 1;

-- Check for duplicates based on underlying source data
SELECT 
    task_id, 
    COUNT(*) as count
FROM public_intermediate.int_collection_tasks
GROUP BY task_id
HAVING COUNT(*) > 1;

-- ============== int_collection_metrics ==============
-- Check for duplicates in int_collection_metrics
SELECT 
    metric_id, 
    COUNT(*) as count
FROM public_intermediate.int_collection_metrics
GROUP BY metric_id
HAVING COUNT(*) > 1;

-- Check for duplicates based on grain
SELECT 
    snapshot_date, 
    campaign_id, 
    user_id, 
    metric_level, 
    COUNT(*) as count
FROM public_intermediate.int_collection_metrics
GROUP BY snapshot_date, campaign_id, user_id, metric_level
HAVING COUNT(*) > 1;

-- ============== Cross-Model Validation ==============
-- Validate task counts between metrics and tasks
SELECT 
    m.campaign_id,
    m.tasks_total AS metric_tasks_count,
    COUNT(DISTINCT t.collection_task_id) AS actual_tasks_count,
    ABS(m.tasks_total - COUNT(DISTINCT t.collection_task_id)) AS difference
FROM public_intermediate.int_collection_metrics m
JOIN public_intermediate.int_collection_tasks t
    ON m.campaign_id = t.campaign_id
WHERE m.campaign_id IS NOT NULL
  AND m.metric_level = 'campaign'
GROUP BY m.campaign_id, m.tasks_total
HAVING ABS(m.tasks_total - COUNT(DISTINCT t.collection_task_id)) > 0;

-- Validate that all payments in communications are reflected in metrics
WITH communication_payments AS (
    SELECT 
        c.campaign_id,
        SUM(c.actual_payment_amount) AS communication_payments
    FROM public_intermediate.int_collection_communication c
    WHERE c.campaign_id IS NOT NULL
      AND c.actual_payment_amount > 0
    GROUP BY c.campaign_id
)
SELECT
    cp.campaign_id,
    cp.communication_payments,
    m.collected_amount AS metric_collected_amount,
    ABS(cp.communication_payments - m.collected_amount) AS difference
FROM communication_payments cp
JOIN public_intermediate.int_collection_metrics m
    ON cp.campaign_id = m.campaign_id
WHERE m.metric_level = 'campaign'
HAVING ABS(cp.communication_payments - m.collected_amount) > 0.01;  -- Small tolerance for floating point issues

-- ============== Range Validation for Derived Metrics ==============
-- Check for metrics outside reasonable ranges
SELECT 
    metric_id,
    metric_level,
    campaign_id,
    user_id,
    collection_rate,
    contact_rate,
    payment_fulfillment_rate,
    payment_punctuality_rate
FROM public_intermediate.int_collection_metrics
WHERE collection_rate > 1.0
   OR contact_rate > 1.0
   OR payment_fulfillment_rate > 1.0
   OR payment_punctuality_rate > 1.0;

-- ============== Relationship Validation ==============
-- Check for tasks without valid campaigns
SELECT 
    t.collection_task_id,
    t.campaign_id
FROM public_intermediate.int_collection_tasks t
LEFT JOIN public_intermediate.int_collection_campaigns c
    ON t.campaign_id = c.campaign_id
WHERE t.campaign_id IS NOT NULL
  AND c.campaign_id IS NULL;

-- Check for communications without valid tasks
SELECT 
    c.collection_communication_id,
    c.collection_task_id
FROM public_intermediate.int_collection_communication c
LEFT JOIN public_intermediate.int_collection_tasks t
    ON c.collection_task_id = t.collection_task_id
WHERE c.collection_task_id IS NOT NULL
  AND t.collection_task_id IS NULL;

-- Check for statements linked to non-existent campaigns
SELECT 
    s.billing_statement_id,
    s.campaign_id
FROM public_intermediate.int_billing_statements s
LEFT JOIN public_intermediate.int_collection_campaigns c
    ON s.campaign_id = c.campaign_id
WHERE s.campaign_id IS NOT NULL
  AND c.campaign_id IS NULL;