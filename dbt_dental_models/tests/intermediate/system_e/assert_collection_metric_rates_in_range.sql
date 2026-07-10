-- Failing rows: collection metric rates outside [0, 1]
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
    OR payment_punctuality_rate < 0 OR payment_punctuality_rate > 1
