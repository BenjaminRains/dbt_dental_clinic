-- Failing rows: collection campaigns with invalid / missing dates
SELECT
    campaign_id,
    campaign_name,
    start_date,
    end_date
FROM {{ ref('int_collection_campaigns') }}
WHERE
    start_date > end_date
    OR start_date IS NULL
    OR end_date IS NULL
