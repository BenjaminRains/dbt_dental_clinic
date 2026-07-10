-- Failing rows: campaign statement counts differ between statements and metrics
WITH campaign_statement_counts AS (
    SELECT
        campaign_id,
        COUNT(DISTINCT billing_statement_id) AS statement_count_from_statements
    FROM {{ ref('int_billing_statements') }}
    WHERE campaign_id IS NOT NULL
    GROUP BY campaign_id
),
metric_statement_counts AS (
    SELECT
        campaign_id,
        total_statements AS statement_count_from_metrics
    FROM {{ ref('int_statement_metrics') }}
    WHERE
        metric_type = 'by_campaign'
        AND campaign_id IS NOT NULL
)
SELECT
    csc.campaign_id,
    csc.statement_count_from_statements,
    msc.statement_count_from_metrics
FROM campaign_statement_counts csc
JOIN metric_statement_counts msc
    ON csc.campaign_id = msc.campaign_id
WHERE csc.statement_count_from_statements != msc.statement_count_from_metrics
