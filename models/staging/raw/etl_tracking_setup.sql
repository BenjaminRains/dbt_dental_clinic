{{ config(
    materialized='table',
    schema='raw',
    tags=['etl', 'tracking', 'setup']
) }}

-- Call the macro to create tracking tables
{{ create_etl_tracking_tables() }}

-- Return a simple result to satisfy dbt
select 1 as setup_complete 