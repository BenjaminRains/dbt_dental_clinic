{{ config(
    materialized='incremental',
    unique_key='recall_id',
    schema='staging'
) }}

WITH Source AS (
    SELECT * 
    FROM {{ source('opendental', 'recall') }}
    WHERE "DateDue" >= '2023-01-01'::date  
        AND "DateDue" <= CURRENT_DATE
    {% if is_incremental() %}
        AND "DateTStamp" > (SELECT max(_updated_at) FROM {{ this }})
    {% endif %}
),

Renamed AS (
    SELECT
        -- Primary key
        "RecallNum" AS recall_id,
        
        -- Relationships (following DDL index order)
        "PatNum" AS patient_id,            -- Has index: recall_PatNum
        "RecallTypeNum" AS recall_type_id, -- Has index: recall_RecallTypeNum
        
        -- Date fields
        "DateDueCalc" AS date_due_calc,
        "DateDue" AS date_due,             -- Has index: recall_DateDisabledType
        "DatePrevious" AS date_previous,   -- Has index: recall_DatePrevious
        "DateScheduled" AS date_scheduled, -- Has index: recall_DateScheduled
        "DisableUntilDate" AS disable_until_date,
        
        -- Status and configuration
        "RecallInterval" AS recall_interval,
        "RecallStatus" AS recall_status,
        CASE WHEN COALESCE("IsDisabled", 0) = 1 THEN TRUE ELSE FALSE END AS is_disabled_flag, -- Has index: recall_IsDisabled
        "DisableUntilBalance" AS disable_until_balance,
        COALESCE("Priority", 0)::smallint AS priority,
        
        -- Text fields
        NULLIF(TRIM("Note"), '') AS note,
        NULLIF(TRIM("TimePatternOverride"), '') AS time_pattern_override,
        
        -- Required metadata columns
        current_timestamp AS _loaded_at,
        "DateTStamp" AS _created_at,
        "DateTStamp" AS _updated_at
    FROM Source
)

SELECT * FROM Renamed
