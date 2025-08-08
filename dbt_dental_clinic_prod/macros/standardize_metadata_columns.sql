{% macro standardize_metadata_columns(
    created_at_column=null, 
    updated_at_column=null, 
    created_by_column=null,
    fallback_created_at_column=null,
    auto_detect_columns=false
) %}
{#- 
    Adds standardized ETL and dbt tracking columns to staging models
    
    Args:
        created_at_column (str|none): Source column for creation timestamp (ignored - kept for compatibility)
        updated_at_column (str|none): Source column for update timestamp (ignored - kept for compatibility)
        created_by_column (str|none): Source column for creating user ID (ignored - kept for compatibility)
        fallback_created_at_column (str|none): Alternative column (ignored - kept for compatibility)
        auto_detect_columns (bool): Whether to attempt common column name detection (ignored - kept for compatibility)
        
    Returns:
        SQL columns for consistent ETL and dbt tracking:
        - _loaded_at: ETL pipeline load timestamp (from tracking table or current_timestamp fallback)
        - _transformed_at: dbt model build timestamp (when this staging model was created)
        
    Example usage:
        {{ standardize_metadata_columns() }}
        {{ standardize_metadata_columns(created_at_column='"DateEntry"') }}
        
    ETL Architecture Notes:
        - _loaded_at: ETL pipeline load timestamp (from tracking tables or current_timestamp fallback)
        - _transformed_at: dbt model build timestamp (when this staging model was created)
-#}
        -- ETL and dbt tracking columns
        COALESCE(
            (SELECT MAX(_loaded_at) FROM {{ source('opendental', 'etl_load_status') }} WHERE table_name = '{{ this.name }}'),
            current_timestamp
        ) as _loaded_at,                    -- ETL pipeline processing time (from tracking table or fallback)
        
        current_timestamp as _transformed_at -- dbt model build timestamp
{% endmacro %} 