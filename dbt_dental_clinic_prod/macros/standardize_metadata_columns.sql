{% macro standardize_metadata_columns(
    created_at_column=none, 
    updated_at_column=none, 
    created_by_column=none,
    fallback_created_at_column=none,
    auto_detect_columns=false
) %}
{#- 
    Adds standardized ETL and dbt tracking columns to staging models
    
    Args:
        created_at_column (str|none): Source column for creation timestamp
        updated_at_column (str|none): Source column for update timestamp  
        created_by_column (str|none): Source column for creating user ID
        fallback_created_at_column (str|none): Alternative column (currently unused)
        auto_detect_columns (bool): Whether to attempt common column name detection (currently unused)
        
    Returns:
        SQL columns for consistent ETL and dbt tracking:
        - _loaded_at: ETL pipeline load timestamp (from tracking table or current_timestamp fallback)
        - _transformed_at: dbt model build timestamp (when this staging model was created)
        - _created_at: Source creation timestamp (if created_at_column provided)
        - _updated_at: Source update timestamp (if updated_at_column provided)
        - _created_by: Source creating user ID (if created_by_column provided)
        
    Example usage:
        {{ standardize_metadata_columns() }}
        {{ standardize_metadata_columns(created_at_column='"DateEntry"') }}
        {{ standardize_metadata_columns(created_at_column='"SecDateTEntry"', updated_at_column='"SecDateTEdit"') }}
        
    ETL Architecture Notes:
        - _loaded_at: ETL pipeline load timestamp (from tracking tables or current_timestamp fallback)
        - _transformed_at: dbt model build timestamp (when this staging model was created)
        - _created_at: Source system creation timestamp (when parameter provided)
        - _updated_at: Source system update timestamp (when parameter provided)
        - _created_by: Source system user ID (when parameter provided)
-#}
        -- ETL and dbt tracking columns
        COALESCE(
            (SELECT _loaded_at 
             FROM {{ source('opendental', 'etl_load_status') }} 
             WHERE table_name = '{{ this.name | replace('stg_opendental__', '') }}'
             AND load_status = 'success'
             ORDER BY _loaded_at DESC 
             LIMIT 1),
            current_timestamp
        ) as _loaded_at,                    -- ETL pipeline processing time (from tracking table or fallback)
        
        current_timestamp as _transformed_at{%- if created_at_column is not none or updated_at_column is not none or created_by_column is not none %},{%- endif %}
        
        {%- if created_at_column is not none %}
        {{ clean_opendental_date(created_at_column) }} as _created_at{%- if updated_at_column is not none or created_by_column is not none %},{%- endif %} -- Source creation timestamp
        {%- endif %}
        
        {%- if updated_at_column is not none %}
        {{ clean_opendental_date(updated_at_column) }} as _updated_at{%- if created_by_column is not none %},{%- endif %} -- Source update timestamp
        {%- endif %}
        
        {%- if created_by_column is not none %}
        {{ created_by_column }} as _created_by -- Source creating user ID
        {%- endif %}
{% endmacro %} 