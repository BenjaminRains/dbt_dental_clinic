{% macro standardize_metadata_columns(
    created_at_column=none, 
    updated_at_column=none, 
    created_by_column=none
) %}
{#- 
    Adds standardized metadata columns to staging models
    
    This macro prioritizes business timestamps over technical processing timestamps,
    while maintaining essential pipeline tracking for debugging and monitoring.
    
    Args:
        created_at_column (str|none): Source column for business creation timestamp
        updated_at_column (str|none): Source column for business update timestamp  
        created_by_column (str|none): Source column for business user ID
        
    Returns:
        SQL columns for consistent metadata tracking:
        - _loaded_at: ETL extraction timestamp (when data was loaded to raw schema)
        - _transformed_at: dbt model build timestamp (when this staging model was built)
        - _created_at: Business creation timestamp (when created_at_column provided)
        - _updated_at: Business update timestamp (when updated_at_column provided)
        - _created_by: Business user ID (when created_by_column provided)
        
    Example usage:
        {{ standardize_metadata_columns() }}
        {{ standardize_metadata_columns(created_at_column='"DateEntry"') }}
        {{ standardize_metadata_columns(
            created_at_column='"SecDateTEntry"', 
            updated_at_column='"SecDateTEdit"',
            created_by_column='"SecUserNumEntry"'
        ) }}
        
    Metadata Strategy Notes:
        - _loaded_at: ETL extraction timestamp (pipeline monitoring)
        - _transformed_at: dbt model build timestamp (model-specific tracking)
        - _created_at: Business creation timestamp (primary focus)
        - _updated_at: Business update timestamp (primary focus)
        - _created_by: Business user ID (secondary focus)
        
    Pipeline Debugging:
        - Different _loaded_at vs _transformed_at timestamps indicate pipeline issues
        - _loaded_at shows ETL success, _transformed_at shows dbt success
        - Both timestamps are valuable for troubleshooting data pipeline problems
-#}
        -- ETL extraction timestamp (when data was loaded to raw schema)
        -- Note: Raw layer doesn't have _loaded_at column, using current_timestamp as fallback
        -- This follows the graceful degradation principle from the metadata strategy
        current_timestamp as _loaded_at,
        
        -- dbt model build timestamp (when this staging model was built)
        current_timestamp as _transformed_at{%- if created_at_column is not none or updated_at_column is not none or created_by_column is not none %},{%- endif %}
        
        {%- if created_at_column is not none %}
        {{ clean_opendental_date(created_at_column) }} as _created_at{%- if updated_at_column is not none or created_by_column is not none %},{%- endif %} -- Business creation timestamp
        {%- endif %}
        
        {%- if updated_at_column is not none %}
        {{ clean_opendental_date(updated_at_column) }} as _updated_at{%- if created_by_column is not none %},{%- endif %} -- Business update timestamp
        {%- endif %}
        
        {%- if created_by_column is not none %}
        {{ transform_id_columns([{'source': created_by_column, 'target': '_created_by'}]) }} -- Business user ID
        {%- endif %}
{% endmacro %} 