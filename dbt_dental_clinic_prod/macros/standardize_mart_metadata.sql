{% macro standardize_mart_metadata() %}
{#- 
    Adds standardized metadata columns to mart models
    
    This macro provides consistent metadata tracking for mart models,
    combining business timestamps with pipeline tracking for comprehensive
    data lineage and debugging capabilities.
    
    Returns:
        SQL columns for consistent mart model tracking:
        - _loaded_at: ETL extraction timestamp (when data was loaded to raw schema)
        - _transformed_at: dbt mart model build timestamp (when this mart model was built)
        - _created_at: Business creation timestamp (from primary source)
        - _updated_at: Business update timestamp (from primary source)
        - _mart_refreshed_at: Mart-specific refresh timestamp (when mart was last refreshed)
        
    Usage in mart models:
        -- Add at the end of the main select statement
        {{ standardize_mart_metadata() }}
        
    Mart Model Architecture Notes:
        - _loaded_at: ETL extraction timestamp (pipeline monitoring)
        - _transformed_at: dbt mart model build timestamp (model-specific tracking)
        - _created_at: Business creation timestamp (primary focus)
        - _updated_at: Business update timestamp (primary focus)
        - _mart_refreshed_at: Mart refresh timestamp (mart-specific tracking)
        
    Best Practices:
        - Always include this macro in mart models for consistent metadata
        - Business timestamps (_created_at, _updated_at) are primary focus
        - Pipeline timestamps (_loaded_at, _transformed_at) are for debugging
        - Mart refresh timestamp (_mart_refreshed_at) is for mart-specific tracking
        
    Pipeline Debugging:
        - _loaded_at shows ETL extraction time
        - _transformed_at shows when this mart model was built
        - _mart_refreshed_at shows when the mart was last refreshed
        - Different timestamps help identify pipeline issues and model dependencies
-#}
    -- ETL extraction timestamp (when data was loaded to raw schema)
    -- Note: Using current_timestamp as fallback since raw layer doesn't have _loaded_at
    current_timestamp as _loaded_at,
    
    -- dbt mart model build timestamp (when this mart model was built)
    current_timestamp as _transformed_at,
    
    -- Business creation timestamp (from primary source)
    -- Note: This should be populated from the primary source's _created_at field
    current_timestamp as _created_at,
    
    -- Business update timestamp (from primary source)
    -- Note: This should be populated from the primary source's _updated_at field
    current_timestamp as _updated_at,
    
    -- Mart-specific refresh timestamp (when mart was last refreshed)
    current_timestamp as _mart_refreshed_at
{% endmacro %}
