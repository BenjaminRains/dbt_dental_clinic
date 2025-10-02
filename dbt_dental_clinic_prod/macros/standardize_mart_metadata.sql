{% macro standardize_mart_metadata(
    primary_source_alias=none,
    preserve_source_metadata=true,
    source_metadata_fields=['_loaded_at', '_created_at', '_updated_at', '_created_by']
) %}
{#- 
    Adds standardized metadata columns to mart models
    
    This macro provides consistent metadata tracking for mart models,
    combining business timestamps with pipeline tracking for comprehensive
    data lineage and debugging capabilities.
    
    Args:
        primary_source_alias (str|none): Alias of the primary source table (e.g., 'clinic_data', 'patient_data')
        REQUIRED when preserve_source_metadata=true to specify which table to pull metadata from
        preserve_source_metadata (bool): Whether to preserve metadata from primary source
        source_metadata_fields (list): List of metadata fields to preserve from primary source
        
    Returns:
        SQL columns for consistent mart model tracking:
        - Primary source metadata fields (if preserve_source_metadata=true and fields exist)
        - _transformed_at: dbt mart model build timestamp (model-specific tracking)
        - _mart_refreshed_at: Mart-specific refresh timestamp (when mart was last refreshed)
        
    Usage in mart models:
        -- Default usage (preserves primary source metadata):
        {{ standardize_mart_metadata(primary_source_alias='source_alias') }}
        
        -- Custom metadata fields (when primary source has different fields):
        {{ standardize_mart_metadata(
            primary_source_alias='source_alias',
            source_metadata_fields=['_loaded_at', '_updated_at']
        ) }}
        
        -- No source metadata (not recommended):
        {{ standardize_mart_metadata(preserve_source_metadata=false) }}
        
        -- IMPORTANT: When preserve_source_metadata=true (default), primary_source_alias is REQUIRED
        -- The macro needs to know which table alias to reference for metadata fields
        -- This should match the alias used in the FROM clause of your final SELECT statement
        
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
    {%- if preserve_source_metadata and primary_source_alias and source_metadata_fields %}
    -- Preserve metadata from primary source (business timestamps first)
    {%- for field in source_metadata_fields %}
    {%- if loop.first %}
    {%- if field == '_transformed_at' %}
    {{ primary_source_alias }}.{{ field }} as _source_transformed_at
    {%- else %}
    {{ primary_source_alias }}.{{ field }}
    {%- endif %}
    {%- else %}
    {%- if field == '_transformed_at' %}
    ,{{ primary_source_alias }}.{{ field }} as _source_transformed_at
    {%- else %}
    ,{{ primary_source_alias }}.{{ field }}
    {%- endif %}
    {%- endif %}
    {%- endfor %},
    {%- endif %}
    
    -- dbt mart model build timestamp (when this mart model was built)
    current_timestamp as _transformed_at,
    
    -- Mart-specific refresh timestamp (when mart was last refreshed)
    current_timestamp as _mart_refreshed_at
{% endmacro %}