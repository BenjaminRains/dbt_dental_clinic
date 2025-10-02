{% macro standardize_intermediate_metadata(
    primary_source_alias=none,
    preserve_source_metadata=true,
    source_metadata_fields=['_loaded_at', '_created_at', '_updated_at', '_created_by']
) %}
{#- 
    Adds standardized metadata columns to intermediate models
    
    This macro prioritizes business timestamps from the primary source,
    while maintaining essential pipeline tracking for debugging and monitoring.
    
    Args:
        primary_source_alias (str|none): Alias of the primary source table (e.g., 'ip', 'pp')
        REQUIRED when preserve_source_metadata=true to specify which table to pull metadata from
        preserve_source_metadata (bool): Whether to preserve metadata from primary source
        source_metadata_fields (list): List of metadata fields to preserve from primary source
        
    Returns:
        SQL columns for consistent intermediate model tracking:
        - Primary source metadata fields (if preserve_source_metadata=true and fields exist)
        - _transformed_at: dbt intermediate model build timestamp (model-specific tracking)
        
    Usage in intermediate models:
        -- Default usage (preserves primary source metadata):
        {{ standardize_intermediate_metadata(primary_source_alias='ip') }}
        
        -- Custom metadata fields (when primary source has different fields):
        {{ standardize_intermediate_metadata(
            primary_source_alias='ip',
            source_metadata_fields=['_loaded_at', '_updated_at']
        ) }}
        
        -- No source metadata (not recommended):
        {{ standardize_intermediate_metadata(preserve_source_metadata=false) }}
        
        -- IMPORTANT: When preserve_source_metadata=true (default), primary_source_alias is REQUIRED
        -- The macro needs to know which table alias to reference for metadata fields
        -- This should match the alias used in the FROM clause of your final SELECT statement
        
    Example with specific options:
        {{ standardize_intermediate_metadata(
            primary_source_alias='ip',
            preserve_source_metadata=true,
            source_metadata_fields=['_loaded_at', '_created_at', '_updated_at', '_created_by']
        ) }}
        
    Intermediate Model Architecture Notes:
        - Primary source metadata: Preserved from primary staging model
        - _transformed_at: dbt intermediate model build timestamp (model-specific tracking)
        - Business timestamps first: _created_at, _updated_at are primary focus
        
    Best Practices:
        - Always preserve primary source metadata for data lineage
        - Choose most business-relevant source as primary
        - Document primary source selection in YAML
        - Include _transformed_at for pipeline debugging and monitoring
        
    Pipeline Debugging:
        - _loaded_at from primary source shows ETL extraction time
        - _transformed_at shows when this intermediate model was built
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
    
    -- dbt intermediate model build timestamp (model-specific tracking)
    current_timestamp as _transformed_at
{% endmacro %}
