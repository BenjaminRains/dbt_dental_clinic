{% macro standardize_metadata_columns(
    created_at_column='"DateEntry"', 
    updated_at_column='"DateTStamp"', 
    created_by_column='"SecUserNumEntry"'
) %}
{#- 
    Adds standardized metadata columns to staging models
    
    Args:
        created_at_column (str): Source column for creation timestamp (default: '"DateEntry"')
        updated_at_column (str): Source column for update timestamp (default: '"DateTStamp"')
        created_by_column (str): Source column for creating user ID (default: '"SecUserNumEntry"')
        
    Returns:
        SQL columns for consistent metadata tracking
        
    Example usage:
        {{ standardize_metadata_columns() }}
        
        -- With custom columns:
        {{ standardize_metadata_columns(
            created_at_column='"SecDateEntry"',
            updated_at_column='"SecDateTEdit"'
        ) }}
-#}
        -- Metadata columns (standardized across all models)
        current_timestamp as _loaded_at,                    -- ETL pipeline processing time
        CASE 
            WHEN {{ created_at_column }} = '0001-01-01'::date 
                OR {{ created_at_column }} = '1900-01-01'::date 
            THEN null
            ELSE {{ created_at_column }}
        END as _created_at,                                 -- Original creation timestamp
        CASE 
            WHEN {{ updated_at_column }} = '0001-01-01'::date 
                OR {{ updated_at_column }} = '1900-01-01'::date 
            THEN {{ created_at_column }}
            ELSE COALESCE({{ updated_at_column }}, {{ created_at_column }})
        END as _updated_at,                                 -- Last update timestamp
        {% if created_by_column %}
        {{ created_by_column }} as _created_by_user_id      -- User who created the record
        {% endif %}
{% endmacro %} 