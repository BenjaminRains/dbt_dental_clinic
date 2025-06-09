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
        created_by_column (str|none): Source column for creating user ID (default: '"SecUserNumEntry"')
                                     Set to none, null, or empty string to omit this column
        
    Returns:
        SQL columns for consistent metadata tracking
        
    Example usage:
        {{ standardize_metadata_columns() }}
        
        -- With custom columns:
        {{ standardize_metadata_columns(
            created_at_column='"SecDateEntry"',
            updated_at_column='"SecDateTEdit"'
        ) }}
        
        -- For tables without SecUserNumEntry column:
        {{ standardize_metadata_columns(
            created_at_column='"DateTStamp"',
            updated_at_column='"DateTStamp"',
            created_by_column=none
        ) }}
-#}
        -- Metadata columns (standardized across all models)
        current_timestamp as _loaded_at,                    -- ETL pipeline processing time
        
        {%- if created_at_column %}
        CASE 
            WHEN {{ created_at_column }} = '0001-01-01'::date 
                OR {{ created_at_column }} = '1900-01-01'::date 
            THEN null
            ELSE {{ created_at_column }}
        END as _created_at,                                 -- Original creation timestamp
        {%- else %}
        null as _created_at,                                -- No creation timestamp available
        {%- endif %}
        
        {%- if updated_at_column %}
        CASE 
            WHEN {{ updated_at_column }} = '0001-01-01'::date 
                OR {{ updated_at_column }} = '1900-01-01'::date 
            THEN {{ created_at_column if created_at_column else 'null' }}
            ELSE COALESCE({{ updated_at_column }}, {{ created_at_column if created_at_column else 'null' }})
        END as _updated_at,                                 -- Last update timestamp
        {%- else %}
        {{ created_at_column if created_at_column else 'null' }} as _updated_at, -- Fallback to created timestamp
        {%- endif %}
        
        {%- if created_by_column and created_by_column != none and created_by_column != '' %}
        NULLIF({{ created_by_column }}, 0) as _created_by_user_id -- User who created the record
        {%- else %}
        null as _created_by_user_id                         -- No user tracking available
        {%- endif %}
{% endmacro %} 