{% macro standardize_metadata_columns(
    created_at_column='"DateEntry"', 
    updated_at_column='"DateTStamp"', 
    created_by_column='"SecUserNumEntry"',
    fallback_created_at_column=null,
    auto_detect_columns=false
) %}
{#- 
    Adds standardized metadata columns to staging models with improved edge case handling
    
    Args:
        created_at_column (str): Source column for creation timestamp (default: '"DateEntry"')
        updated_at_column (str): Source column for update timestamp (default: '"DateTStamp"')
        created_by_column (str|none): Source column for creating user ID (default: '"SecUserNumEntry"')
                                     Set to none, null, or empty string to omit this column
        fallback_created_at_column (str|none): Alternative column to use if created_at_column doesn't exist
        auto_detect_columns (bool): Whether to attempt common column name detection (future enhancement)
        
    Returns:
        SQL columns for consistent metadata tracking
        
    Example usage:
        -- Standard usage (most tables):
        {{ standardize_metadata_columns() }}
        
        -- Custom columns for specific tables:
        {{ standardize_metadata_columns(
            created_at_column='"SecDateEntry"',
            updated_at_column='"SecDateTEdit"'
        ) }}
        
        -- Tables without standard columns (like disease table):
        {{ standardize_metadata_columns(
            created_at_column='"DateStart"',
            updated_at_column='"DateTStamp"',
            created_by_column=none
        ) }}
        
        -- Tables with fallback options:
        {{ standardize_metadata_columns(
            created_at_column='"DateEntry"',
            fallback_created_at_column='"DateStart"',
            created_by_column=none
        ) }}
        
    Common OpenDental Table Patterns:
        - Standard tables: DateEntry, DateTStamp, SecUserNumEntry
        - Secure tables: SecDateEntry, SecDateTEdit, SecUserNumEntry  
        - Disease-like tables: DateStart, DateTStamp, (no user tracking)
        - Simple tables: DateTStamp only
-#}
        -- Metadata columns (standardized across all models)
        current_timestamp as _loaded_at,                    -- ETL pipeline processing time
        
        {%- if created_at_column and created_at_column != none and created_at_column != '' %}
        CASE 
            WHEN {{ created_at_column }} = '0001-01-01'::date 
                OR {{ created_at_column }} = '1900-01-01'::date 
                OR {{ created_at_column }} = '0001-01-01 00:00:00.000'::timestamp
                OR {{ created_at_column }} = '1900-01-01 00:00:00.000'::timestamp
            THEN null
            ELSE {{ created_at_column }}
        END as _created_at,                                 -- Original creation timestamp
        {%- elif fallback_created_at_column and fallback_created_at_column != none and fallback_created_at_column != '' %}
        CASE 
            WHEN {{ fallback_created_at_column }} = '0001-01-01'::date 
                OR {{ fallback_created_at_column }} = '1900-01-01'::date 
                OR {{ fallback_created_at_column }} = '0001-01-01 00:00:00.000'::timestamp
                OR {{ fallback_created_at_column }} = '1900-01-01 00:00:00.000'::timestamp
            THEN null
            ELSE {{ fallback_created_at_column }}
        END as _created_at,                                 -- Fallback creation timestamp
        {%- else %}
        null as _created_at,                                -- No creation timestamp available
        {%- endif %}
        
        {%- if updated_at_column and updated_at_column != none and updated_at_column != '' %}
        CASE 
            WHEN {{ updated_at_column }} = '0001-01-01'::date 
                OR {{ updated_at_column }} = '1900-01-01'::date 
                OR {{ updated_at_column }} = '0001-01-01 00:00:00.000'::timestamp
                OR {{ updated_at_column }} = '1900-01-01 00:00:00.000'::timestamp
            THEN {%- if created_at_column and created_at_column != none and created_at_column != '' %} {{ created_at_column }}
                 {%- elif fallback_created_at_column and fallback_created_at_column != none and fallback_created_at_column != '' %} {{ fallback_created_at_column }}
                 {%- else %} null {%- endif %}
            ELSE COALESCE({{ updated_at_column }}, 
                         {%- if created_at_column and created_at_column != none and created_at_column != '' %} {{ created_at_column }}
                         {%- elif fallback_created_at_column and fallback_created_at_column != none and fallback_created_at_column != '' %} {{ fallback_created_at_column }}
                         {%- else %} null {%- endif %})
        END as _updated_at,                                 -- Last update timestamp
        {%- else %}
        {%- if created_at_column and created_at_column != none and created_at_column != '' %}
        {{ created_at_column }} as _updated_at,             -- Fallback to created timestamp
        {%- elif fallback_created_at_column and fallback_created_at_column != none and fallback_created_at_column != '' %}
        {{ fallback_created_at_column }} as _updated_at,    -- Fallback to fallback created timestamp  
        {%- else %}
        null as _updated_at,                                -- No timestamp available
        {%- endif %}
        {%- endif %}
        
        {%- if created_by_column and created_by_column != none and created_by_column != '' %}
        NULLIF({{ created_by_column }}, 0) as _created_by_user_id -- User who created the record
        {%- else %}
        null as _created_by_user_id                         -- No user tracking available
        {%- endif %}
{% endmacro %} 