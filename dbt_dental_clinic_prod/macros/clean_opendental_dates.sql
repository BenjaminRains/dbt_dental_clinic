{% macro clean_opendental_date(column_name, default_null=true) %}
{#- 
    Cleans OpenDental date columns that use various invalid dates to represent NULL
    
    Args:
        column_name (str): The quoted column name from OpenDental (e.g., '"DateEntry"')
        default_null (bool): Whether to return null for invalid dates (default: true)
        
    Returns:
        SQL expression that converts invalid OpenDental dates to null or a default
        
    Example usage:
        {{ clean_opendental_date('"DateEntry"') }} as entry_date,
        {{ clean_opendental_date('"DateTimeDeceased"') }} as deceased_datetime
-#}
    CASE 
        WHEN {{ column_name }} = '0001-01-01 00:00:00.000'::timestamp
            OR {{ column_name }} = '1900-01-01 00:00:00.000'::timestamp
            OR {{ column_name }} = '0001-01-01'::date 
            OR {{ column_name }} = '1900-01-01'::date 
        THEN {% if default_null %}null{% else %}'1900-01-01'::date{% endif %}
        ELSE {{ column_name }}
    END
{% endmacro %}

{% macro clean_opendental_string(column_name, trim_whitespace=true) %}
{#- 
    Cleans OpenDental string columns by handling empty strings and whitespace
    
    Args:
        column_name (str): The quoted column name from OpenDental
        trim_whitespace (bool): Whether to trim whitespace (default: true)
        
    Returns:
        SQL expression that converts empty strings to null and optionally trims
        
    Example usage:
        {{ clean_opendental_string('"CheckNum"') }} as check_number,
        {{ clean_opendental_string('"PatNote"', false) }} as patient_notes
-#}
    NULLIF(
        {% if trim_whitespace %}TRIM({{ column_name }}){% else %}{{ column_name }}{% endif %}, 
        ''
    )
{% endmacro %} 