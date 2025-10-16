{% macro staging_model_cte_structure(source_name, table_name) %}
{#- 
    Provides standardized CTE structure for staging models
    
    Args:
        source_name (str): Source name (e.g., 'opendental')
        table_name (str): Table name (e.g., 'patient')
        
    Returns:
        Standard CTE structure using snake_case naming
        
    Example usage:
        {{ staging_model_cte_structure('opendental', 'patient') }}
-#}
with source_data as (
    select * from {{ source(source_name, table_name) }}
),

renamed_columns as (
    select
        -- Add your column transformations here
        -- Use {{ convert_opendental_boolean('"ColumnName"') }} for boolean fields
        -- Use {{ clean_opendental_date('"DateColumn"') }} for date fields
        -- Use {{ clean_opendental_string('"StringColumn"') }} for string fields
        
        -- Always include metadata at the end:
        {{ standardize_metadata_columns() }}
    from source_data
)

select * from renamed_columns
{% endmacro %}

{% macro generate_staging_model_docs(model_name, description, columns) %}
{#- 
    Generates standardized documentation structure for staging models
    
    Args:
        model_name (str): Model name (e.g., 'stg_opendental__patient')
        description (str): Model description
        columns (list): List of column documentation dicts
        
    Returns:
        YAML documentation structure
        
    Example usage:
        In your _stg_model.yml file, reference this for consistent formatting
-#}
version: 2

models:
  - name: {{ model_name }}
    description: {{ description }}
    columns:
      {% for column in columns %}
      - name: {{ column.name }}
        description: {{ column.description }}
        {% if column.tests %}
        tests:
          {% for test in column.tests %}
          - {{ test }}
          {% endfor %}
        {% endif %}
      {% endfor %}
      
      # Standard metadata columns (always include these)
      - name: _loaded_at
        description: "Timestamp when record was loaded by ETL pipeline"
      - name: _created_at  
        description: "Original creation timestamp from source system"
      - name: _updated_at
        description: "Last update timestamp from source system"
      - name: _created_by_user_id
        description: "ID of user who created the record in source system"
{% endmacro %} 