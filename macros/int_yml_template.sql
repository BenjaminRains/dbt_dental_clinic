# Template for intermediate model documentation based on your 5-template system

{% macro generate_intermediate_yml(
    model_name,
    template_type,
    system_name,
    business_purpose,
    key_features,
    data_sources,
    business_logic,
    data_quality_notes,
    owner,
    contains_pii=true,
    business_process,
    refresh_frequency,
    business_impact="High",
    sla_requirements={
        'freshness': {'warn_after': '24h', 'error_after': '48h'},
        'completeness': {'threshold': 0.95},
        'accuracy': {'threshold': 0.99},
        'timeliness': {'threshold': '1h'}
    },
    data_quality_thresholds={
        'null_rates': {'critical': 0.01, 'warning': 0.05},
        'duplicate_rates': {'critical': 0.001, 'warning': 0.01},
        'freshness': {'critical': '24h', 'warning': '12h'}
    }
) %}

version: 2

models:
  - name: {{ model_name }}
    description: >
      {{ business_purpose }}
      
      This model serves as {{ get_template_role(template_type) }} and provides {{ get_template_value(template_type) }}.
      Part of System {{ system_name }}: {{ get_system_name(system_name) }} workflow.
      
      Key Features:
      {% for feature in key_features %}
      - {{ feature.name }}: {{ feature.description }}
      {% endfor %}
      
      Data Sources:
      {% for source in data_sources %}
      - {{ source.model }}: {{ source.purpose }}
      {% endfor %}
      
      Business Logic Features:
      {% for logic in business_logic %}
      - {{ logic.feature }}: {{ logic.implementation }}
      {% endfor %}
      
      {% if template_type == 'financial' %}
      Financial Calculations:
      - Amount categorization: Large (>$1000), Medium ($100-$1000), Small (<$100)
      - Validation rules: Non-negative amounts, valid account codes
      - Payment allocation: Complex multi-payer distribution logic
      {% elif template_type == 'clinical' %}
      Clinical Calculations:
      - Appointment efficiency: Duration vs allocated time
      - Provider utilization: Scheduled vs available time
      - Patient satisfaction: Wait time and service quality metrics
      {% elif template_type == 'insurance' %}
      Insurance Calculations:
      - Coverage determination: Plan benefits and patient eligibility
      - Deductible tracking: Annual accumulation and reset logic
      - Claims processing: Status tracking and payment allocation
      {% elif template_type == 'system' %}
      System Calculations:
      - Activity tracking: User actions and system events
      - Performance metrics: Response times and error rates
      - Security monitoring: Access patterns and anomaly detection
      {% elif template_type == 'metrics' %}
      Metrics Calculations:
      - KPI aggregation: Time-based performance indicators
      - Trend analysis: Period-over-period comparisons
      - Efficiency ratios: Resource utilization and productivity
      {% endif %}
      
      Data Quality Notes:
      {% for note in data_quality_notes %}
      - {{ note.issue }}: {{ note.description }}, {{ note.impact }}, {{ note.mitigation }}
      {% endfor %}
      
      Business Rules:
      {% for rule in get_template_business_rules(template_type) %}
      - {{ rule.name }}: {{ rule.implementation }}
      {% endfor %}
    
    config:
      materialized: {{ get_template_materialization(template_type) }}
      schema: intermediate
      unique_key: {{ model_name.split('_')[1] }}_id
      
      # Data Quality Configuration
      data_quality:
        freshness:
          warn_after: {{ sla_requirements.freshness.warn_after }}
          error_after: {{ sla_requirements.freshness.error_after }}
        completeness:
          threshold: {{ sla_requirements.completeness.threshold }}
        accuracy:
          threshold: {{ sla_requirements.accuracy.threshold }}
        timeliness:
          threshold: {{ sla_requirements.timeliness.threshold }}
    
    columns:
      # Primary key following naming conventions
      - name: {{ model_name.split('_')[1] }}_id
        description: >
          Primary key - {{ get_primary_key_description(template_type) }}
          (maps from "{{ get_source_column_name(model_name) }}Num" in OpenDental)
          
          Source Transformation:
          - OpenDental source: "{{ get_source_column_name(model_name) }}Num" (CamelCase)
          - Transformed to: {{ model_name.split('_')[1] }}_id (snake_case with _id suffix)
          - Transformation rule: All columns ending in "Num" become "_id" fields
        tests:
          - unique
          - not_null
          - positive_values
          - dbt_expectations.expect_column_values_to_not_be_null:
              config:
                severity: error
                description: "Primary key must never be null"
      
      # Standard foreign keys
      {% if 'patient' in template_type or template_type != 'system' %}
      - name: patient_id
        description: >
          Foreign key to patient (maps from "PatNum" in OpenDental)
          
          Source Transformation:
          - OpenDental source: "PatNum" (CamelCase as stored)
          - Transformed to: patient_id (snake_case per naming conventions)
        tests:
          - relationships:
              to: ref('int_patient_profile')
              field: patient_id
              config:
                severity: error
                description: "All records must reference valid patients"
          - dbt_expectations.expect_column_values_to_not_be_null:
              config:
                severity: error
                description: "Patient ID must never be null"
      {% endif %}
      
      {% if template_type in ['clinical', 'financial'] %}
      - name: provider_id
        description: >
          Foreign key to provider (maps from "ProvNum" in OpenDental)
          
          Source Transformation:
          - OpenDental source: "ProvNum" (CamelCase as stored)  
          - Transformed to: provider_id (snake_case per naming conventions)
        tests:
          - relationships:
              to: ref('int_provider_profile')
              field: provider_id
              config:
                severity: warn
                description: "Most records should have provider assignment"
          - dbt_expectations.expect_column_values_to_not_be_null:
              config:
                severity: warn
                description: "Provider ID should not be null for clinical/financial records"
      {% endif %}
      
      # Template-specific columns
      {% if template_type == 'financial' %}
      - name: amount
        description: >
          Financial amount in USD with business validation
          
          Business Rules:
          - Must be non-negative for most transaction types
          - Validated against chart of accounts
          - Supports up to 2 decimal places for cents
        tests:
          - not_null
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: -10000
              max_value: 50000
              config:
                description: "Financial amounts within reasonable business range"
                severity: error
      
      - name: amount_category
        description: >
          Amount categorization for business reporting
          
          Categories:
          - 'large': Amount > $1,000 (high-value transactions)
          - 'medium': Amount $100-$1,000 (standard transactions)  
          - 'small': Amount < $100 (minor transactions)
          - 'other': Edge cases and adjustments
        tests:
          - accepted_values:
              values: ['large', 'medium', 'small', 'other']
              config:
                severity: error
                description: "Amount category must be one of the defined values"
      {% endif %}
      
      # Standard metadata fields
      - name: _extracted_at
        description: >
          ETL pipeline extraction timestamp - when the record was extracted from the source system
          
          Source: ETL pipeline metadata (added during extraction process)
          Purpose: Data lineage tracking and pipeline monitoring
        tests:
          - not_null
          - dbt_expectations.expect_column_values_to_not_be_null:
              config:
                severity: error
                description: "Extraction timestamp must never be null"
      
      - name: _created_at  
        description: >
          Original creation timestamp from OpenDental source system
          
          Source Transformation:
          - OpenDental source: "DateEntry" (CamelCase as stored)
          - Represents: When the record was originally created in OpenDental
        tests:
          - not_null
          - dbt_expectations.expect_column_values_to_not_be_null:
              config:
                severity: error
                description: "Creation timestamp must never be null"
      
      - name: _updated_at
        description: >
          Last update timestamp from OpenDental source system
          
          Source Transformation:
          - OpenDental source: COALESCE("DateTStamp", "DateEntry") 
          - Logic: Uses DateTStamp if available, falls back to DateEntry
        tests:
          - not_null
          - dbt_expectations.expect_column_values_to_not_be_null:
              config:
                severity: error
                description: "Update timestamp must never be null"
      
      - name: _transformed_at
        description: >
          dbt model processing timestamp - when this intermediate model was last run
          
          Source: current_timestamp at dbt model execution
          Purpose: Model execution tracking and debugging
        tests:
          - not_null
          - dbt_expectations.expect_column_values_to_not_be_null:
              config:
                severity: error
                description: "Transformation timestamp must never be null"
    
    tests:
      # Template-specific model tests
      {% if template_type == 'financial' %}
      - dbt_expectations.expect_table_row_count_to_be_between:
          min_value: 1000
          max_value: 1000000
          config:
            description: "Financial transaction volume within expected range"
            severity: error
      
      - dbt_utils.expression_is_true:
          expression: "sum(case when amount < 0 then 1 else 0 end) / count(*) < 0.1"
          config:
            description: "Less than 10% negative amounts (business rule)"
            severity: error
      {% elif template_type == 'metrics' %}
      - dbt_expectations.expect_table_row_count_to_be_between:
          min_value: 100
          max_value: 100000
          config:
            description: "Metrics aggregation within expected range"
            severity: error
      {% endif %}
      
      # Universal model tests
      - dbt_utils.expression_is_true:
          expression: "_updated_at >= _created_at"
          config:
            description: "Update timestamp must be >= creation timestamp"
            severity: error
    
    meta:
      owner: "{{ owner }}"
      contains_pii: {{ contains_pii }}
      business_process: "{{ business_process }}"
      refresh_frequency: "{{ refresh_frequency }}"
      business_impact: "{{ business_impact }}"
      system_integration: "System {{ system_name }}: {{ get_system_name(system_name) }}"
      data_quality_requirements:
        {% for req in get_template_quality_requirements(template_type) %}
        - "{{ req }}"
        {% endfor %}
      sla_requirements:
        freshness:
          warn_after: {{ sla_requirements.freshness.warn_after }}
          error_after: {{ sla_requirements.freshness.error_after }}
        completeness:
          threshold: {{ sla_requirements.completeness.threshold }}
        accuracy:
          threshold: {{ sla_requirements.accuracy.threshold }}
        timeliness:
          threshold: {{ sla_requirements.timeliness.threshold }}
      data_quality_thresholds:
        null_rates:
          critical: {{ data_quality_thresholds.null_rates.critical }}
          warning: {{ data_quality_thresholds.null_rates.warning }}
        duplicate_rates:
          critical: {{ data_quality_thresholds.duplicate_rates.critical }}
          warning: {{ data_quality_thresholds.duplicate_rates.warning }}
        freshness:
          critical: {{ data_quality_thresholds.freshness.critical }}
          warning: {{ data_quality_thresholds.freshness.warning }}

{% endmacro %}

{# Helper macros for template-specific content #}
{% macro get_template_role(template_type) %}
  {% if template_type == 'financial' %}
    {{ return('financial data processing and business rule enforcement') }}
  {% elif template_type == 'clinical' %}
    {{ return('clinical workflow optimization and patient care tracking') }}
  {% elif template_type == 'insurance' %}
    {{ return('insurance coverage management and claims processing') }}
  {% elif template_type == 'system' %}
    {{ return('system monitoring and audit trail maintenance') }}
  {% elif template_type == 'metrics' %}
    {{ return('performance metrics aggregation and KPI calculation') }}
  {% endif %}
{% endmacro %}

{% macro get_system_name(system_code) %}
  {% set systems = {
    'A': 'Fee Processing & Verification',
    'B': 'Insurance & Claims Processing',
    'C': 'Payment Allocation & Reconciliation', 
    'D': 'AR Analysis',
    'E': 'Collections',
    'F': 'Communications',
    'G': 'Scheduling'
  } %}
  {{ return(systems[system_code]) }}
{% endmacro %}

{% macro get_template_business_rules(template_type) %}
  {% if template_type == 'financial' %}
    {{ return([
      {'name': 'Amount Validation', 'implementation': 'Non-negative amounts with business exception handling'},
      {'name': 'Account Code Validation', 'implementation': 'Valid chart of accounts reference required'},
      {'name': 'Payment Allocation', 'implementation': 'Multi-payer distribution with remainder handling'}
    ]) }}
  {% elif template_type == 'clinical' %}
    {{ return([
      {'name': 'Provider Assignment', 'implementation': 'Valid provider required for clinical procedures'},
      {'name': 'Appointment Scheduling', 'implementation': 'No double-booking and resource conflict prevention'},
      {'name': 'Patient Safety', 'implementation': 'Required safety checks and documentation'}
    ]) }}
  {% else %}
    {{ return([
      {'name': 'Data Integrity', 'implementation': 'Required field validation and referential integrity'},
      {'name': 'Business Logic', 'implementation': 'Domain-specific validation rules'}
    ]) }}
  {% endif %}
{% endmacro %}