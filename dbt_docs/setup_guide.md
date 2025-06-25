{% docs setup_guide %}

# DBT Documentation Setup Guide

## Overview

This guide explains how to set up, generate, and maintain comprehensive documentation for the 
Dental Clinic Analytics Platform using DBT's built-in documentation features.

## 🚀 Quick Start

### 1. Generate Documentation
```bash
# Generate documentation files
dbt docs generate

# Serve documentation locally
dbt docs serve

# Or serve on specific port
dbt docs serve --port 8080
```

### 2. View Documentation
- Open browser to `http://localhost:8080`
- Navigate through model documentation
- View lineage graphs and dependencies
- Check test coverage and results

## 📋 Documentation Structure

### Core Documentation Files
```
dbt_docs/
├── index.md              # Main landing page
├── architecture.md       # Technical architecture
├── data_quality.md       # Quality framework
├── setup_guide.md        # This guide
├── dental_process_overview.md  # Business processes
└── payment.md            # Payment-specific docs
```

### Model Documentation
```
models/
├── staging/
│   └── opendental/
│       ├── _stg_opendental__patient.yml    # Patient model docs
│       ├── _stg_opendental__appointment.yml # Appointment docs
│       └── ...
├── intermediate/
│   └── _intermediate.yml  # Intermediate model docs
└── marts/
    ├── _marts.yml        # Mart model docs
    ├── _dim_patient.yml  # Patient dimension docs
    └── ...
```

## 🔧 Configuration

### 1. dbt_project.yml Configuration
```yaml
# Documentation paths
docs-paths: ["dbt_docs"]

# Model documentation settings
models:
  dbt_dental_clinic:
    +persist_docs:
      relation: true    # Persist docs to database
      columns: true     # Persist column docs
```

### 2. Model Documentation Structure
```yaml
version: 2

models:
  - name: dim_patient
    description: "Patient dimension table containing demographic and contact information"
    columns:
      - name: patient_id
        description: "Unique identifier for each patient"
        tests:
          - not_null
          - unique
      - name: first_name
        description: "Patient's first name"
        tests:
          - not_null
      - name: last_name
        description: "Patient's last name"
        tests:
          - not_null
      - name: email
        description: "Patient's email address for communications"
        tests:
          - not_null
          - unique
```

## 📝 Documentation Best Practices

### 1. Model Documentation
- **Clear Descriptions**: Explain what the model represents
- **Business Context**: Why this model exists
- **Column Documentation**: Every column should have a description
- **Business Rules**: Document important business logic
- **Examples**: Provide usage examples where helpful

### 2. Column Documentation
```yaml
- name: payment_amount
  description: "Total payment amount in USD. Positive values indicate payments received, negative values indicate refunds or adjustments."
  tests:
    - not_null
    - dbt_expectations.expect_column_values_to_be_between:
        min_value: -10000
        max_value: 10000
```

### 3. Test Documentation
```yaml
- name: payment_amount
  description: "Payment amount with business rule validation"
  tests:
    - name: payment_amount_positive_for_cash
      description: "Cash payments must be positive amounts"
      config:
        severity: error
```

## 🔄 Documentation Workflow

### 1. Development Workflow
```bash
# 1. Make model changes
# 2. Update YAML documentation
# 3. Generate and review docs
dbt docs generate
dbt docs serve

# 4. Commit changes
git add .
git commit -m "Update model documentation"
```

### 2. CI/CD Integration
```yaml
# .github/workflows/docs.yml
name: Generate Documentation
on: [push, pull_request]
jobs:
  docs:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Generate DBT docs
        run: |
          dbt deps
          dbt docs generate
      - name: Upload docs artifact
        uses: actions/upload-artifact@v2
        with:
          name: dbt-docs
          path: target/
```

### 3. Documentation Deployment
```bash
# Deploy to static hosting (e.g., GitHub Pages)
dbt docs generate
cp -r target/ docs/
git add docs/
git commit -m "Update documentation"
git push
```

## 📊 Documentation Features

### 1. Lineage Graphs
- **Visual Dependencies**: See how models connect
- **Impact Analysis**: Understand downstream effects
- **Dependency Tracking**: Identify critical paths

### 2. Model Coverage
- **Test Coverage**: See which models have tests
- **Documentation Coverage**: Track documentation completeness
- **Quality Metrics**: Monitor data quality scores

### 3. Search and Navigation
- **Full-Text Search**: Find models and columns quickly
- **Filtering**: Filter by tags, owners, or status
- **Bookmarking**: Save frequently accessed models

## 🛠️ Advanced Configuration

### 1. Custom Documentation Macros
```sql
-- macros/docs_helpers.sql
{% macro get_model_description(model_name) %}
  {% set model_docs = ref(model_name) %}
  {% if model_docs %}
    {{ model_docs.description }}
  {% else %}
    No description available for {{ model_name }}
  {% endif %}
{% endmacro %}
```

### 2. Documentation Templates
```yaml
# templates/model_template.yml
version: 2

models:
  - name: {{ model_name }}
    description: "{{ model_description }}"
    columns:
      - name: id
        description: "Primary key identifier"
        tests:
          - not_null
          - unique
```

### 3. Automated Documentation Generation
```python
# scripts/generate_docs.py
import yaml
import os

def generate_model_docs(model_path):
    """Generate basic documentation for a model"""
    model_name = os.path.basename(model_path).replace('.sql', '')
    
    doc_template = {
        'version': 2,
        'models': [{
            'name': model_name,
            'description': f'Documentation for {model_name}',
            'columns': []
        }]
    }
    
    return doc_template
```

## 📈 Documentation Metrics

### 1. Coverage Tracking
```sql
-- Documentation coverage query
SELECT 
    model_name,
    CASE 
        WHEN description IS NOT NULL THEN 1 
        ELSE 0 
    END as has_description,
    CASE 
        WHEN column_count = documented_columns THEN 1 
        ELSE 0 
    END as fully_documented
FROM model_documentation
```

### 2. Quality Metrics
- **Documentation Completeness**: % of models with descriptions
- **Column Documentation**: % of columns with descriptions
- **Test Coverage**: % of models with tests
- **Freshness**: How often docs are updated

## 🔍 Troubleshooting

### Common Issues

#### 1. Documentation Not Generating
```bash
# Check DBT project configuration
dbt debug

# Verify documentation paths
cat dbt_project.yml | grep docs-paths

# Check for YAML syntax errors
dbt parse
```

#### 2. Missing Model Documentation
```bash
# Generate docs for specific models
dbt docs generate --models model_name

# Check model YAML files
find models/ -name "*.yml" -exec cat {} \;
```

#### 3. Documentation Server Issues
```bash
# Check port availability
netstat -an | grep 8080

# Use different port
dbt docs serve --port 8081

# Check firewall settings
```

## 📚 Documentation Resources

### 1. DBT Documentation
- [DBT Documentation Guide](https://docs.getdbt.com/docs/building-a-dbt-project/documentation)
- [DBT Docs CLI Reference](https://docs.getdbt.com/reference/commands/cmd-docs)
- [DBT Best Practices](https://docs.getdbt.com/guides/best-practices)

### 2. Project-Specific Documentation
- [Architecture Overview](architecture)
- [Data Quality Framework](data_quality)
- [Business Process Overview](dental_process_overview)

### 3. External Tools
- [DBT Docs Hosting](https://docs.getdbt.com/docs/deploy/deploy-docs)
- [GitHub Pages Setup](https://pages.github.com/)
- [Netlify Deployment](https://www.netlify.com/)

## 🎯 Next Steps

### 1. Immediate Actions
- [ ] Generate initial documentation: `dbt docs generate`
- [ ] Review existing model documentation
- [ ] Update missing descriptions
- [ ] Add column-level documentation

### 2. Medium-term Goals
- [ ] Set up automated documentation deployment
- [ ] Create documentation templates
- [ ] Implement documentation quality checks
- [ ] Train team on documentation standards

### 3. Long-term Objectives
- [ ] Integrate with CI/CD pipeline
- [ ] Set up documentation hosting
- [ ] Create documentation metrics dashboard
- [ ] Establish documentation review process

---

*This setup guide provides a comprehensive approach to DBT documentation. Regular updates and
 reviews ensure documentation remains current and valuable.*

{% enddocs %} 