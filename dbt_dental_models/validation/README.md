# Validation Queries

This directory contains SQL validation queries for validating marts and intermediate models in the dbt project.

## Purpose

Validation queries are used to:
- Verify data completeness and accuracy
- Compare source data with transformed data
- Identify data quality issues
- Validate business logic and calculations
- Ensure referential integrity

## Structure

```
validation/
├── README.md (this file)
├── marts/           # Validation queries for mart models
│   ├── fact_claim_validation_queries.sql
│   └── ...
└── intermediate/    # Validation queries for intermediate models
    └── ...
```

## Usage

These queries are designed to be run directly in your database client (e.g., DBeaver, pgAdmin) or via dbt's `dbt run-operation` command.

### Running Validation Queries

1. **Direct SQL Execution**: Copy and run queries in your database client
2. **dbt Operations**: Use `dbt run-operation` to execute queries programmatically
3. **Automated Testing**: Integrate into CI/CD pipelines for automated validation

## Validation Best Practices

1. **Document Expected Results**: Each validation query should document expected results
2. **Use Consistent Patterns**: Follow established validation patterns for consistency
3. **Include Diagnostics**: Add diagnostic queries to help troubleshoot failures
4. **Version Control**: Keep validation queries in sync with model changes
5. **Regular Execution**: Run validations regularly (daily/weekly) to catch issues early

## Related Documentation

- Validation plans: See `docs/dbt/` for detailed validation plans
- Analysis queries: See `analysis/` for exploratory analysis queries
- dbt tests: See `tests/` for automated dbt tests
