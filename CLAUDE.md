# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

### Build & Run
- Run DBT models: `./scripts/run_dbt.sh run` (Linux/Mac) or `.\scripts\run_dbt.bat run` (Windows)
- Run specific models: `./scripts/run_dbt.sh run --select model_name`
- Test models: `./scripts/run_dbt.sh test` or `./scripts/run_dbt.sh test --select model_name`
- Python formatting: `pipenv run format` (Black)
- Python linting: `pipenv run lint` (Pylint)
- Sort imports: `pipenv run sort-imports` (isort)
- Run tests: `pipenv run test` (pytest)

## Code Style Guidelines

### SQL
- Source columns: Use **CamelCase** for raw database columns (e.g., "PatNum")
- Derived fields: Use **snake_case** for transformed fields
- CTEs: Use **CamelCase** for ALL CTEs (project-specific convention)
- File names: Use **snake_case** for all SQL files
- Boolean conversions: Use CASE statements for smallint to boolean conversions

### Python
- Follow standard Python style (Black formatter)
- Python version: 3.11
- Use proper type annotations

### Directory Structure
- models/staging/: Raw data transformations
- models/intermediate/: Business logic by system (A-G)
- models/marts/: Business-specific data marts
- macros/: Reusable SQL templates
- tests/: Data quality tests