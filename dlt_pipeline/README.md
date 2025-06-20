# DLT Pipeline for OpenDental

## Project Structure
```
dlt_pipeline/
├── config/                     # Configuration files
│   ├── __init__.py
│   ├── tables.yml             # Table configurations and mappings
│   └── pipeline_config.yml    # Pipeline settings and parameters
│
├── sources/                    # Data source definitions
│   ├── __init__.py
│   ├── opendental_source.py   # OpenDental source connector
│   └── replication_source.py  # Replication database connector
│
├── scripts/                    # Pipeline execution scripts
│   ├── __init__.py
│   ├── run_pipeline.py        # Main pipeline execution script
│   └── setup_database.py      # Database setup and initialization
│
├── utils/                      # Utility functions and helpers
│   ├── __init__.py
│   ├── logging_utils.py       # Logging configuration
│   ├── db_utils.py           # Database utility functions
│   └── validation.py         # Data validation utilities
│
├── tests/                      # Test suite
│   ├── __init__.py
│   ├── dlt_test_connections.py    # Database connection tests
│   ├── test_pipeline.py      # Pipeline execution tests
│   └── test_utils.py         # Utility function tests
│
├── logs/                       # Log files directory
│   └── .gitkeep
│
├── .env.template              # Environment variables template
├── Pipfile                    # Python dependencies
├── requirements.txt           # Alternative Python dependencies
├── README.md                  # Project documentation
└── __init__.py               # Package initialization
```

## Database Architecture

The pipeline uses a three-database architecture:

1. **Source Database (OpenDental MySQL)**
   - Production database (read-only)
   - Contains raw OpenDental data
   - Accessed via `opendental_source.py`

2. **Replication Database (Local MySQL)**
   - Local copy of source data
   - Used for staging and transformation
   - Managed by `replication_source.py`

3. **Analytics Database (PostgreSQL)**
   - Final destination for analytics
   - Uses dlt for loading and transformation
   - Schema: raw, public_staging, public_intermediate, public_marts, public, public_dbt_test__audit

## Setup Instructions

1. Copy environment template:
   ```bash
   cp .env.template .env
   ```

2. Install dependencies:
   ```bash
   pipenv install
   ```

3. Run connection tests:
   ```bash
   pipenv run python tests/dlt_test_connections.py
   ```

4. Run the pipeline:
   ```bash
   pipenv run python scripts/run_pipeline.py
   ```

## Configuration

- `config/tables.yml`: Define table configurations, mappings, and transformations
- `config/pipeline_config.yml`: Pipeline settings, batch sizes, and scheduling
- `.env`: Database credentials and connection settings

## Logging

Logs are stored in the `logs/` directory with timestamps:
```
logs/
└── pipeline_YYYYMMDD_HHMMSS.log
```
