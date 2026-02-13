# Dental Practice Analytics Platform

Data pipeline and analytics for OpenDental: ETL from MySQL into PostgreSQL, dbt models, and a web API/dashboard for reporting.

## Overview

- **ETL**: Replicates 432+ OpenDental tables to PostgreSQL with schema discovery and incremental loading
- **Analytics**: dbt project with 88 staging, 50+ intermediate, and 17 mart models
- **API**: FastAPI backend serving analytics and patient/appointment endpoints
- **Frontend**: React dashboard for revenue, AR, providers, and patients

## Architecture

### Data Flow
```
OpenDental (MySQL) → ETL → PostgreSQL → dbt → API / Dashboard
    432 tables        replication   warehouse   models
```

### ETL
- Schema discovery over 432 tables
- Incremental loading using timestamp columns
- Batched processing (1K–5K rows by table size)
- Validation and basic monitoring

### dbt (Analytics)

#### Staging (88 models)
- Standardized source data, metadata columns (`_loaded_at`, `_transformed_at`, `_created_by`), validation

#### Intermediate (50+ models)
- Cross-system: patient financial/treatment journey; system-specific: fees, insurance, payments, AR, collections, communications, scheduling

#### Marts (17 models)
- Dimensions: Patient, Provider, Procedure, Insurance, Date
- Facts: Appointment, Claim, Payment, Communication
- Summaries: production, AR, revenue lost, provider performance, patient retention

## Stack

- **Source**: MariaDB/MySQL (OpenDental)
- **Warehouse**: PostgreSQL
- **ETL**: Python, CLI in `etl_pipeline/cli/`
- **Transform**: dbt Core
- **API**: FastAPI (OpenAPI, API key auth, rate limiting, CORS)
- **Frontend**: React, TypeScript, Material-UI, Recharts

## API

### FastAPI backend

- **Endpoints**: Patients, appointments, reports (revenue, providers, dashboard KPIs, AR)
- **Auth**: API key in `X-API-Key` header; **rate limits**: 60/min, 1000/hour by IP
- **Security**: CORS, request logging, Pydantic validation, parameterized queries. See `api/README.md` for details.
- **Docs**: OpenAPI at `/docs`

Optional hosted API (sample data): [https://api.dbtdentalclinic.com](https://api.dbtdentalclinic.com) (EC2 + ALB, HTTPS via ACM).

### Frontend (React + TypeScript)

- **Pages**: Dashboard (KPIs), Revenue, AR aging, Providers, Patients, Appointments, treatment acceptance
- **Stack**: React, Material-UI, Zustand, Recharts, Axios; React Router
- **Security**: Error sanitization, PII handling, search engines blocked via robots.txt

Optional hosted frontend (sample data): [https://dbtdentalclinic.com](https://dbtdentalclinic.com) (S3 + CloudFront).

### Project layout
```
dbt_dental_clinic/
├── etl_pipeline/              # ETL from MySQL to PostgreSQL
│   ├── elt_pipeline.py
│   ├── mysql_replicator.py
│   ├── cli/                   # CLI (main.py, commands.py, etl_functions.ps1)
│   ├── core/                  # Schema discovery, connections
│   ├── config/tables.yaml     # Table config
│   ├── synthetic_data_generator/  # Synthetic test data (see QUICKSTART.md)
│   └── logs/
├── dbt_dental_models/         # dbt project (staging, intermediate, marts)
├── api/                       # FastAPI (routers, models, services, database.py)
├── frontend/                  # React app (src/pages, components, services)
├── scripts/
│   ├── environment_manager.ps1   # dbt-init, etl-init, api-init, frontend-deploy, env-status
│   └── deployment/
├── docs/
└── tests/
```



## Synthetic data

The `etl_pipeline/synthetic_data_generator/` creates synthetic OpenDental-like data (Faker-based, no real PHI) for development and testing. Configurable patient count; maintains referential integrity and basic dental workflow (appointments, procedures, claims, payments). See `etl_pipeline/synthetic_data_generator/QUICKSTART.md`.

## Deployment (optional)

Deployment is optional; the app can run locally against a PostgreSQL warehouse.

**Frontend (S3 + CloudFront):** Use the `frontend-deploy` command from `scripts/environment_manager.ps1`. It builds the React app, uploads to S3, and invalidates CloudFront. Set `FRONTEND_BUCKET_NAME`, `FRONTEND_DIST_ID`, and `FRONTEND_DOMAIN` (env or `.frontend-deploy.json`). Other commands: `dbt-init`, `etl-init`, `api-init`, `frontend-status`, `env-status`.

**Backend (EC2 + ALB):** API can be run on EC2 behind an ALB with RDS PostgreSQL; see `docs/DEPLOYMENT_WORKFLOW.md` and deployment scripts in `scripts/deployment/`. Hosted sample API: [https://api.dbtdentalclinic.com](https://api.dbtdentalclinic.com); frontend: [https://dbtdentalclinic.com](https://dbtdentalclinic.com). Demo uses synthetic data only; no production OpenDental connection.

**Environment files:** The repo uses many `.env` and `.env_*` files (API, ETL, dbt, frontend, Docker). For a single reference and inventory script, see [docs/ENVIRONMENT_FILES.md](docs/ENVIRONMENT_FILES.md). Run `.\scripts\list_env_files.ps1` to see which env files exist.

