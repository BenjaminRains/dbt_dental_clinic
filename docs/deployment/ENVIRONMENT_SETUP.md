# Environment Setup Guide

**Day-to-day SoT for env files and loaders:** [ENVIRONMENT_FILES.md](ENVIRONMENT_FILES.md) (§2.2 for stage naming)  
**CLI:** [`tools/mdc_cli/README.md`](../../tools/mdc_cli/README.md) (`mdc api|etl|dbt … --env <stage>`)  
**Phase history (archived):** [archive/ENVIRONMENT_HANDLING_REVIEW.md](archive/ENVIRONMENT_HANDLING_REVIEW.md)  
**Naming history (archived):** [archive/ENVIRONMENT_NAMING_CONVENTION.md](archive/ENVIRONMENT_NAMING_CONVENTION.md)

This page is a **setup cheat sheet** (local / EC2 helpers + Snowflake). Prefer `mdc` for API, ETL, and dbt — do not use archived `environment_manager.ps1` / `*-init` for new work.

---

## Stages (quick map)

| Stage | Meaning | Typical command |
|-------|---------|-----------------|
| `local` | Local Postgres warehouse | `mdc dbt run --env local` |
| `demo` | Portfolio synthetic (Postgres demo) | `mdc dbt run --env demo` |
| `clinic` | Clinic RDS (PHI) | `mdc dbt run --env clinic --tunnel-db` |
| `snowflake` | Portfolio Snowflake mini warehouse | `mdc dbt validate --env snowflake` |
| `test` | CI / test DBs (API + ETL) | `mdc etl validate --env test` |

Full inventory and authority matrix: [ENVIRONMENT_FILES.md](ENVIRONMENT_FILES.md).

---

## Snowflake portfolio warehouse

Synthetic / demo only — never load clinic PHI into Snowflake.

### 1. Env file (component-scoped)

```powershell
copy dbt_dental_models\.env_snowflake.template dbt_dental_models\.env_snowflake
```

Fill at least:

| Variable | Purpose |
|----------|---------|
| `SNOWFLAKE_ACCOUNT` | Account id (e.g. `orgname-account`) |
| `SNOWFLAKE_USER` | Login user (e.g. `CONCRETE1866`) |
| `SNOWFLAKE_PRIVATE_KEY_PATH` | Path to `dbt_dental_models/.snowflake/rsa_key.p8` |
| `SNOWFLAKE_ROLE` | Usually `TRANSFORMER` (use `ACCOUNTADMIN` only for bootstrap) |
| `SNOWFLAKE_WAREHOUSE` | `WH_DEMO_XS` |
| `SNOWFLAKE_DATABASE` | `OPENDENTAL_SF` |

Auth is **key-pair preferred** (Snowsight passkey login does not work from Python/dbt). Password is optional fallback only.

Demo Postgres for export stays on:

`etl_pipeline/synthetic_data_generator/.env_demo` (`DEMO_POSTGRES_*`)

Do **not** duplicate `DEMO_POSTGRES_*` or `POSTGRES_ANALYTICS_*` into `.env_snowflake`.

### 2. Bootstrap + key-pair

Follow [docs/snowflake/SETUP.md](../snowflake/SETUP.md):

1. Run [`docs/snowflake/sql/01_bootstrap.sql`](../snowflake/sql/01_bootstrap.sql) in Snowsight (one statement at a time)
2. Attach public key via `dbt_dental_models/.snowflake/set_rsa_public_key.sql`
3. Confirm `profiles.yml` includes the `snowflake` target from `profiles.yml.template`

### 3. Verify + run

```powershell
mdc dbt validate --env snowflake
mdc dbt invoke --env snowflake -- debug

# Wave 1 export (uses .env_snowflake + generator .env_demo)
python scripts/snowflake/export_demo_to_snowflake.py --wave wave1_payments

mdc dbt invoke --env snowflake -- build --select tag:snowflake
```

Plan: [docs/snowflake/SNOWFLAKE_INTEGRATION_PLAN.md](../snowflake/SNOWFLAKE_INTEGRATION_PLAN.md).

---

## Postgres warehouses (summary)

### Local / clinic analytics (`POSTGRES_ANALYTICS_*`)

| Stage | Authority |
|-------|-----------|
| `local` | `dbt_dental_models/.env_local` |
| `clinic` | `deployment_credentials.json` → `clinic_database.postgresql` (+ live RDS password via Secrets Manager / `mdc secrets pull clinic`) |

```powershell
mdc dbt validate --env local
mdc tunnel clinic-db   # other terminal
mdc dbt run --env clinic --tunnel-db
```

### Demo Postgres (`DEMO_POSTGRES_*` / generator)

| Authority | File |
|-----------|------|
| Synthetic generator + Snowflake export source | `etl_pipeline/synthetic_data_generator/.env_demo` |
| dbt `--env demo` | `deployment_credentials.json` → `demo_database.postgresql` (via `mdc`) |

---

## Deployment helper scripts (EC2 / analysis)

Loads connection env vars for helpers under `deployment/`. Prefer resolving live passwords via AWS Secrets Manager when a script already does that — do not rewrite those to read passwords from the credentials file snapshot.

### PowerShell (Windows)

```powershell
# Thin fallback loader for deployment analysis scripts
. .\deployment\environment\setup_ec2_environment.ps1

python deployment/analysis/analyze_production_volumes.py
```

### Bash (Linux/Mac/WSL / EC2)

```bash
source deployment/environment/setup_ec2_environment.sh
python deployment/analysis/analyze_production_volumes.py
```

### EC2 instance environment

```bash
aws ssm start-session --target <API_EC2_INSTANCE_ID>
cd /opt/dbt_dental_clinic
source deployment/environment/setup_ec2_environment.sh
```

Optional persistent:

```bash
echo 'source /opt/dbt_dental_clinic/deployment/environment/setup_ec2_environment.sh' >> ~/.bashrc
```

### Variable sources (deployment credentials / RDS)

#### Production analytics (`opendental_analytics`)

| Variable | Source |
|----------|--------|
| `POSTGRES_ANALYTICS_HOST` | `backend_api.rds.endpoint` or `clinic_database.postgresql.host` |
| `POSTGRES_ANALYTICS_PORT` | `5432` (or credentials) |
| `POSTGRES_ANALYTICS_DB` | `opendental_analytics` |
| `POSTGRES_ANALYTICS_USER` | credentials / `clinic_database.postgresql.user` |
| `POSTGRES_ANALYTICS_PASSWORD` | Secrets Manager when script uses SM; else credentials snapshot |

#### Demo database (`opendental_demo`)

| Variable | Source |
|----------|--------|
| `POSTGRES_DEMO_*` / `DEMO_POSTGRES_*` | `demo_database` in `deployment_credentials.json` or generator `.env_demo` |

#### Local via SSM tunnel

| Variable | Value |
|----------|-------|
| Host / port | `localhost` / tunnel port (e.g. `5433`) |
| DB | `opendental_analytics` |
| User / password | From credentials / Secrets Manager |

```powershell
mdc tunnel clinic-db
# then mdc dbt|etl|api … --env clinic --tunnel-db
```

---

## See also

- [ENVIRONMENT_FILES.md](ENVIRONMENT_FILES.md) — inventory, loaders, precedence
- [CLINIC_ANALYTICS_WORKFLOW.md](CLINIC_ANALYTICS_WORKFLOW.md) — local dbt → clinic RDS
- [docs/snowflake/SETUP.md](../snowflake/SETUP.md) — Snowflake account, bootstrap, key-pair
- [README_CREDENTIALS.md](README_CREDENTIALS.md), [deployment/README.md](../../deployment/README.md)
