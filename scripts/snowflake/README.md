# Snowflake mini warehouse scripts

Export synthetic demo Postgres tables into Snowflake `OPENDENTAL_SF.RAW` for the
payments/collections portfolio slice.

## Environment (repo convention)

Follow [docs/deployment/ENVIRONMENT_FILES.md](../../docs/deployment/ENVIRONMENT_FILES.md):

| Need | Authority |
|------|-----------|
| Snowflake (`SNOWFLAKE_*`) | `dbt_dental_models/.env_snowflake` (from `.env_snowflake.template`) |
| Demo Postgres export source (`DEMO_POSTGRES_*`) | `etl_pipeline/synthetic_data_generator/.env_demo` |
| dbt runs | `mdc dbt … --env snowflake` (loads `.env_snowflake` via `mdc_cli/dbt_env.py`) |

Do **not** put secrets under `scripts/snowflake/` (legacy path warns if present).

## Setup

1. Complete [docs/snowflake/SETUP.md](../../docs/snowflake/SETUP.md) (account + bootstrap).
2. Install deps: `pip install -r scripts/snowflake/requirements.txt`
3. Copy env template:

```powershell
copy dbt_dental_models\.env_snowflake.template dbt_dental_models\.env_snowflake
```

4. Fill `SNOWFLAKE_*`. Ensure generator `.env_demo` has demo Postgres creds.
5. Bootstrap (ACCOUNTADMIN role in `.env_snowflake`):

```powershell
python scripts/snowflake/bootstrap_snowflake.py
```

Then set `SNOWFLAKE_ROLE=TRANSFORMER`.

## Export Wave 1

```powershell
# From repo root
python scripts/snowflake/export_demo_to_snowflake.py --dry-run
python scripts/snowflake/export_demo_to_snowflake.py --wave wave1_payments
```

Table lists: [`export_tables.yml`](export_tables.yml) (add waves as the warehouse grows).

## Safety

The script **refuses** clinic / analytics database names. Source DB must match
`opendental_demo*`.

## Then dbt

```powershell
mdc dbt validate --env snowflake
mdc dbt invoke --env snowflake -- build --select tag:snowflake
```
