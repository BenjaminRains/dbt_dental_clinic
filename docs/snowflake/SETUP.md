# Snowflake account setup (Phase 1)

Portfolio / demo only. Do **not** connect clinic RDS or load PHI.

Env handling matches the rest of the repo — see
[ENVIRONMENT_FILES.md](../deployment/ENVIRONMENT_FILES.md).

| Need | Authority |
|------|-----------|
| Snowflake connection | `dbt_dental_models/.env_snowflake` via `mdc dbt --env snowflake` |
| Demo Postgres export source | `etl_pipeline/synthetic_data_generator/.env_demo` |
| Clinic / local Postgres | unchanged (`local` / `clinic` / `demo` stages) |

### Database naming

| Platform | Database | Role |
|----------|----------|------|
| Postgres (clinic) | `opendental_analytics` | Production warehouse |
| Postgres (demo) | `opendental_demo` | Synthetic portfolio |
| Snowflake | `OPENDENTAL_SF` | Dual-warehouse portfolio mini stack |

Do **not** name the Snowflake database `ANALYTICS` — that collides mentally with Postgres `opendental_analytics`.

## 1. Create a Snowflake trial

1. Go to [https://signup.snowflake.com/](https://signup.snowflake.com/)
2. Prefer **AWS** as cloud provider (matches existing AWS portfolio narrative)
3. Prefer a US region you already use for the project
4. Choose **Standard** edition (enough for this mini warehouse)
5. Save account identifier, username, and password somewhere safe (password manager)

Typical trial: ~$400 credits / ~30 days. Enough to build Wave 1–2 if the warehouse auto-suspends.

## 2. Run bootstrap SQL (Snowsight — preferred with passkey)

Password-based scripts often fail when you sign in with a **passkey**. Do Phase 1 in the UI:

1. Open https://igpxtll-rwc63998.snowflakecomputing.com and sign in (passkey OK)
2. Switch role to **ACCOUNTADMIN** (top-right / role selector)
3. **Projects → Worksheets → +** new SQL worksheet
4. Open [`sql/01_bootstrap.sql`](sql/01_bootstrap.sql) from the repo, paste into the worksheet
5. Run **one statement at a time**: highlight from `CREATE`/`GRANT`/`ALTER` through its `;`, then Run  
   (Avoid “run entire file” if you see *Multiple SQL statements in a single API call…*)
6. Finish with the sanity checks at the bottom (`SHOW DATABASES` / `SHOW WAREHOUSES` / `SHOW ROLES`)

Creates:

| Object | Purpose |
|--------|---------|
| `WH_DEMO_XS` | X-Small warehouse, auto-suspend 60s |
| `OPENDENTAL_SF` database | Snowflake mini warehouse (not Postgres `opendental_analytics`) |
| Schemas `RAW`, `STAGING`, `INT`, `MARTS`, `DBT` | Aligns with dbt `generate_schema_name` |
| Stage `RAW.DEMO_EXPORT` | Internal stage for COPY INTO |
| Role `TRANSFORMER` | dbt + load |
| Role `ANALYST` | Read marts |
| Resource monitor | Credit guardrail |

## 3. Key-pair auth for scripts / dbt (required with passkey login)

Snowsight passkeys do not work from Python/dbt. Use RSA key-pair:

1. Keys are generated under `dbt_dental_models/.snowflake/` (gitignored):
   - `rsa_key.p8` — private key (stays on your machine)
   - `set_rsa_public_key.sql` — one statement to run in Snowsight
2. In Snowsight as **ACCOUNTADMIN**, run the statement in `set_rsa_public_key.sql`
3. Confirm `.env_snowflake` has:
   - `SNOWFLAKE_USER=CONCRETE1866`
   - `SNOWFLAKE_PRIVATE_KEY_PATH=.../dbt_dental_models/.snowflake/rsa_key.p8`
4. Then from repo root:

```powershell
python -c "from pathlib import Path; from dotenv import load_dotenv; import sys; sys.path.insert(0,'scripts/snowflake'); load_dotenv('dbt_dental_models/.env_snowflake', interpolate=False); from sf_connect import connect_snowflake; c=connect_snowflake(); cur=c.cursor(); cur.execute('select current_user(), current_role()'); print(cur.fetchone()); c.close()"
```

After that works, set `SNOWFLAKE_ROLE=TRANSFORMER` and run `mdc dbt validate --env snowflake`.

## 4. Local secrets (component-scoped)

```powershell
copy dbt_dental_models\.env_snowflake.template dbt_dental_models\.env_snowflake
# Edit SNOWFLAKE_* — never commit .env_snowflake
```

Demo Postgres for the export script continues to use
`etl_pipeline/synthetic_data_generator/.env_demo` (do not invent a second demo Postgres file).

Also ensure local `dbt_dental_models/profiles.yml` includes the `snowflake` output from
[`profiles.yml.template`](../../dbt_dental_models/profiles.yml.template)
(env vars only — no hardcoded secrets).

## 5. Python / dbt packages

```powershell
pip install -r scripts/snowflake/requirements.txt
```

Use the same dbt Core major you already run (1.7.x). `dbt-snowflake` must match.

## 5. Verify connectivity

```powershell
mdc dbt validate --env snowflake
mdc dbt invoke --env snowflake -- debug
```

## 6. Next: load Wave 1 tables

```powershell
# From repo root (loads .env_snowflake + generator .env_demo)
python scripts/snowflake/export_demo_to_snowflake.py
```

Then:

```powershell
mdc dbt invoke --env snowflake -- build --select tag:snowflake
```

## Cost habits (keep forever)

- Warehouse size: **X-Small only**
- Auto-suspend: **60 seconds**
- No Snowpipe / no always-on tasks in v1
- Suspend or drop `WH_DEMO_XS` if you pause the portfolio demo for weeks
- Resource monitor alert before credits burn
