# Clinic analytics workflow (local dbt → clinic RDS)

How to refresh analytics on **clinic RDS** (`opendental_analytics` behind `api-clinic.dbtdentalclinic.com`) from a developer laptop.

Clinic RDS lives in a **private VPC**. Your laptop cannot connect to the RDS hostname directly (`connection … timeout expired`). Use one of the workflows below.

**Linux / platform overview:** See [PLATFORM_AND_LINUX_MIGRATION.md](PLATFORM_AND_LINUX_MIGRATION.md) for stack architecture and the plan to run this workflow without PowerShell.

---

## Prerequisites

One-time setup:

```powershell
pip install -e tools/mdc_cli
.\load_project.ps1
```

On Linux, `load_project.ps1` is optional — `pip install -e tools/mdc_cli` is sufficient. `mdc publish analytics` requires the Python port described in [PLATFORM_AND_LINUX_MIGRATION.md](PLATFORM_AND_LINUX_MIGRATION.md) §6 Phase 1 until that work is complete.

Also required for the tunnel path:

- AWS CLI configured for the clinic account
- [Session Manager plugin](https://docs.aws.amazon.com/systems-manager/latest/userguide/session-manager-working-with-install-plugin.html)
- `deployment_credentials.json` with clinic EC2 instance ID and RDS endpoint
- PostgreSQL client tools (`pg_dump`, `pg_restore`, `psql`) on PATH
- Local Postgres with analytics data (`etl_pipeline/.env_clinic` or `dbt_dental_models/.env_local`)
- `api/.env_api_clinic` for RDS credentials used during publish

`mdc dbt validate --env clinic` only checks that **credentials exist** — it does not test network connectivity.

---

## Workflow A — Local dbt, then publish (recommended from laptop)

Build models on **local** Postgres, then copy the **`marts`** schema to clinic RDS via an SSM tunnel.

### Step 1 — Run dbt locally

```powershell
mdc dbt run --env local
```

Use `--env local`, not `--env clinic`, unless you use an SSM tunnel. The `clinic` target loads the RDS hostname from `deployment_credentials.json`; without a tunnel that host times out from a laptop. Use:

```powershell
mdc tunnel clinic-db
mdc dbt run --env clinic --tunnel-db -- --select mart_daily_payments
```

`--tunnel-db` points dbt at `127.0.0.1:5433` (same as `mdc api run --tunnel-db`).

### Step 2 — Start the tunnel (separate terminal)

```powershell
mdc tunnel clinic-db
```

Leave this terminal open. Wait until you see:

```
Port 5433 opened ...
Waiting for connections...
```

Default local port is **5433** (`POSTGRES_PORT` env var overrides for the tunnel command).

Confirm the port is listening (optional):

```powershell
Test-NetConnection 127.0.0.1 -Port 5433
```

### Step 3 — Publish to clinic RDS

```powershell
mdc publish analytics --env clinic
```

`mdc publish` checks that `127.0.0.1:5433` accepts connections before running the PowerShell script. On success you should see local and RDS schema counts match, then `Publish complete.`

Dry run (preflight only):

```powershell
mdc publish analytics --env clinic --dry-run
```

Custom tunnel port:

```powershell
mdc tunnel clinic-db          # with POSTGRES_PORT=5434 if needed
mdc publish analytics --env clinic --tunnel-port 5434
```

### What publish does

1. Reads **local** connection from `etl_pipeline/.env_clinic` (falls back to `dbt_dental_models/.env_local`)
2. Reads **RDS** host/user/db from `api/.env_api_clinic` and the **live password from the RDS master user secret** (`rds!db-...` in Secrets Manager) at publish time — rotation-safe
3. `pg_dump` local `marts` schema
4. `pg_restore` into clinic RDS through `127.0.0.1:<tunnel-port>` (replaces **marts** only; `int` / `staging` on RDS are unchanged)

### RDS password rotation (Secrets Manager)

Clinic RDS has **one** authoritative credential in Secrets Manager:

| Secret | Role |
|--------|------|
| `rds!db-...` | **RDS-owned master user secret** — rotates every ~7 days; this is the password PostgreSQL accepts |

The legacy secret **`dental-clinic/database`** was a duplicate copy (never rotated with RDS). CloudTrail audit (Jun 2025) showed no production callers — **delete it** in the AWS console and do not recreate it.

**Runtime on EC2 (rotation-safe):**

| Consumer | Password source |
|----------|-----------------|
| Clinic API | Live `GetSecretValue` via `api/clinic_rds_secret.py` (TTL cache ~5 min; falls back to `api/.env`) |
| EC2 dbt (`run_dbt_on_ec2.ps1`) | `scripts/ec2/load_api_env.sh` → `overlay_live_clinic_rds_password` (AWS CLI + python3) |

Host/user/db still come from `api/.env` (deployed from `api/.env_api_clinic`). The **password** is fetched live so weekly rotation does not require a redeploy.

**Disable live fetch** (tests / offline): `API_CLINIC_RDS_LIVE_PASSWORD=0`.

**Optional env on EC2:**

| Variable | Purpose |
|----------|---------|
| `CLINIC_RDS_MASTER_SECRET_ARN` | Skip `DescribeDBInstances` and use this secret ARN directly (written by `mdc secrets pull clinic`) |
| `CLINIC_RDS_INSTANCE_ID` | Default `dental-clinic-analytics` (also written by `mdc secrets pull clinic`) |
| `API_CLINIC_RDS_PASSWORD_TTL_SECONDS` | Cache TTL (default 300) |

**IAM (clinic EC2 instance role):** `secretsmanager:GetSecretValue` on `rds!db-*` plus `rds:DescribeDBInstances` (unless `CLINIC_RDS_MASTER_SECRET_ARN` is set).

**Developer laptop:** `mdc publish analytics`, `mdc status --env clinic --tunnel-db`, and `mdc secrets pull clinic` already fetch the live password via `GetSecretValue` on `rds!db-...`.

**Sync local deploy file + EC2 after rotation** (optional now — only needed if live fetch is disabled or IAM is missing):

```powershell
mdc secrets pull clinic      # writes api/.env_api_clinic from rds!db-...
mdc deploy api --env clinic  # copies .env_api_clinic → EC2 api/.env
```

**IAM (developer user):** `GetSecretValue` on `rds!db-...` plus `rds:DescribeDBInstances` (to resolve the master secret ARN). These are typically included in `RDSAndSecretsManagerAccess` or equivalent — no `PutSecretValue` needed.

Find the master secret name for instance `dental-clinic-analytics`:

```powershell
aws rds describe-db-instances --db-instance-identifier dental-clinic-analytics `
  --query 'DBInstances[0].MasterUserSecret.SecretArn' --output text
```

**Force stale file password** (debug only): `--use-env-file` on `mdc publish` / `mdc status`, or `API_CLINIC_RDS_LIVE_PASSWORD=0` on EC2.

After rotation (~7 days), clinic API and EC2 dbt should keep working without redeploy. If `/health/db` still fails, check instance-role IAM, then fall back to `mdc secrets pull clinic` + `mdc deploy api --env clinic`.

#### Delete legacy `dental-clinic/database` (one-time)

After CloudTrail confirms no production callers (see [CLOUDTRAIL_SETUP.md](CLOUDTRAIL_SETUP.md)):

1. AWS Console → **Secrets Manager** → region **us-east-1**
2. Open **`dental-clinic/database`** → **Actions** → **Delete secret**
3. Choose immediate delete (or minimum recovery window you prefer)
4. **Do not delete** `rds!db-...` — that secret is owned by RDS and required for rotation

Then on your laptop:

```powershell
mdc secrets pull clinic
mdc status --env clinic --tunnel-db
```

Update `password_source` / `secret_name` in your local **`deployment_credentials.json`** (gitignored) to match [`deployment_credentials.json.template`](../deployment_credentials.json.template) — RDS master secret, not `dental-clinic/database`.

---

## Workflow A2 — Airflow nightly (Option A, manual tunnel)

Same local dbt + publish path, orchestrated by the `etl_pipeline` DAG on your laptop. **You** start the SSM tunnel; the DAG does not start it yet.

**Before the run (scheduled 9 PM Central or manual trigger):**

| Prerequisite | Command / check |
|--------------|-----------------|
| WireGuard (OpenDental ETL) | VPN connected to clinic LAN |
| SSM tunnel (RDS publish) | Terminal 1: `mdc tunnel clinic-db` → wait for **Port 5433 opened** |
| Airflow | Native scheduler + webserver running |
| Variables | `etl_environment=clinic`, `dbt_target=local`, `publish_environment=clinic`, `project_root=<repo>` |

**DAG order:** guard → schema refresh → ETL → dbt (`mdc`, local target) → `mdc publish analytics` → notify.

If publish fails with tunnel not reachable, the SSM session was not open or dropped — restart `mdc tunnel clinic-db` and re-trigger from the publish task or rerun the DAG.

See [`airflow/ORCHESTRATION_ROADMAP.md`](../airflow/ORCHESTRATION_ROADMAP.md) for full Option A setup.

---

## Workflow B — Run dbt on EC2 (direct to RDS)

When you want dbt to run **inside the VPC** (no tunnel, no publish step):

```powershell
.\scripts\ec2\run_dbt_on_ec2.ps1 -Clinic run

# Specific models
.\scripts\ec2\run_dbt_on_ec2.ps1 -Clinic run --select mart_referral_source_kpis

# Full project refresh after ETL
.\scripts\ec2\run_dbt_on_ec2.ps1 -Clinic -RefreshProject
```

EC2 has direct network access to RDS. Use this after raw/ETL loads on clinic or for large full refreshes.

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|--------|----------------|-----|
| `password authentication failed` for `analytics_user` | Stale password in `api/.env` **and** live Secrets Manager fetch failed/disabled | Confirm EC2 instance role can `GetSecretValue` on `rds!db-*`; or `mdc secrets pull clinic` then `mdc deploy api --env clinic` |
| `AccessDeniedException` on `GetSecretValue` (rds!db-...) | IAM user cannot read RDS master secret | Grant `secretsmanager:GetSecretValue` on `rds!db-*` and `rds:DescribeDBInstances` |
| `Tunnel not reachable on 127.0.0.1:5433` | Tunnel not running | Start `mdc tunnel clinic-db` in another terminal first |
| `ModuleNotFoundError: No module named 'mdc_cli'` | `mdc` not installed in that shell | `pip install -e tools/mdc_cli` from repo root |
| dbt `timeout expired` to `*.rds.amazonaws.com` | Hitting RDS without tunnel/VPC access | Use `mdc dbt run --env local` + publish, or EC2 workflow |
| Local marts missing or empty | dbt not run locally | `mdc dbt run --env local` before publish |
| Tunnel starts then publish still fails | Wrong port | Match `--tunnel-port` to tunnel local port (default 5433) |

### API against clinic DB from laptop

For the FastAPI app (not dbt):

```powershell
# Terminal 1
mdc tunnel clinic-db

# Terminal 2
mdc api run --env clinic --tunnel-db
```

---

## Related docs

- [ENVIRONMENT_FILES.md](ENVIRONMENT_FILES.md) — env file locations and `mdc` stages
- [scripts/README.md](../../scripts/README.md) — EC2 dbt and deployment scripts
- [tools/mdc_cli/README.md](../../tools/mdc_cli/README.md) — full `mdc` command reference
