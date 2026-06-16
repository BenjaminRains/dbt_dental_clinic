# Scripts

Project scripts organized by purpose. Run from project root (e.g. `.\scripts\ec2\run_dbt_on_ec2.ps1 run`).

## Entry Points

| Script | Purpose |
|--------|---------|
| [environment_manager.ps1](environment_manager.ps1) | Legacy full manager (`-Legacy`): deploy, SSM, frontend. *-init deprecated (Phase 4.6). |
| [mdc_aliases.ps1](mdc_aliases.ps1) | **Default** thin `mdc` aliases. Load via `load_project.ps1`. |
| [mdc_invoke.ps1](mdc_invoke.ps1) | Shared `Invoke-MDC` helper used by aliases and environment manager. |
| **ssm_tunnels.ps1** | Dot-source in default aliases replaced by `mdc ssm connect`; file kept for Legacy env manager |
| [project_profile.ps1](project_profile.ps1) | PowerShell profile loader; sources `load_project.ps1` (mdc aliases) when in project. |
| [run_dbt.bat](run_dbt.bat) | Convenience wrapper to run dbt on EC2; forwards to `ec2\run_dbt_on_ec2.ps1`. |

## Directory Layout

| Path | Purpose |
|------|---------|
| **deployment/** | Deploy code/config to AWS/EC2 |
| **ec2/** | EC2 dbt runtime, setup script, fixes |
| **verification/** | Verify AWS resources (IAM, security groups, target groups, etc.) |
| **database/** | Local demo DB setup and queries |
| **testing/** | API and connection tests |
| **utils/** | One-off tools, exports, metadata, audits |

## Common Workflows

### Daily dev (mdc — default)

From project root after `.\load_project.ps1`:

```powershell
status
api-test
etl-validate
mdc api run --env local
mdc etl run --env clinic --profile full
mdc dbt run --env clinic
mdc tunnel clinic-db
ssm-connect-clinic-api    # SSM shell on clinic API EC2 (also in default aliases)
```

Use `.\load_project.ps1 -Legacy` for frontend deploy menus and other legacy helpers not yet in `mdc`.

### Deploy clinic API env to EC2

Copies local `api/.env_api_clinic` → `/opt/dbt_dental_clinic/api/.env`, restarts systemd unit `dental-clinic-api`, verifies `/health/db`:

```powershell
.\load_project.ps1
mdc deploy api --env clinic
```

See `docs/ENVIRONMENT_FILES.md` §4.8 for naming (EC2 Name tag vs systemd unit).

### Deploy other assets to clinic EC2

```powershell
.\scripts\deployment\deploy_codebase_to_clinic_ec2.ps1
.\scripts\deployment\deploy_dbt_file.ps1 -FilePath "models\marts\fact_claim.sql"
.\scripts\deployment\deploy_credentials_json.ps1
```

### Run dbt on EC2

```powershell
.\scripts\run_dbt.bat run --select fact_claim
# or directly:
.\scripts\ec2\run_dbt_on_ec2.ps1 run --select fact_claim
```

### Fix EC2 dbt setup

```powershell
.\scripts\ec2\fix_ec2_dbt_setup.ps1
```

### Query demo database

```powershell
.\scripts\database\query_demo_db.ps1 -Query "SELECT * FROM marts.dim_procedure LIMIT 5"
.\scripts\database\query_demo_db.ps1 -Query "SELECT 1" -Local
```

### Verify AWS deployment

```powershell
.\scripts\verification\verify_clinic_ec2_details.ps1
.\scripts\verification\check_ec2_setup.ps1
```

### Utilities

```powershell
.\scripts\utils\list_env_files.ps1
.\scripts\utils\generate_api_key.ps1 -Clinic
.\scripts\utils\backup_dbt_dental_clinic.ps1
```

### CI

Pull requests that touch `tools/mdc_cli` or API/ETL settings loaders run
`.github/workflows/mdc_cli.yml` (pytest + `mdc status` smoke, no PowerShell).
