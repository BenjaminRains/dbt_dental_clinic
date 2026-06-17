# Scripts

Project scripts organized by purpose. Run from project root (e.g. `.\scripts\ec2\run_dbt_on_ec2.ps1 run`).

## Entry Points

| Script | Purpose |
|--------|---------|
| [mdc_aliases.ps1](mdc_aliases.ps1) | **Default** thin `mdc` aliases. Load via `load_project.ps1`. |
| [mdc_invoke.ps1](mdc_invoke.ps1) | Shared `Invoke-MDC` helper used by aliases. |
| [environment_manager.ps1](environment_manager.ps1) | **Deprecated stub** — points to `mdc`. Full copy in [archive/](archive/). |
| [ssm_tunnels.ps1](ssm_tunnels.ps1) | Legacy port-forward helpers; use `mdc tunnel` / `mdc ssm connect` instead. |
| [project_profile.ps1](project_profile.ps1) | PowerShell profile loader; sources `load_project.ps1` when in project. |
| [run_dbt.bat](run_dbt.bat) | Convenience wrapper to run dbt on EC2; forwards to `ec2\run_dbt_on_ec2.ps1`. |

## Directory Layout

| Path | Purpose |
|------|---------|
| **archive/** | Archived Phase 5.5 legacy orchestration (reference only) |
| **deployment/** | Deploy code/config to AWS/EC2 |
| **ec2/** | EC2 dbt runtime, setup script, fixes |
| **verification/** | Verify AWS resources (IAM, security groups, target groups, etc.) |
| **database/** | Local demo DB setup and queries |
| **testing/** | API and connection tests |
| **utils/** | One-off tools, exports, metadata, audits |

## Common Workflows

### Daily dev (mdc — default)

One-time: `pip install -e tools/mdc_cli`

From project root after `.\load_project.ps1`:

```powershell
status
api-test
etl-validate          # default: local + load
mdc etl status --env clinic   # etl-status alias defaults to clinic + full
mdc api run --env local
mdc api run --env clinic --tunnel-db   # with mdc tunnel clinic-db
mdc etl run --env clinic --profile full
mdc dbt run --env clinic
mdc tunnel clinic-db
ssm-connect-clinic-api
```

### Consult audio (Phase 5.4)

Stateless runs in `consult_audio_pipe/venv` with child env from `consult_audio_pipe/.env`:

```bash
mdc consult-audio install
mdc consult-audio validate
mdc consult-audio pipeline run --llm claude
mdc consult-audio pipeline status
```

Aliases: `consult-audio-validate`, `consult-audio-run`.

### Deploy dbt docs to portfolio site

```powershell
dbt-docs-deploy
# or: mdc deploy dbt-docs
```

### Deploy clinic API env to EC2

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
.\scripts\verification\verify_clinic_api_smoke.ps1
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
