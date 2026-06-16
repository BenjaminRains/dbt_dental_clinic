# Scripts

Project scripts organized by purpose. Run from project root (e.g. `.\scripts\ec2\run_dbt_on_ec2.ps1 run`).

## Entry Points

| Script | Purpose |
|--------|---------|
| [environment_manager.ps1](environment_manager.ps1) | Legacy full manager: deploy, SSM, frontend, `*-init`. Load via `load_project.ps1 -Legacy`. |
| [mdc_aliases.ps1](mdc_aliases.ps1) | **Default** thin `mdc` aliases. Load via `load_project.ps1`. |
| [mdc_invoke.ps1](mdc_invoke.ps1) | Shared `Invoke-MDC` helper used by aliases and environment manager. |
| [project_profile.ps1](project_profile.ps1) | PowerShell profile loader; sources `environment_manager.ps1` when in project. |
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

### Deploy to clinic EC2

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
