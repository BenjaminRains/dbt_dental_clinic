# Archived scripts

Retired helpers kept for reference. They are not on the daily path.

**Rule:** move a script here when the incident or migration it served is finished. Delete it after two quarters if nothing still calls it.

## Phase 5.5 orchestration

| File | Replacement |
|------|-------------|
| `environment_manager.ps1` | `mdc` CLI (`pip install -e tools/mdc_cli`) + `load_project.ps1` aliases |
| `etl_functions.ps1` | ETL commands in `mdc` / `etl_pipeline` |
| `mdc_run_ps_function.ps1` | Removed — `mdc` implements commands in Python |
| `mdc_run_ssm_tunnel.ps1` | `mdc tunnel` / `mdc_cli/ssm.py` |

Do not dot-source these for daily work.

## One-off utilities (2026-10)

| File | Why it was retired |
|------|--------------------|
| `reorder_deployment_credentials.py` | Wrote `deployment_credentials_ordered.json`, which nothing read. Canonical file is `deployment_credentials.json`. |
| `audit_ohio_resources.ps1` | One-time us-east-2 inventory. |
| `detach_demo_role_secrets_manager.ps1` | One-time demo IAM tightening. Still cited from `docs/deployment/CLINIC_DEPLOYMENT_PHASE2_ACTION_PLAN.md`. |
| `count_lines_of_code.ps1` | One-time size snapshot. |

ETL and dbt one-offs live next to those projects: `etl_pipeline/scripts/archive/` and `dbt_dental_models/scripts/archive/`.
