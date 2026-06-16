# mdc CLI (Phase 4)

Monorepo developer CLI for API, ETL, and dbt workflows. Configuration is loaded only
via existing pydantic loaders in `api/settings.py` and `etl_pipeline/.../settings_v2.py`.

## Install (editable)

From the repository root:

```powershell
pip install -e tools/mdc_cli
.\load_project.ps1
```

Then:

```powershell
mdc --help
mdc status
status
api-run
```

Use `.\load_project.ps1 -Legacy` for deploy, SSM port-forward, and frontend commands.

## Commands

### Validation

- `mdc status` — config paths, validation overview, venv discovery
- `mdc api test-config --env <stage>`
- `mdc etl validate --env <stage> [--profile load|full]`
- `mdc dbt validate --env <stage>`

### Runtime (stateless, isolated child env)

- `mdc api run --env <stage>` — uvicorn; `--reload` default on `local` only
- `mdc etl run|status|test-connections --env <stage> [--profile full] -- [args]`
- `mdc dbt run|test|docs --env <stage> -- [args]`
- `mdc dbt invoke --env <stage> -- deps` — arbitrary dbt subcommands

### Infrastructure wrappers (Phase 4.5)

- `mdc tunnel clinic-db|demo-db|rds` — SSM port forward via `scripts/ssm_tunnels.ps1`
- `mdc deploy frontend` — `Deploy-Frontend`
- `mdc deploy api --env clinic` — `scripts/deployment/deploy_api_file.ps1` when present

### PowerShell aliases (Phase 4.5)

`scripts/mdc_aliases.ps1` (default via `load_project.ps1`): `status`, `api-run`, `api-test`,
`etl-run`, `etl-validate`, `etl-test`, `etl-status`, `env-status`.

Stages: `local`, `clinic`, `test`, `demo` (API/dbt). Use `clinic` for live clinic context.
