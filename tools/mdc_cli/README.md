# mdc CLI (Phase 4)

Monorepo developer CLI for API, ETL, and dbt workflows. Configuration is loaded only
via existing pydantic loaders in `api/settings.py` and `etl_pipeline/.../settings_v2.py`.

## Install (editable)

From the repository root:

```powershell
pip install -e tools/mdc_cli
```

Then:

```powershell
mdc --help
mdc status
mdc status --env local
```

Or without installing:

```powershell
python -m mdc_cli --help
```

## Commands (Phase 4.1–4.2)

- `mdc status` — config paths, validation overview, venv discovery
- `mdc api test-config --env <stage>` — validate API pydantic settings
- `mdc api health --env <stage>` — config health (settings load; HTTP checks in Phase 4.3)
- `mdc etl validate --env <stage> [--profile load|full]` — validate ETL settings
- `mdc dbt validate --env <stage>` — validate dbt connection env (local/clinic/demo)
- Stub: `mdc api run`, `mdc etl run`, `mdc dbt run`, `mdc tunnel *` (later phases)

Stages are dev/test targets only: `local`, `clinic`, `test`, and `demo` (API/dbt).
Use `clinic` for the live clinic deployment context — not a separate `production` stage name.
