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

## Phase 4.1 scope

- `mdc status` — config paths, validation, venv discovery
- Stub subcommands for `api`, `etl`, `dbt`, `tunnel` (implemented in later phases)

Stages are dev/test targets only: `local`, `clinic`, `test`, and `demo` (API/dbt).
Use `clinic` for the live clinic deployment context — not a separate `production` stage name.
