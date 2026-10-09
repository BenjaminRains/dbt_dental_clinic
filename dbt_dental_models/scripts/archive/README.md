# Archived dbt scripts

One-time YAML migrations. Finished 2026-07-10. See `scripts/archive/README.md` for the two-quarter deletion rule.

| File | What it did |
|------|-------------|
| `fix_duplicate_yaml_keys.py` | Merged duplicate model-level `tests`, `meta`, and `config` keys |
| `migrate_staging_config_to_yaml.py` | Moved `stg_opendental__*` `{{ config() }}` blocks into YAML |
| `move_staging_yaml_config_to_top.py` | Moved those YAML `config:` blocks up under the model name |

`list_slow_models.ps1` stays in the parent `scripts/` folder.
