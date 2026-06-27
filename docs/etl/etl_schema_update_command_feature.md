# ETL Schema Update Command Feature

> **Status (2026-06-26):** **Shipped (v1).** CLI command `update-schema` and nightly Airflow `refresh_schema_configuration` are live. Change reporting is simplified; `--dry-run`, rollback CLI, and dedicated tests remain deferred. See [§ Status](#status-2026-06).

## Overview

This document specifies the ETL schema-update feature: automatically refresh `tables.yml` by running `analyze_opendental_schema.py`, keeping ETL configuration aligned with the live OpenDental schema and reducing schema-drift failures.

**Shipped entry points:**

- **Manual (laptop):** `mdc etl schema --env local` or `mdc etl invoke --env local -- update-schema` — loads `.env_local`; OpenDental source is still the clinic server (VPN). Requires `--profile full` (default for `mdc etl schema`).
- **Manual (on clinic EC2):** `mdc etl schema --env clinic` — same command, different env file on that host.
- **Nightly:** Airflow `etl_pipeline` DAG → `refresh_schema_configuration` (first step after business-hours guard; runs on clinic with `--env clinic`)
- **Related:** `mdc etl invoke --env clinic -- check-schema-drift` (table-count sanity check on clinic; does not regenerate config)
## Problem Statement

The ETL pipeline can experience schema drift when:

- Database schemas evolve (columns added/removed/modified)
- `tables.yml` references columns or strategies that no longer match MySQL
- Manual schema analysis was required before automated refresh existed

**Mitigation (shipped):** Regenerate `tables.yml` on demand or every nightly ETL run.
## Proposed Solution

Add an ETL **`update-schema`** command (original name in this doc: `etl-update-schema`) that:

1. Runs `analyze_opendental_schema.py`
2. Generates updated `tables.yml`
3. Reports detected configuration changes (simplified diff)
4. Integrates with CLI, `mdc`, and Airflow orchestration
## Feature Requirements

### Functional Requirements

| # | Requirement | Status |
| --- | --- | --- |
| 1 | Command interface (`update-schema`) | ✅ Shipped |
| 2 | Execute `analyze_opendental_schema.py` | ✅ Shipped (subprocess) |
| 3 | Generate `tables.yml` | ✅ Shipped |
| 4 | Change detection / reporting | ⚠️ Simplified (`_detect_schema_changes`) |
| 5 | Backup before overwrite | ✅ Shipped → `etl_pipeline/logs/schema_analysis/backups/` |
| 6 | Validate new config (YAML syntax) | ✅ Shipped |
### Non-Functional Requirements
1. **Performance**: Complete schema analysis within reasonable time (< 30 minutes)
2. **Reliability**: Handle database connection failures gracefully
3. **Usability**: Provide clear progress indicators and error messages
4. **Integration**: Work with existing ETL environment management
5. **Safety**: Never overwrite configuration without backup

## Implementation Plan

> **Implementation status:** Phases 1–3 and most of Phase 4 are **done**. See [§ Status](#status-2026-06) for gaps.

### Phase 1: Command Structure ✅

1. **CLI command:** `update_schema` in `etl_pipeline/etl_pipeline/cli/commands.py`; registered in `cli/main.py`
2. **Options:** `--backup`, `--force`, `--output-dir`, `--log-level` — **`--dry-run` not implemented**
3. **Environment:** `get_settings()`, `ConnectionFactory` pre-flight
4. **Error handling:** `click.ClickException`, connection failure path matches spec

### Phase 2: Schema Analysis Integration ✅

1. Subprocess execution of `etl_pipeline/scripts/analyze_opendental_schema.py`
2. Real-time stdout (not fully parsed into structured progress)
3. Script writes `tables.yml` and schema changelog via analyzer
4. Failures honor `--force` to continue with warning

### Phase 3: Configuration Management ✅ (partial rollback)

1. Timestamped backup via `schema_analysis_backups_dir()` before run
2. Post-run YAML syntax validation
3. Script replaces config; CLI validates result exists
4. **Rollback:** manual restore from backup only — no `restore-schema` command

### Phase 4: Change Reporting ⚠️

1. `_detect_schema_changes()` compares backup vs new config
2. Reports table count delta, extraction strategy changes, incremental-column list changes
3. **Not implemented:** per-column add/remove counts (example output below is illustrative)
4. **Not implemented:** automated ETL impact assessment
## Command Design

### Command Syntax (shipped)

```bash
# Preferred from laptop (VPN to clinic OpenDental)
mdc etl schema --env local
mdc etl schema --env local -- --force

# On clinic EC2
mdc etl schema --env clinic

# Direct CLI module
python -m etl_pipeline.cli.main update-schema
mdc etl invoke --env local -- update-schema [--backup] [--force] [--output-dir PATH] [--log-level LEVEL]

Options:
  --backup          Create backup of current configuration (default: true)
  --force           Force analysis even when existing schema hash suggests skip
  --output-dir      Output directory for tables.yml (default: etl_pipeline/config)
  --log-level       DEBUG | INFO | WARNING | ERROR (default: INFO)
```

**Config file (canonical):** `etl_pipeline/etl_pipeline/config/tables.yml`  
**Backups:** `etl_pipeline/logs/schema_analysis/backups/tables.yml.backup.{timestamp}`
### Command Flow
1. **Environment Setup**: Load ETL environment variables
2. **Pre-flight Checks**: Verify database connectivity and permissions
3. **Backup Creation**: Timestamped backup under `etl_pipeline/logs/schema_analysis/backups/`4. **Schema Analysis**: Execute `analyze_opendental_schema.py`
5. **Configuration Validation**: Verify new configuration is valid
6. **Change Detection**: Compare old vs new configuration
7. **Update Application**: Replace configuration if valid
8. **Summary Report**: Display changes and next steps

### Example Usage

```bash
# Laptop dev workflow (loads .env_local; source MySQL via VPN)
mdc etl test-connections --env local --profile full
mdc etl schema --env local

# Force re-analysis
mdc etl invoke --env local -- update-schema --force

# Custom output directory (advanced)
mdc etl invoke --env local -- update-schema --output-dir etl_pipeline/config
```
## Integration Points

### Existing ETL Infrastructure

1. **CLI:** `etl_pipeline/etl_pipeline/cli/commands.py` → `update_schema`
2. **mdc:** `tools/mdc_cli/mdc_cli/commands/etl.py` → `mdc etl schema`
3. **Airflow:** `airflow/dags/lib/schema_refresh.py` → `refresh_schema_configuration` (nightly)
4. **Schema drift check:** `check-schema-drift` in same CLI (table count vs config; optional pre-ETL)

### Schema Analysis Script

1. **Script Location:** `etl_pipeline/scripts/analyze_opendental_schema.py`
2. **SCD features:** [schema_analysis_scd_improvements.md](schema_analysis_scd_improvements.md) — backup, compare, changelog
3. **Execution Method:** Subprocess from `update-schema`; or run analyzer directly
4. **Error Handling:** Capture and report analysis failures; `--force` on CLI

## Expected Output

> **Note:** Success example below includes **illustrative** change lines. Shipped `_detect_schema_changes()` reports table-count, strategy, and incremental-column deltas — not per-column add/remove counts.

### Success Case

```
ETL Schema Update
=================

Environment: production
Database: opendental
Configuration: etl_pipeline/etl_pipeline/config/tables.yml

[INFO] Creating backup: etl_pipeline/logs/schema_analysis/backups/tables.yml.backup.20251006_143022
[INFO] Running schema analysis...
[INFO] Analyzing 432 tables...
[INFO] Schema analysis complete in 18.3s

Schema Changes Detected:
- Table count changed: 430 -> 432
- Tables with strategy changes: 3
- Tables with column changes: 2

Configuration updated successfully!
Backup saved: etl_pipeline/logs/schema_analysis/backups/tables.yml.backup.20251006_143022

Next steps:
1. Test ETL: mdc etl run --env clinic -- --tables appointment,statement,document
2. Monitor for any remaining issues
3. Run full ETL pipeline when ready
```

### Error Case
```
ETL Schema Update
=================

[ERROR] Database connection failed: Connection timeout
[ERROR] Cannot proceed with schema analysis

Troubleshooting:
1. Check database connectivity
2. Verify environment variables
3. Ensure database permissions

Configuration unchanged.
```

## Benefits

### Immediate Benefits
1. **Automated Schema Sync**: No manual intervention required
2. **Error Prevention**: Catch schema drift before ETL failures
3. **Configuration Safety**: Automatic backup and validation
4. **Change Visibility**: Clear reporting of schema changes

### Long-term Benefits
1. **Reduced Maintenance**: Automated schema management
2. **Improved Reliability**: Fewer ETL failures due to schema issues
3. **Better Monitoring**: Regular schema health checks
4. **Faster Recovery**: Quick resolution of schema drift issues

## Implementation Considerations

### Technical Considerations
1. **Script Execution**: Handle subprocess execution and output capture
2. **File Management**: Safe configuration file replacement with backups in `logs/schema_analysis/`
3. **Error Recovery**: Graceful handling of analysis failures
4. **Performance**: Monitor analysis execution time

### Operational Considerations

1. **Scheduling:** ✅ Nightly via Airflow `refresh_schema_configuration` (~7 min for ~446 tables)
2. **Monitoring:** ⚠️ Changelog markdown from analyzer; no Slack/email alerts
3. **Documentation:** `etl_pipeline/README.md`, `airflow/NIGHTLY_RUN.md`, [ETL_CDC §3 schema CDC](ETL_CDC_IMPLEMENTATION_AND_OPTIONS.md)
4. **Training:** Use `mdc etl schema --env local` for manual refresh from a laptop; use `--env clinic` only when running on the clinic EC2 host.
## Testing Strategy

**Status:** Minimal automated coverage today (`tools/mdc_cli/tests/test_phase45.py` asserts `update-schema` in mdc command). Dedicated unit/integration tests for `update_schema` and `_detect_schema_changes` remain **deferred**.

### Unit Tests (deferred)1. **Command parsing**: Test command line argument handling
2. **Script execution**: Test subprocess execution
3. **Configuration validation**: Test configuration file validation
4. **Error handling**: Test error scenarios

### Integration Tests
1. **End-to-end flow**: Test complete command execution
2. **Database connectivity**: Test with real database
3. **Configuration updates**: Test actual configuration changes
4. **Backup/restore**: Test backup and rollback functionality

### User Acceptance Tests
1. **Command usability**: Test command interface
2. **Output clarity**: Test output readability
3. **Error messages**: Test error message clarity
4. **Documentation**: Test documentation completeness

## Future Enhancements

### Phase 2 Features

| Feature | Status |
| --- | --- |
| Automated scheduling | ✅ Nightly Airflow |
| Change notifications (email/Slack) | ❌ Pending |
| Pre-ETL schema validation | ⚠️ Partial (`check-schema-drift`) |
| Rollback command | ❌ Pending (manual backup restore) |
| `--dry-run` | ❌ Not implemented |

### Advanced Features

1. **Schema versioning** — backups exist; no version registry CLI
2. **Impact analysis** — changelog from analyzer; no dbt breakage gate
3. **Migration scripts** — see [ETL_CDC §5.3](ETL_CDC_IMPLEMENTATION_AND_OPTIONS.md)
4. **Continuous monitoring** — table-count drift only

---

## Status (2026-06)

### Shipped

| Component | Location |
| --- | --- |
| CLI `update-schema` | `etl_pipeline/etl_pipeline/cli/commands.py` |
| Change diff helper | `_detect_schema_changes()` (same file) |
| Schema analyzer | `etl_pipeline/scripts/analyze_opendental_schema.py` |
| Backup paths | `etl_pipeline/etl_pipeline/config/paths.py` |
| mdc wrapper | `mdc etl schema` → `tools/mdc_cli/mdc_cli/commands/etl.py` |
| Nightly refresh | `airflow/dags/lib/schema_refresh.py`, task `refresh_schema_configuration` |
| Optional manual DAG | `schema_analysis` (not required nightly) |

### Deferred

- `--dry-run` flag
- `restore-schema` / rollback CLI
- Rich column-level diff in CLI output (analyzer changelog is the detailed source)
- Dedicated pytest suite for `update_schema`
- Change notifications

### Related docs

- [schema_analysis_scd_improvements.md](schema_analysis_scd_improvements.md) — SCD detection inside the analyzer (changelog detail)
- [ETL_CDC_IMPLEMENTATION_AND_OPTIONS.md](ETL_CDC_IMPLEMENTATION_AND_OPTIONS.md) — schema vs row-level “CDC”
- [airflow/ORCHESTRATION_ROADMAP.md](../../airflow/ORCHESTRATION_ROADMAP.md) — nightly schema refresh decision

---

## Conclusion

The **`update-schema`** feature is **in production use**: manual refresh via `mdc etl schema --env local` (laptop) or nightly on clinic EC2/Airflow. It addresses schema drift by regenerating `tables.yml` from live OpenDental with backup + YAML validation.

Remaining work is **enhancement** (richer diffs, rollback CLI, alerts, tests) — not core functionality.

---

**Document Created:** 2025 (feature spec)  
**Feature Shipped:** CLI + Airflow integration (2025–2026)  
**Document Updated:** 2026-06-26  
**Status:** v1 complete; enhancements deferred