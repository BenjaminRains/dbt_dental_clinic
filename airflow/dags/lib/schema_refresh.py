"""
Refresh etl_pipeline/config/tables.yml from live OpenDental schema.

Used by etl_pipeline (before every nightly ETL) and schema_analysis DAG.
"""

from __future__ import annotations

import logging
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import yaml
from airflow.exceptions import AirflowException

from lib.mdc_runner import run_mdc

logger = logging.getLogger(__name__)

SCHEMA_ANALYSIS_TIMEOUT_SECONDS = 1800
# Refuse to replace a full clinic-sized config with a tiny test/partial dump.
MIN_TABLES_YML_FLOOR = 100


def _warn_legacy_log_dirs(project_root: Path) -> None:
    legacy = project_root / "logs" / "schema_analysis"
    if not legacy.exists():
        return
    logger.warning(
        "Legacy log directory %s still exists; backups now go to %s. "
        "Archive and remove the legacy directory after review.",
        legacy,
        backup_dir(project_root).parent,
    )


def tables_yml_path(project_root: Path) -> Path:
    return project_root / "etl_pipeline" / "etl_pipeline" / "config" / "tables.yml"


def etl_logs_root(project_root: Path) -> Path:
    return project_root / "etl_pipeline" / "logs"


def backup_dir(project_root: Path) -> Path:
    return etl_logs_root(project_root) / "schema_analysis" / "backups"


def etl_pipeline_dir(project_root: Path) -> Path:
    return project_root / "etl_pipeline"


def _copy_tables_yml(src: Path, dst: Path) -> None:
    """Copy YAML bytes without copystat (Windows Docker bind mounts often deny metadata)."""
    shutil.copyfile(src, dst)


def backup_tables_yml(
    project_root: Path,
    *,
    task_instance: Optional[Any] = None,
) -> bool:
    """Timestamped backup of tables.yml; returns False if no file existed."""
    tables_path = tables_yml_path(project_root)
    dest_dir = backup_dir(project_root)

    _warn_legacy_log_dirs(project_root)

    logger.info("Backing up existing tables.yml")
    if not tables_path.exists():
        logger.warning("No existing tables.yml to backup")
        if task_instance:
            task_instance.xcom_push(key="backup_created", value=False)
        return False

    dest_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = dest_dir / f"tables.yml.backup.{timestamp}"

    _copy_tables_yml(tables_path, backup_path)
    logger.info("Backup created: %s", backup_path)
    if task_instance:
        task_instance.xcom_push(key="backup_path", value=str(backup_path))
        task_instance.xcom_push(key="backup_created", value=True)
    return True


def count_tables_in_yml(tables_path: Path) -> int:
    """Return number of table entries in tables.yml, or 0 if missing/unreadable."""
    if not tables_path.exists():
        return 0
    try:
        with tables_path.open(encoding="utf-8") as fh:
            data = yaml.safe_load(fh) or {}
    except (OSError, yaml.YAMLError) as exc:
        logger.warning("Could not parse %s: %s", tables_path, exc)
        return 0
    if not isinstance(data, dict):
        return 0
    tables = data.get("tables", data)
    return len(tables) if isinstance(tables, dict) else 0


def _latest_backup_path(project_root: Path) -> Optional[Path]:
    dest_dir = backup_dir(project_root)
    if not dest_dir.exists():
        return None
    backups = sorted(dest_dir.glob("tables.yml.backup.*"), key=lambda p: p.stat().st_mtime)
    return backups[-1] if backups else None


def restore_tables_yml_from_backup(project_root: Path) -> Path:
    """Restore tables.yml from the newest backup; raise if none exists."""
    backup_path = _latest_backup_path(project_root)
    if backup_path is None:
        raise AirflowException(
            f"tables.yml fell below {MIN_TABLES_YML_FLOOR} tables and no backup was found "
            f"under {backup_dir(project_root)}"
        )
    tables_path = tables_yml_path(project_root)
    _copy_tables_yml(backup_path, tables_path)
    logger.warning("Restored tables.yml from %s (%s tables)", backup_path, count_tables_in_yml(tables_path))
    return backup_path


def run_opendental_schema_analysis(project_root: Path, environment: str) -> None:
    """Run OpenDental schema analysis via mdc (ETL Pipenv + stage env)."""
    run_mdc(
        ["etl", "schema", "--env", environment],
        cwd=project_root,
        timeout_seconds=SCHEMA_ANALYSIS_TIMEOUT_SECONDS,
    )
    logger.info("Schema analysis completed successfully")


def refresh_schema_configuration(
    project_root: Path,
    environment: str,
    *,
    task_instance: Optional[Any] = None,
) -> bool:
    """Backup (if present) and regenerate tables.yml from source schema.

    If the new file has fewer than MIN_TABLES_YML_FLOOR tables and a prior
    full-sized config existed, restore the backup and continue with it so a
    tiny test OpenDental cannot wipe the shared clinic tables.yml.
    """
    tables_path = tables_yml_path(project_root)
    prior_count = count_tables_in_yml(tables_path)

    backup_tables_yml(project_root, task_instance=task_instance)
    run_opendental_schema_analysis(project_root, environment)

    new_count = count_tables_in_yml(tables_path)
    if new_count < MIN_TABLES_YML_FLOOR and prior_count >= MIN_TABLES_YML_FLOOR:
        restore_tables_yml_from_backup(project_root)
        msg = (
            f"Refused to overwrite tables.yml: schema refresh produced {new_count} tables "
            f"(floor={MIN_TABLES_YML_FLOOR}); kept prior config with {prior_count} tables. "
            f"environment={environment}"
        )
        logger.error(msg)
        if task_instance:
            task_instance.xcom_push(key="tables_yml_protected", value=True)
            task_instance.xcom_push(key="tables_yml_rejected_count", value=new_count)
        # Continue the DAG with the restored clinic-sized config (Phase A test OD is tiny).
        return True

    if new_count < MIN_TABLES_YML_FLOOR:
        raise AirflowException(
            f"Schema refresh produced only {new_count} tables "
            f"(floor={MIN_TABLES_YML_FLOOR}) and no prior full tables.yml to keep"
        )

    if task_instance:
        task_instance.xcom_push(key="tables_yml_table_count", value=new_count)
    return True
