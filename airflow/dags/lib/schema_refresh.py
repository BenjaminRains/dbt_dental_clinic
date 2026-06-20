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

from lib.mdc_runner import run_mdc

logger = logging.getLogger(__name__)

SCHEMA_ANALYSIS_TIMEOUT_SECONDS = 1800


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

    shutil.copy2(tables_path, backup_path)
    logger.info("Backup created: %s", backup_path)
    if task_instance:
        task_instance.xcom_push(key="backup_path", value=str(backup_path))
        task_instance.xcom_push(key="backup_created", value=True)
    return True


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
    """Backup (if present) and regenerate tables.yml from source schema."""
    backup_tables_yml(project_root, task_instance=task_instance)
    run_opendental_schema_analysis(project_root, environment)
    return True
