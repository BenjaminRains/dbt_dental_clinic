"""
Canonical filesystem paths for ETL pipeline artifacts.

All application logs and schema-analysis outputs live under ``etl_pipeline/logs/``.
Airflow task logs remain in Airflow's own log directory (``airflow/logs/``).

Layout::

    etl_pipeline/logs/
        etl_pipeline/           # per-run ETL logs
        tests/                  # test run logs
        schema_analysis/
            backups/
            reports/
            logs/               # schema analyzer session logs
"""
from __future__ import annotations

import logging
from pathlib import Path

logger = logging.getLogger(__name__)

_LEGACY_REPO_LOGS_DIRNAME = "logs"


def etl_project_root() -> Path:
    """Top-level etl_pipeline project directory (contains scripts/, etl_pipeline/)."""
    return Path(__file__).resolve().parent.parent.parent


def monorepo_root() -> Path:
    """Repository root (parent of etl_pipeline/)."""
    return etl_project_root().parent


def logs_root(*, repo_root: Path | None = None) -> Path:
    """Canonical application log root under etl_pipeline/logs/."""
    if repo_root is not None:
        return repo_root / "etl_pipeline" / "logs"
    return etl_project_root() / "logs"


def schema_analysis_dir(*, repo_root: Path | None = None) -> Path:
    return logs_root(repo_root=repo_root) / "schema_analysis"


def schema_analysis_backups_dir(*, repo_root: Path | None = None) -> Path:
    return schema_analysis_dir(repo_root=repo_root) / "backups"


def schema_analysis_reports_dir(*, repo_root: Path | None = None) -> Path:
    return schema_analysis_dir(repo_root=repo_root) / "reports"


def schema_analysis_session_logs_dir(*, repo_root: Path | None = None) -> Path:
    return schema_analysis_dir(repo_root=repo_root) / "logs"


def etl_run_logs_dir(*, repo_root: Path | None = None) -> Path:
    return logs_root(repo_root=repo_root) / "etl_pipeline"


def etl_test_logs_dir(*, repo_root: Path | None = None) -> Path:
    return logs_root(repo_root=repo_root) / "tests"


def schema_analysis_backup_display_path(timestamp: str) -> str:
    """Human-readable backup path for changelogs and docs."""
    return f"etl_pipeline/logs/schema_analysis/backups/tables.yml.backup.{timestamp}"


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def warn_legacy_log_dirs(*, repo_root: Path | None = None) -> None:
    """Warn if pre-consolidation repo-root logs/ still exists."""
    root = repo_root if repo_root is not None else monorepo_root()
    legacy = root / _LEGACY_REPO_LOGS_DIRNAME / "schema_analysis"
    if not legacy.exists():
        return
    logger.warning(
        "Legacy log directory %s still exists; new artifacts are written to %s. "
        "Archive and remove the legacy directory after review.",
        legacy,
        schema_analysis_dir(repo_root=root),
    )
