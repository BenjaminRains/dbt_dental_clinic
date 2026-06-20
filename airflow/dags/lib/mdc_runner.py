"""
Run mdc CLI subprocesses from Airflow tasks (ETL Pipenv isolation).

Airflow's .venv-airflow has orchestration deps only; ETL code runs via mdc
the same way as manual `mdc etl run --env clinic`.
"""

from __future__ import annotations

import json
import logging
import shutil
import subprocess
from pathlib import Path
from typing import Any, Optional

from airflow.exceptions import AirflowException

logger = logging.getLogger(__name__)

AIRFLOW_RESULT_PREFIX = "###AIRFLOW_RESULT###"


def resolve_mdc_cmd() -> list[str]:
    """Find mdc CLI; Airflow task subprocesses often lack user Python Scripts on PATH."""
    import sys

    mdc = shutil.which("mdc")
    if mdc:
        return [mdc]

    local_app = Path.home() / "AppData" / "Local" / "Programs" / "Python"
    for scripts in sorted(local_app.glob("Python*/Scripts/mdc.exe")):
        return [str(scripts)]

    return [sys.executable, "-m", "mdc_cli"]


def parse_airflow_result(stdout: str) -> dict[str, Any]:
    """Parse structured result line emitted by ETL CLI commands for Airflow XCom."""
    for line in stdout.splitlines():
        if line.startswith(AIRFLOW_RESULT_PREFIX):
            payload = line[len(AIRFLOW_RESULT_PREFIX) :]
            return json.loads(payload)
    return {}


def run_mdc(
    mdc_args: list[str],
    *,
    cwd: Path,
    timeout_seconds: int | None = None,
) -> subprocess.CompletedProcess[str]:
    """Run mdc with args from project root; log output and raise on non-zero exit."""
    cmd = [*resolve_mdc_cmd(), *mdc_args]
    logger.info("Running: %s (cwd=%s)", " ".join(cmd), cwd)

    try:
        result = subprocess.run(
            cmd,
            cwd=str(cwd),
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired as exc:
        raise AirflowException(
            f"mdc command timed out after {timeout_seconds} seconds: {' '.join(cmd)}"
        ) from exc

    if result.stdout:
        logger.info("mdc stdout:\n%s", result.stdout)
    if result.stderr:
        logger.warning("mdc stderr:\n%s", result.stderr)
    if result.returncode != 0:
        raise AirflowException(
            f"mdc command failed (exit {result.returncode}): {' '.join(cmd)}"
        )
    return result


def run_mdc_etl_invoke(
    environment: str,
    cli_args: list[str],
    *,
    project_root: Path,
    profile: str = "full",
    timeout_seconds: int | None = None,
) -> dict[str, Any]:
    """Run `mdc etl invoke --env <stage> -- <cli_args>` and parse AIRFLOW_RESULT."""
    result = run_mdc(
        ["etl", "invoke", "--env", environment, "--profile", profile, "--", *cli_args],
        cwd=project_root,
        timeout_seconds=timeout_seconds,
    )
    return parse_airflow_result(result.stdout)
