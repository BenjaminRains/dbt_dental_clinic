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


def parse_dbt_run_summary(stdout: str) -> dict[str, Any]:
    """
    Parse dbt CLI summary lines (PASS=/WARN=/ERROR= or Completed with N errors).

    Returns counts plus exit-friendly flags; missing fields default to 0/False.
    """
    import re

    summary: dict[str, Any] = {
        "pass": 0,
        "warn": 0,
        "error": 0,
        "skip": 0,
        "total": 0,
        "has_errors": False,
        "raw_done_line": "",
    }
    # e.g. Done. PASS=2773 WARN=253 ERROR=4 SKIP=1980 NO-OP=1 TOTAL=5011
    done = re.search(
        r"Done\.\s+PASS=(?P<pass>\d+)\s+WARN=(?P<warn>\d+)\s+ERROR=(?P<error>\d+)"
        r"(?:\s+SKIP=(?P<skip>\d+))?(?:.*?TOTAL=(?P<total>\d+))?",
        stdout,
    )
    if done:
        summary["pass"] = int(done.group("pass"))
        summary["warn"] = int(done.group("warn"))
        summary["error"] = int(done.group("error"))
        if done.group("skip") is not None:
            summary["skip"] = int(done.group("skip"))
        if done.group("total") is not None:
            summary["total"] = int(done.group("total"))
        summary["raw_done_line"] = done.group(0).strip()
    else:
        completed = re.search(
            r"Completed with (?P<error>\d+) errors?.*?and (?P<warn>\d+) warnings?",
            stdout,
            re.IGNORECASE | re.DOTALL,
        )
        if completed:
            summary["error"] = int(completed.group("error"))
            summary["warn"] = int(completed.group("warn"))
            summary["raw_done_line"] = completed.group(0).strip()[:200]

    summary["has_errors"] = summary["error"] > 0
    return summary


def run_mdc(
    mdc_args: list[str],
    *,
    cwd: Path,
    timeout_seconds: int | None = None,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    """Run mdc with args from project root; log output and optionally raise on non-zero exit."""
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
    if check and result.returncode != 0:
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
