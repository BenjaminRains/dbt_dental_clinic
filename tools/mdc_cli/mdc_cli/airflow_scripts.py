"""Native Airflow bootstrap scripts under scripts/airflow/."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

from mdc_cli.paths import REPO_ROOT
from mdc_cli.ps_invoke import invoke_ps_script_file

AIRFLOW_SCRIPTS_DIR = REPO_ROOT / "scripts" / "airflow"


def airflow_script(name: str) -> Path:
    return AIRFLOW_SCRIPTS_DIR / name


def require_windows_native() -> None:
    if os.name != "nt":
        raise RuntimeError(
            "Native Airflow scripts are Windows-only. "
            "On Linux/macOS use scripts/airflow/init-airflow.sh (Docker) or host install."
        )


def run_airflow_init() -> int:
    require_windows_native()
    script = airflow_script("init-airflow-native.ps1")
    if not script.exists():
        return 127
    return invoke_ps_script_file(script, [])


def run_airflow_start(*, scheduler: bool, webserver: bool) -> int:
    require_windows_native()
    script = airflow_script("start-airflow-native.ps1")
    if not script.exists():
        return 127
    ps_args: list[str] = []
    if scheduler:
        ps_args.append("-SchedulerOnly")
    elif webserver:
        ps_args.append("-WebserverOnly")
    return invoke_ps_script_file(script, ps_args)


def run_airflow_logs(
    *,
    dag_id: str,
    run_id: str,
    task: str,
    limit: int,
    tail: int,
) -> int:
    require_windows_native()
    script = airflow_script("airflow-logs.ps1")
    if not script.exists():
        return 127
    ps_args: list[str] = []
    if dag_id != "etl_pipeline":
        ps_args.extend(["-DagId", dag_id])
    if run_id:
        ps_args.extend(["-RunId", run_id])
    if task:
        ps_args.extend(["-Task", task])
    if limit != 8:
        ps_args.extend(["-Limit", str(limit)])
    if tail:
        ps_args.extend(["-Tail", str(tail)])
    return invoke_ps_script_file(script, ps_args)


def run_airflow_docker_init() -> int:
    """Docker Compose init (scripts/airflow/init-airflow.sh)."""
    script = airflow_script("init-airflow.sh")
    if not script.exists():
        return 127
    completed = subprocess.run(
        ["bash", str(script)],
        cwd=str(REPO_ROOT),
        check=False,
    )
    return int(completed.returncode)
