"""Invoke PowerShell functions from environment_manager (Phase 4.5 wrappers)."""

from __future__ import annotations

import subprocess
from pathlib import Path

from mdc_cli.paths import REPO_ROOT

ENV_MANAGER = REPO_ROOT / "scripts" / "environment_manager.ps1"
RUN_PS_FUNCTION = REPO_ROOT / "scripts" / "mdc_run_ps_function.ps1"
RUN_SSM_TUNNEL = REPO_ROOT / "scripts" / "mdc_run_ssm_tunnel.ps1"


def invoke_ps_function(function_name: str, *, check: bool = False) -> int:
    """
    Run a function from environment_manager.ps1 via mdc_run_ps_function.ps1.

    Avoids fragile -Command quoting and skips duplicate startup banners when possible.
    """
    if not RUN_PS_FUNCTION.exists():
        raise FileNotFoundError(f"Missing {RUN_PS_FUNCTION}")
    if not ENV_MANAGER.exists():
        raise FileNotFoundError(f"Missing {ENV_MANAGER}")

    completed = subprocess.run(
        [
            "powershell",
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(RUN_PS_FUNCTION),
            function_name,
        ],
        cwd=str(REPO_ROOT),
        check=False,
    )
    if check and completed.returncode != 0:
        raise subprocess.CalledProcessError(completed.returncode, function_name)
    return int(completed.returncode)


def invoke_tunnel_function(function_name: str, *, check: bool = False) -> int:
    """Run an SSM port-forward function from scripts/ssm_tunnels.ps1."""
    if not RUN_SSM_TUNNEL.exists():
        raise FileNotFoundError(f"Missing {RUN_SSM_TUNNEL}")

    completed = subprocess.run(
        [
            "powershell",
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(RUN_SSM_TUNNEL),
            function_name,
        ],
        cwd=str(REPO_ROOT),
        check=False,
    )
    if check and completed.returncode != 0:
        raise subprocess.CalledProcessError(completed.returncode, function_name)
    return int(completed.returncode)


def invoke_ps_script_file(script_path: Path, script_args: list[str]) -> int:
    """Run a .ps1 file with arguments from repo root."""
    if not script_path.exists():
        return 127
    cmd = [
        "powershell",
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        str(script_path),
        *script_args,
    ]
    completed = subprocess.run(cmd, cwd=str(REPO_ROOT), check=False)
    return int(completed.returncode)
