"""Invoke standalone PowerShell deploy scripts (Phase 5.5 — deploy API only)."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path
from typing import Mapping

from mdc_cli.paths import REPO_ROOT


def invoke_ps_script_file(
    script_path: Path,
    script_args: list[str],
    *,
    extra_env: Mapping[str, str] | None = None,
) -> int:
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
    env = os.environ.copy()
    if extra_env:
        env.update(extra_env)
    completed = subprocess.run(cmd, cwd=str(REPO_ROOT), env=env, check=False)
    return int(completed.returncode)
