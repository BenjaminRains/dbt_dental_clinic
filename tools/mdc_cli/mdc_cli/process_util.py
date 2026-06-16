"""Cross-platform subprocess helpers (Windows npm.cmd / aws.exe resolution)."""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path
from typing import Optional


def find_executable(name: str) -> Optional[str]:
    return shutil.which(name)


def resolve_cmd(cmd: list[str]) -> list[str]:
    """
    Resolve the first argv token to a full executable path when possible.

    On Windows, ``subprocess.run(["npm", ...])`` fails unless the path includes
    ``npm.cmd``; ``shutil.which`` returns the correct path.
    """
    if not cmd:
        return cmd
    token = cmd[0]
    if os.path.isabs(token) and Path(token).exists():
        return cmd
    found = shutil.which(token)
    if found:
        return [found, *cmd[1:]]
    return cmd


def run_subprocess(
    cmd: list[str],
    *,
    cwd: Path | str | None = None,
    env: dict[str, str] | None = None,
    capture_output: bool = False,
) -> int:
    resolved = resolve_cmd(cmd)
    kwargs: dict = {"check": False}
    if cwd is not None:
        kwargs["cwd"] = str(cwd)
    if env is not None:
        kwargs["env"] = env
    if capture_output:
        kwargs["capture_output"] = True
        kwargs["text"] = True
    completed = subprocess.run(resolved, **kwargs)
    return int(completed.returncode)


def run_subprocess_completed(
    cmd: list[str],
    *,
    cwd: Path | str | None = None,
    env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    resolved = resolve_cmd(cmd)
    kwargs: dict = {"check": False, "capture_output": True, "text": True}
    if cwd is not None:
        kwargs["cwd"] = str(cwd)
    if env is not None:
        kwargs["env"] = env
    return subprocess.run(resolved, **kwargs)
