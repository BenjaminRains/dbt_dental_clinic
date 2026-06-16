"""Shared CLI output for validation commands."""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer
from rich.console import Console

from mdc_cli.paths import REPO_ROOT

console = Console()


def relative_repo_path(path: Path) -> str:
    try:
        return str(path.relative_to(REPO_ROOT))
    except ValueError:
        return str(path)


def finish_validation(
    *,
    component: str,
    stage: str,
    config_path: Path,
    ok: bool,
    error: Optional[str],
    profile: Optional[str] = None,
    success_label: str = "ok",
    failure_label: str = "fail",
) -> None:
    """Print one-line validation result and exit 0 or 1."""
    config_display = relative_repo_path(config_path)
    profile_part = f"  profile={profile}" if profile else ""
    status = success_label if ok else failure_label

    console.print(
        f"{component.upper():3}  {stage:6}  {config_display}{profile_part}  {status}"
    )
    if not ok and error:
        console.print(f"[red]{error}[/red]")

    raise typer.Exit(code=0 if ok else 1)
