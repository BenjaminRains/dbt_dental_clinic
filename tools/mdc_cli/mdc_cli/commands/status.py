"""mdc status — show config paths, validation, and venv discovery."""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

from mdc_cli.env import validate_component_stage
from mdc_cli.paths import (
    REPO_ROOT,
    ALL_MDC_STAGES,
    ComponentStage,
    discover_component_python,
    iter_status_targets,
)

console = Console()


def _relative_config_path(path: Path) -> str:
    try:
        return str(path.relative_to(REPO_ROOT))
    except ValueError:
        return str(path)


def _validation_cell(component: str, target: ComponentStage) -> tuple[str, str]:
    ok, err = validate_component_stage(
        component,
        target.stage,
        profile=target.profile,
    )
    if ok:
        return "ok", ""
    message = (err or "validation failed").splitlines()[0]
    if len(message) > 72:
        message = message[:69] + "..."
    return "fail", message


def render_status_table(env_filter: Optional[str] = None) -> Table:
    table = Table(title="mdc status", show_lines=False)
    table.add_column("Component", style="cyan", no_wrap=True)
    table.add_column("Stage", no_wrap=True)
    table.add_column("Config", overflow="fold")
    table.add_column("File", no_wrap=True)
    table.add_column("Profile", no_wrap=True)
    table.add_column("Valid", no_wrap=True)
    table.add_column("Venv python", overflow="fold")
    table.add_column("Notes", overflow="fold")

    for target in iter_status_targets(env_filter):
        config_display = _relative_config_path(target.config_path)
        file_status = "yes" if target.config_path.exists() else "no"
        valid_status, note = _validation_cell(target.component, target)
        venv_python = discover_component_python(target.component)
        venv_display = _relative_config_path(venv_python) if venv_python else "—"

        table.add_row(
            target.component,
            target.stage,
            config_display,
            file_status,
            target.profile or "—",
            valid_status,
            venv_display,
            note,
        )

    return table


def status(
    env: Optional[str] = typer.Option(
        None,
        "--env",
        help="Filter to one stage (e.g. local, clinic, test, demo)",
    ),
) -> None:
    """Show config files, validation, and venv paths (stateless — no shell activation)."""
    if env:
        if env not in ALL_MDC_STAGES:
            raise typer.BadParameter(
                f"Unsupported stage '{env}'. Expected one of: {list(ALL_MDC_STAGES)}"
            )

    console.print(render_status_table(env_filter=env))
    console.print(
        "\n[dim]Configuration authority: api/settings.py, "
        "etl_pipeline/.../settings_v2.py[/dim]"
    )
