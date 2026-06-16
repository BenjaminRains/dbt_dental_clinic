"""SSM status and interactive shell connect (Phase 5.1)."""

from __future__ import annotations

import typer

from mdc_cli.ssm import (
    connect_api,
    connect_clinic_api,
    connect_demo_db,
    print_ssm_status,
)

ssm_app = typer.Typer(help="AWS SSM status and shell sessions")


@ssm_app.command("status")
def ssm_status() -> None:
    """Show AWS CLI, Session Manager plugin, and cached instance IDs."""
    print_ssm_status()


@ssm_app.command("connect")
def ssm_connect(
    target: str = typer.Argument(
        ...,
        help="Target: api | clinic-api | demo-db",
    ),
) -> None:
    """Open an interactive SSM shell on an EC2 instance."""
    handlers = {
        "api": connect_api,
        "clinic-api": connect_clinic_api,
        "demo-db": connect_demo_db,
    }
    if target not in handlers:
        typer.echo("Target must be api, clinic-api, or demo-db.", err=True)
        raise typer.Exit(code=2)
    raise typer.Exit(code=handlers[target]())
