"""SSM port-forward tunnels (Phase 5.1 — Python implementation)."""

from __future__ import annotations

import typer

from mdc_cli.ssm import tunnel_clinic_db, tunnel_demo_db, tunnel_rds_demo

tunnel_app = typer.Typer(help="SSM port-forward tunnels")


@tunnel_app.command("clinic-db")
def clinic_db() -> None:
    """Port-forward clinic RDS via dental-clinic-api-clinic."""
    raise typer.Exit(code=tunnel_clinic_db())


@tunnel_app.command("demo-db")
def demo_db() -> None:
    """Port-forward demo database to localhost."""
    raise typer.Exit(code=tunnel_demo_db())


@tunnel_app.command("rds")
def rds_demo() -> None:
    """Port-forward RDS via dental-clinic-api-demo."""
    raise typer.Exit(code=tunnel_rds_demo())


@tunnel_app.command("close")
def close() -> None:
    """SSM sessions are stopped with Ctrl+C in the forwarding terminal."""
    typer.echo(
        "Active SSM port-forward sessions run in their own terminal. "
        "Press Ctrl+C in that window to close the tunnel.",
    )
    raise typer.Exit(code=0)
