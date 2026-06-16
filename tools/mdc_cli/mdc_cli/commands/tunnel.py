"""Infrastructure wrappers to existing PowerShell SSM helpers (Phase 4.5)."""

from __future__ import annotations

import typer

from mdc_cli.ps_invoke import invoke_tunnel_function

tunnel_app = typer.Typer(help="SSM port-forward wrappers (PowerShell)")


@tunnel_app.command("clinic-db")
def clinic_db() -> None:
    """Port-forward clinic RDS via dental-clinic-api-clinic (ssm-port-forward-rds-clinic)."""
    typer.echo("TUNNEL  clinic-db  -> Start-SSMPortForwardRDSClinic")
    code = invoke_tunnel_function("Start-SSMPortForwardRDSClinic")
    raise typer.Exit(code=code)


@tunnel_app.command("demo-db")
def demo_db() -> None:
    """Port-forward demo database to localhost (ssm-port-forward-demo-db)."""
    typer.echo("TUNNEL  demo-db  -> Start-SSMPortForwardDemoDB")
    code = invoke_tunnel_function("Start-SSMPortForwardDemoDB")
    raise typer.Exit(code=code)


@tunnel_app.command("rds")
def rds_demo() -> None:
    """Port-forward RDS via demo API instance (ssm-port-forward-rds)."""
    typer.echo("TUNNEL  rds  -> Start-SSMPortForwardRDS")
    code = invoke_tunnel_function("Start-SSMPortForwardRDS")
    raise typer.Exit(code=code)


@tunnel_app.command("close")
def close() -> None:
    """SSM sessions are stopped with Ctrl+C in the forwarding terminal."""
    typer.echo(
        "Active SSM port-forward sessions run in their own terminal. "
        "Press Ctrl+C in that window to close the tunnel.",
    )
    raise typer.Exit(code=0)
