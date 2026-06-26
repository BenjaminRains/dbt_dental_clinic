"""dbt commands."""

from __future__ import annotations

from typing import Optional

import typer

from mdc_cli.dbt_env import dbt_config_path, validate_dbt_stage
from mdc_cli.output import finish_validation
from mdc_cli.run_helper import echo_run_banner, run_dbt_command
from mdc_cli.stages import require_dbt_stage

dbt_app = typer.Typer(help="dbt project commands")

PASSTHROUGH = {"allow_extra_args": True, "ignore_unknown_options": True}


def _execute_dbt(
    env: str,
    dbt_args: list[str],
    *,
    detail: str,
    tunnel_db: bool = False,
    tunnel_port: Optional[int] = None,
) -> None:
    require_dbt_stage(env)
    ok, error = validate_dbt_stage(env)
    if not ok:
        finish_validation(
            component="dbt",
            stage=env,
            config_path=dbt_config_path(env),
            ok=False,
            error=error,
        )
    tunnel_note = " tunnel-db->127.0.0.1" if tunnel_db else ""
    echo_run_banner("dbt", env, dbt_config_path(env), f"{detail}{tunnel_note}")
    code = run_dbt_command(
        env,
        dbt_args,
        tunnel_db=tunnel_db,
        tunnel_port=tunnel_port,
    )
    raise typer.Exit(code=code)


@dbt_app.command("validate")
def validate(
    env: str = typer.Option(..., "--env", help="Stage: local, clinic, or demo"),
) -> None:
    """Validate dbt connection env for a stage (profiles.yml env_var sources)."""
    require_dbt_stage(env)
    ok, error = validate_dbt_stage(env)
    finish_validation(
        component="dbt",
        stage=env,
        config_path=dbt_config_path(env),
        ok=ok,
        error=error,
    )


@dbt_app.command("run", context_settings=PASSTHROUGH)
def run(
    ctx: typer.Context,
    env: str = typer.Option(..., "--env", help="Stage: local, clinic, or demo"),
    tunnel_db: bool = typer.Option(
        False,
        "--tunnel-db",
        help="Route Postgres to localhost (for mdc tunnel clinic-db). Overrides host/port after loading stage env.",
    ),
    tunnel_port: Optional[int] = typer.Option(
        None,
        "--tunnel-port",
        help="Local tunnel port (default: POSTGRES_PORT env or 5433)",
    ),
) -> None:
    """Run dbt with isolated injected env (extra args after --)."""
    _execute_dbt(
        env,
        ["run", *ctx.args],
        detail="-> dbt run",
        tunnel_db=tunnel_db,
        tunnel_port=tunnel_port,
    )


@dbt_app.command("test", context_settings=PASSTHROUGH)
def test(
    ctx: typer.Context,
    env: str = typer.Option(..., "--env", help="Stage: local, clinic, or demo"),
    tunnel_db: bool = typer.Option(False, "--tunnel-db"),
    tunnel_port: Optional[int] = typer.Option(None, "--tunnel-port"),
) -> None:
    """Run dbt test with isolated injected env."""
    _execute_dbt(
        env,
        ["test", *ctx.args],
        detail="-> dbt test",
        tunnel_db=tunnel_db,
        tunnel_port=tunnel_port,
    )


@dbt_app.command("docs", context_settings=PASSTHROUGH)
def docs(
    ctx: typer.Context,
    env: str = typer.Option(..., "--env", help="Stage: local, clinic, or demo"),
    serve: bool = typer.Option(
        False,
        "--serve",
        help="Run dbt docs serve (default subcommand is docs generate)",
    ),
    tunnel_db: bool = typer.Option(False, "--tunnel-db"),
    tunnel_port: Optional[int] = typer.Option(None, "--tunnel-port"),
) -> None:
    """Generate or serve dbt docs with isolated injected env."""
    subcommand = "serve" if serve else "generate"
    _execute_dbt(
        env,
        ["docs", subcommand, *ctx.args],
        detail=f"-> dbt docs {subcommand}",
        tunnel_db=tunnel_db,
        tunnel_port=tunnel_port,
    )


@dbt_app.command("invoke", context_settings=PASSTHROUGH, hidden=True)
def invoke(
    ctx: typer.Context,
    env: str = typer.Option(..., "--env", help="Stage: local, clinic, or demo"),
    tunnel_db: bool = typer.Option(False, "--tunnel-db"),
    tunnel_port: Optional[int] = typer.Option(None, "--tunnel-port"),
) -> None:
    """Run an arbitrary dbt subcommand (deps, build, compile, …)."""
    if not ctx.args:
        typer.echo("Usage: mdc dbt invoke --env <stage> -- <dbt subcommand> [args]", err=True)
        raise typer.Exit(code=2)
    _execute_dbt(
        env,
        list(ctx.args),
        detail=f"-> dbt {' '.join(ctx.args)}",
        tunnel_db=tunnel_db,
        tunnel_port=tunnel_port,
    )
