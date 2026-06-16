"""dbt commands."""

from __future__ import annotations

import typer

from mdc_cli.dbt_env import dbt_config_path, validate_dbt_stage
from mdc_cli.output import finish_validation
from mdc_cli.run_helper import echo_run_banner, run_dbt_command
from mdc_cli.stages import require_dbt_stage

dbt_app = typer.Typer(help="dbt project commands")

PASSTHROUGH = {"allow_extra_args": True, "ignore_unknown_options": True}


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
) -> None:
    """Run dbt with isolated injected env (extra args after --)."""
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
    echo_run_banner("dbt", env, dbt_config_path(env), "→ dbt run")
    code = run_dbt_command(env, ["run", *ctx.args])
    raise typer.Exit(code=code)


@dbt_app.command("test", context_settings=PASSTHROUGH)
def test(
    ctx: typer.Context,
    env: str = typer.Option(..., "--env", help="Stage: local, clinic, or demo"),
) -> None:
    """Run dbt test with isolated injected env."""
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
    echo_run_banner("dbt", env, dbt_config_path(env), "→ dbt test")
    code = run_dbt_command(env, ["test", *ctx.args])
    raise typer.Exit(code=code)


@dbt_app.command("docs", context_settings=PASSTHROUGH)
def docs(
    ctx: typer.Context,
    env: str = typer.Option(..., "--env", help="Stage: local, clinic, or demo"),
    serve: bool = typer.Option(
        False,
        "--serve",
        help="Run dbt docs serve (default subcommand is docs generate)",
    ),
) -> None:
    """Generate or serve dbt docs with isolated injected env."""
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
    subcommand = "serve" if serve else "generate"
    echo_run_banner("dbt", env, dbt_config_path(env), f"→ dbt docs {subcommand}")
    code = run_dbt_command(env, ["docs", subcommand, *ctx.args])
    raise typer.Exit(code=code)


@dbt_app.command("invoke", context_settings=PASSTHROUGH, hidden=True)
def invoke(
    ctx: typer.Context,
    env: str = typer.Option(..., "--env", help="Stage: local, clinic, or demo"),
) -> None:
    """Run an arbitrary dbt subcommand (deps, build, compile, …)."""
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
    if not ctx.args:
        typer.echo("Usage: mdc dbt invoke --env <stage> -- <dbt subcommand> [args]", err=True)
        raise typer.Exit(code=2)
    echo_run_banner("dbt", env, dbt_config_path(env), f"→ dbt {' '.join(ctx.args)}")
    code = run_dbt_command(env, list(ctx.args))
    raise typer.Exit(code=code)
