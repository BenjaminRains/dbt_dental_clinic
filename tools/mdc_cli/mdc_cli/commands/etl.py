"""ETL commands."""

from __future__ import annotations

from typing import Optional

import typer

from mdc_cli.env import validate_etl_stage
from mdc_cli.output import finish_validation
from mdc_cli.paths import ETL_DIR, default_etl_profile, etl_env_file, etl_pipeline_config_arg
from mdc_cli.run_helper import (
    echo_run_banner,
    load_env_dict_isolated,
    require_component_python,
    run_isolated,
    venv_root_from_python,
)
from mdc_cli.stages import require_etl_profile, require_etl_stage

etl_app = typer.Typer(help="ETL pipeline commands")

PASSTHROUGH = {"allow_extra_args": True, "ignore_unknown_options": True}


def _cli_args_with_default_config(ctx: typer.Context) -> list[str]:
    """Prepend default pipeline.yml when status is invoked without --config."""
    args = list(ctx.args)
    has_config = any(
        arg in ("--config", "-c") or arg.startswith("--config=") for arg in args
    )
    if not has_config:
        args = ["--config", etl_pipeline_config_arg(), *args]
    return args


def _tunnel_options(
    tunnel_db: bool,
    tunnel_port: Optional[int],
) -> str:
    if not tunnel_db:
        return ""
    port = tunnel_port if tunnel_port is not None else 5433
    return f" tunnel-db->127.0.0.1:{port}"


def _load_etl_settings(
    env: str,
    profile: str,
    *,
    tunnel_db: bool = False,
    tunnel_port: Optional[int] = None,
) -> dict[str, str]:
    return load_env_dict_isolated(
        "etl",
        env,
        profile=profile,
        tunnel_db=tunnel_db,
        tunnel_port=tunnel_port,
    )


@etl_app.command("validate")
def validate(
    env: str = typer.Option(..., "--env", help="Stage: local, clinic, or test"),
    profile: Optional[str] = typer.Option(
        None,
        "--profile",
        help="Connection subset: load (repl+analytics) or full (all three)",
    ),
    tunnel_db: bool = typer.Option(
        False,
        "--tunnel-db",
        help="Validate clinic RDS via localhost tunnel (mdc tunnel clinic-db)",
    ),
    tunnel_port: Optional[int] = typer.Option(
        None,
        "--tunnel-port",
        help="Local tunnel port (default: POSTGRES_PORT env or 5433)",
    ),
) -> None:
    """Validate ETL pydantic settings for a stage."""
    require_etl_stage(env)
    resolved_profile = require_etl_profile(profile or default_etl_profile(env))
    ok, error = validate_etl_stage(
        env,
        profile=resolved_profile,
        tunnel_db=tunnel_db,
        tunnel_port=tunnel_port,
    )
    finish_validation(
        component="etl",
        stage=env,
        config_path=etl_env_file(env),
        ok=ok,
        error=error,
        profile=resolved_profile,
    )


def _require_valid_etl(
    env: str,
    profile: str,
    *,
    tunnel_db: bool = False,
    tunnel_port: Optional[int] = None,
) -> None:
    ok, error = validate_etl_stage(
        env,
        profile=profile,
        tunnel_db=tunnel_db,
        tunnel_port=tunnel_port,
    )
    if not ok:
        finish_validation(
            component="etl",
            stage=env,
            config_path=etl_env_file(env),
            ok=False,
            error=error,
            profile=profile,
        )


@etl_app.command("run", context_settings=PASSTHROUGH)
def run(
    ctx: typer.Context,
    env: str = typer.Option(..., "--env", help="Stage: local, clinic, or test"),
    profile: str = typer.Option(
        "full",
        "--profile",
        help="Default full - full pipeline requires source + replication + analytics",
    ),
    tunnel_db: bool = typer.Option(
        False,
        "--tunnel-db",
        help="Route clinic Postgres to localhost (mdc tunnel clinic-db)",
    ),
    tunnel_port: Optional[int] = typer.Option(
        None,
        "--tunnel-port",
        help="Local tunnel port (default: POSTGRES_PORT env or 5433)",
    ),
) -> None:
    """Run ETL pipeline with isolated injected env (passthrough args after --)."""
    require_etl_stage(env)
    resolved_profile = require_etl_profile(profile)
    _require_valid_etl(
        env,
        resolved_profile,
        tunnel_db=tunnel_db,
        tunnel_port=tunnel_port,
    )

    settings = _load_etl_settings(
        env,
        resolved_profile,
        tunnel_db=tunnel_db,
        tunnel_port=tunnel_port,
    )
    python = require_component_python("etl")
    cmd = [str(python), "-m", "etl_pipeline.cli.main", "run", *ctx.args]
    echo_run_banner(
        "etl",
        env,
        etl_env_file(env),
        f"profile={resolved_profile}  -> etl run{_tunnel_options(tunnel_db, tunnel_port)}",
    )
    code = run_isolated(
        settings=settings,
        cmd=cmd,
        cwd=ETL_DIR,
        venv_root=venv_root_from_python(python),
    )
    raise typer.Exit(code=code)


@etl_app.command("test-connections", context_settings=PASSTHROUGH)
def test_connections(
    ctx: typer.Context,
    env: str = typer.Option(..., "--env", help="Stage: local, clinic, or test"),
    profile: str = typer.Option("full", "--profile"),
    tunnel_db: bool = typer.Option(False, "--tunnel-db"),
    tunnel_port: Optional[int] = typer.Option(None, "--tunnel-port"),
) -> None:
    """Test database connections with isolated injected env."""
    require_etl_stage(env)
    resolved_profile = require_etl_profile(profile)
    _require_valid_etl(
        env,
        resolved_profile,
        tunnel_db=tunnel_db,
        tunnel_port=tunnel_port,
    )

    settings = _load_etl_settings(
        env,
        resolved_profile,
        tunnel_db=tunnel_db,
        tunnel_port=tunnel_port,
    )
    python = require_component_python("etl")
    cmd = [str(python), "-m", "etl_pipeline.cli.main", "test-connections", *ctx.args]
    echo_run_banner(
        "etl",
        env,
        etl_env_file(env),
        f"profile={resolved_profile}  -> test-connections{_tunnel_options(tunnel_db, tunnel_port)}",
    )
    code = run_isolated(
        settings=settings,
        cmd=cmd,
        cwd=ETL_DIR,
        venv_root=venv_root_from_python(python),
    )
    raise typer.Exit(code=code)


@etl_app.command("status", context_settings=PASSTHROUGH)
def status_cmd(
    ctx: typer.Context,
    env: str = typer.Option(..., "--env", help="Stage: local, clinic, or test"),
    profile: Optional[str] = typer.Option(
        None,
        "--profile",
        help="Connection subset for env load (default: load for local, full otherwise)",
    ),
    tunnel_db: bool = typer.Option(False, "--tunnel-db"),
    tunnel_port: Optional[int] = typer.Option(None, "--tunnel-port"),
) -> None:
    """Run ETL pipeline status with isolated injected env."""
    require_etl_stage(env)
    resolved_profile = require_etl_profile(profile or default_etl_profile(env))
    _require_valid_etl(
        env,
        resolved_profile,
        tunnel_db=tunnel_db,
        tunnel_port=tunnel_port,
    )

    settings = _load_etl_settings(
        env,
        resolved_profile,
        tunnel_db=tunnel_db,
        tunnel_port=tunnel_port,
    )
    python = require_component_python("etl")
    cmd = [str(python), "-m", "etl_pipeline.cli.main", "status", *_cli_args_with_default_config(ctx)]
    echo_run_banner(
        "etl",
        env,
        etl_env_file(env),
        f"profile={resolved_profile}  -> etl status{_tunnel_options(tunnel_db, tunnel_port)}",
    )
    code = run_isolated(
        settings=settings,
        cmd=cmd,
        cwd=ETL_DIR,
        venv_root=venv_root_from_python(python),
    )
    raise typer.Exit(code=code)


@etl_app.command("schema", context_settings=PASSTHROUGH)
def schema_cmd(
    ctx: typer.Context,
    env: str = typer.Option(..., "--env", help="Stage: local, clinic, or test"),
    profile: str = typer.Option(
        "full",
        "--profile",
        help="Default full - schema analysis requires source database access",
    ),
    tunnel_db: bool = typer.Option(False, "--tunnel-db"),
    tunnel_port: Optional[int] = typer.Option(None, "--tunnel-port"),
) -> None:
    """Update tables.yml by running OpenDental schema analysis."""
    require_etl_stage(env)
    resolved_profile = require_etl_profile(profile)
    _require_valid_etl(
        env,
        resolved_profile,
        tunnel_db=tunnel_db,
        tunnel_port=tunnel_port,
    )

    settings = _load_etl_settings(
        env,
        resolved_profile,
        tunnel_db=tunnel_db,
        tunnel_port=tunnel_port,
    )
    python = require_component_python("etl")
    cmd = [str(python), "-m", "etl_pipeline.cli.main", "update-schema", *ctx.args]
    echo_run_banner(
        "etl",
        env,
        etl_env_file(env),
        f"profile={resolved_profile}  -> etl schema (update-schema){_tunnel_options(tunnel_db, tunnel_port)}",
    )
    code = run_isolated(
        settings=settings,
        cmd=cmd,
        cwd=ETL_DIR,
        venv_root=venv_root_from_python(python),
    )
    raise typer.Exit(code=code)


@etl_app.command("exec", context_settings=PASSTHROUGH)
def exec_cmd(
    ctx: typer.Context,
    env: str = typer.Option(..., "--env", help="Stage: local, clinic, or test"),
    profile: str = typer.Option("full", "--profile"),
    tunnel_db: bool = typer.Option(False, "--tunnel-db"),
    tunnel_port: Optional[int] = typer.Option(None, "--tunnel-port"),
) -> None:
    """Run an arbitrary command with composed ETL env (passthrough after --)."""
    require_etl_stage(env)
    resolved_profile = require_etl_profile(profile)
    if not ctx.args:
        typer.echo(
            "Usage: mdc etl exec --env <stage> [--tunnel-db] -- <command> [args...]",
            err=True,
        )
        raise typer.Exit(code=2)

    _require_valid_etl(
        env,
        resolved_profile,
        tunnel_db=tunnel_db,
        tunnel_port=tunnel_port,
    )
    settings = _load_etl_settings(
        env,
        resolved_profile,
        tunnel_db=tunnel_db,
        tunnel_port=tunnel_port,
    )
    python = require_component_python("etl")
    echo_run_banner(
        "etl",
        env,
        etl_env_file(env),
        f"profile={resolved_profile}  -> exec {_tunnel_options(tunnel_db, tunnel_port)}",
    )
    code = run_isolated(
        settings=settings,
        cmd=list(ctx.args),
        cwd=ETL_DIR,
        venv_root=venv_root_from_python(python),
    )
    raise typer.Exit(code=code)


@etl_app.command("invoke", context_settings=PASSTHROUGH, hidden=True)
def invoke(
    ctx: typer.Context,
    env: str = typer.Option(..., "--env", help="Stage: local, clinic, or test"),
    profile: str = typer.Option("full", "--profile"),
    tunnel_db: bool = typer.Option(False, "--tunnel-db"),
    tunnel_port: Optional[int] = typer.Option(None, "--tunnel-port"),
) -> None:
    """Run an arbitrary etl_pipeline.cli subcommand."""
    require_etl_stage(env)
    resolved_profile = require_etl_profile(profile)
    if not ctx.args:
        typer.echo("Usage: mdc etl invoke --env <stage> -- <subcommand> [args]", err=True)
        raise typer.Exit(code=2)

    _require_valid_etl(
        env,
        resolved_profile,
        tunnel_db=tunnel_db,
        tunnel_port=tunnel_port,
    )

    settings = _load_etl_settings(
        env,
        resolved_profile,
        tunnel_db=tunnel_db,
        tunnel_port=tunnel_port,
    )
    python = require_component_python("etl")
    cmd = [str(python), "-m", "etl_pipeline.cli.main", *ctx.args]
    echo_run_banner(
        "etl",
        env,
        etl_env_file(env),
        f"profile={resolved_profile}  -> etl {' '.join(ctx.args)}{_tunnel_options(tunnel_db, tunnel_port)}",
    )
    code = run_isolated(
        settings=settings,
        cmd=cmd,
        cwd=ETL_DIR,
        venv_root=venv_root_from_python(python),
    )
    raise typer.Exit(code=code)


@etl_app.command("replicate")
def replicate(
    env: str = typer.Option(..., "--env", help="Stage: local, clinic, or test"),
    profile: str = typer.Option("full", "--profile"),
) -> None:
    """Not available — ETL CLI has no replicate subcommand; use mdc etl run."""
    typer.echo(
        "mdc etl replicate is not available: etl_pipeline.cli has no replicate command. "
        "Use `mdc etl run --env <stage> --profile full`.",
        err=True,
    )
    raise typer.Exit(code=2)
