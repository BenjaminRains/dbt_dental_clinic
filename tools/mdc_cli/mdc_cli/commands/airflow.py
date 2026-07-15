"""Native Airflow bootstrap and runtime (wraps scripts/airflow/)."""

from __future__ import annotations

import typer

from mdc_cli.airflow_scripts import (
    run_airflow_docker_init,
    run_airflow_init,
    run_airflow_logs,
    run_airflow_start,
)

airflow_app = typer.Typer(help="Native Airflow bootstrap, start, and logs (Windows)")


@airflow_app.command("init")
def init_cmd(
    docker: bool = typer.Option(
        False,
        "--docker",
        help="Use Docker Compose init (init-airflow.sh) instead of native Windows venv.",
    ),
) -> None:
    """Initialize Airflow metadata DB, admin user, and Phase A variables (no DAG run)."""
    if docker:
        code = run_airflow_docker_init()
    else:
        try:
            code = run_airflow_init()
        except RuntimeError as exc:
            typer.echo(str(exc), err=True)
            raise typer.Exit(code=2) from exc
    raise typer.Exit(code=code)


@airflow_app.command("start")
def start_cmd(
    scheduler: bool = typer.Option(
        False,
        "--scheduler",
        help="Start scheduler only (recommended terminal 1 on Windows).",
    ),
    dag_processor: bool = typer.Option(
        False,
        "--dag-processor",
        help="Start dag-processor only (recommended terminal 2 on Windows; required in Airflow 3).",
    ),
    api_server: bool = typer.Option(
        False,
        "--api-server",
        help="Start api-server only (recommended terminal 3 on Windows; UI at :8080).",
    ),
    webserver: bool = typer.Option(
        False,
        "--webserver",
        help="Alias for --api-server (Airflow 2 name).",
    ),
) -> None:
    """Start native Airflow scheduler, dag-processor, and/or api-server."""
    flags = sum(bool(x) for x in (scheduler, dag_processor, api_server, webserver))
    if flags > 1:
        typer.echo(
            "Run --scheduler, --dag-processor, and --api-server in separate terminals, not together.",
            err=True,
        )
        raise typer.Exit(code=2)
    try:
        code = run_airflow_start(
            scheduler=scheduler,
            dag_processor=dag_processor,
            api_server=api_server or webserver,
        )
    except RuntimeError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=2) from exc
    raise typer.Exit(code=code)


@airflow_app.command("logs")
def logs_cmd(
    dag_id: str = typer.Option("etl_pipeline", "--dag-id", help="DAG id under airflow/logs."),
    run_id: str = typer.Option("", "--run-id", help="Specific run_id to inspect."),
    task: str = typer.Option("", "--task", help="Task id filter."),
    limit: int = typer.Option(8, "--limit", help="Recent runs to list when run-id omitted."),
    tail: int = typer.Option(0, "--tail", help="Lines to tail from a task log file."),
) -> None:
    """List recent DAG runs and log paths (Windows-safe)."""
    try:
        code = run_airflow_logs(
            dag_id=dag_id,
            run_id=run_id,
            task=task,
            limit=limit,
            tail=tail,
        )
    except RuntimeError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=2) from exc
    raise typer.Exit(code=code)
