"""Consult audio pipeline commands (Phase 5.4)."""

from __future__ import annotations

import typer

from mdc_cli.consult_audio_env import (
    install_consult_audio_venv,
    normalize_run_args,
    run_consult_audio_module,
    run_consult_audio_pipeline,
    validate_consult_audio,
)
from mdc_cli.output import ascii_cli_text

consult_audio_app = typer.Typer(help="Consult audio pipe (stateless venv + child env)")

pipeline_app = typer.Typer(help="consult_audio_pipe.pipeline orchestrator")

PASSTHROUGH = {"allow_extra_args": True, "ignore_unknown_options": True}


@consult_audio_app.command("validate")
def validate_cmd() -> None:
    """Check venv, requirements, and API keys in consult_audio_pipe/.env."""
    result = validate_consult_audio()
    for warning in result.warnings:
        typer.echo(f"warning: {ascii_cli_text(warning)}", err=True)
    for error in result.errors:
        typer.echo(ascii_cli_text(error), err=True)
    if result.ok:
        typer.echo("consult-audio validate: ok")
        raise typer.Exit(code=0)
    raise typer.Exit(code=1)


@consult_audio_app.command("install")
def install_cmd() -> None:
    """Create consult_audio_pipe/venv and pip install -r requirements.txt."""
    code = install_consult_audio_venv()
    raise typer.Exit(code=code)


@consult_audio_app.command("run", context_settings=PASSTHROUGH)
def run_cmd(ctx: typer.Context) -> None:
    """Run a command in the consult audio venv (extra args after --)."""
    if not ctx.args:
        typer.echo(
            "Usage: mdc consult-audio run -- python -m consult_audio_pipe.pipeline status",
            err=True,
        )
        raise typer.Exit(code=2)
    args = normalize_run_args(list(ctx.args))
    if not args:
        typer.echo("No command after python.", err=True)
        raise typer.Exit(code=2)
    code = run_consult_audio_module(args)
    raise typer.Exit(code=code)


@pipeline_app.command("run")
def pipeline_run(
    llm: str = typer.Option(
        "claude",
        "--llm",
        help="LLM backend: claude or chatgpt",
    ),
) -> None:
    """Run full audio → transcript → analysis pipeline."""
    if llm not in ("claude", "chatgpt"):
        typer.echo("--llm must be claude or chatgpt.", err=True)
        raise typer.Exit(code=2)
    code = run_consult_audio_pipeline("run", llm=llm)
    raise typer.Exit(code=code)


@pipeline_app.command("status")
def pipeline_status() -> None:
    """Show pipeline directory status."""
    code = run_consult_audio_pipeline("status")
    raise typer.Exit(code=code)


@pipeline_app.command("validate")
def pipeline_validate() -> None:
    """Validate transcript files on disk."""
    code = run_consult_audio_pipeline("validate")
    raise typer.Exit(code=code)


@pipeline_app.command("cleanup")
def pipeline_cleanup() -> None:
    """Remove invalid transcript files."""
    code = run_consult_audio_pipeline("cleanup")
    raise typer.Exit(code=code)


consult_audio_app.add_typer(pipeline_app, name="pipeline")


@consult_audio_app.command("analyze")
def analyze_cmd() -> None:
    """Run legacy LLM analysis script on clean transcripts (Claude)."""
    code = run_consult_audio_module(
        ["scripts/llm_analysis_integration.py", "analyze"],
    )
    raise typer.Exit(code=code)


@consult_audio_app.command("tokens")
def tokens_cmd() -> None:
    """Show token usage from prior LLM analysis runs."""
    code = run_consult_audio_module(
        ["scripts/llm_analysis_integration.py", "tokens"],
    )
    raise typer.Exit(code=code)
