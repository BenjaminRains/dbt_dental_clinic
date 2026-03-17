from __future__ import annotations

"""
Agent-oriented ETL CLI.

Intended usage examples:
- Machine-readable health checks for automation:
    python -m etl_pipeline.cli.agent_commands status --since 2025-01-01T00:00:00 --format json --quiet

- Filtered status for a specific clinic / pipeline window:
    python -m etl_pipeline.cli.agent_commands status --clinic-id 123 --pipeline daily_ingest --since 2025-01-10T00:00:00 --until 2025-01-11T00:00:00 --format json --quiet

- Human-friendly inspection with structured output:
    python -m etl_pipeline.cli.agent_commands status --since 2025-01-01T00:00:00 --format text

This module is not currently wired into the main `etl_pipeline` CLI entry point; it is designed
for direct invocation by agents, scripts, or tools that prefer a stable JSON envelope.
"""

import json
import sys
from datetime import datetime
from typing import Any, Dict, Optional

import click

from etl_pipeline.core import ops


def _make_envelope_ok(data: Any) -> Dict[str, Any]:
    return {"status": "ok", "data": data, "error": None}


def _make_envelope_error(code: str, message: str, details: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    return {
        "status": "error",
        "data": None,
        "error": {"code": code, "message": message, "details": details or {}},
    }


def _print_output(payload: Dict[str, Any], fmt: str, *, quiet: bool) -> None:
    if fmt == "json":
        sys.stdout.write(json.dumps(payload, default=str) + "\n")
    else:
        # For now, render the same structure but pretty-printed.
        # Human-centric formatting can be improved later.
        click.echo(json.dumps(payload, indent=2, default=str))


@click.group()
@click.option(
    "--format",
    "fmt",
    type=click.Choice(["text", "json"], case_sensitive=False),
    default="text",
    show_default=True,
    help="Output format.",
)
@click.option(
    "--no-color",
    is_flag=True,
    default=False,
    help="Disable colors in text output.",
)
@click.option(
    "--quiet",
    is_flag=True,
    default=False,
    help="Suppress non-essential logs; stdout is just the payload.",
)
@click.pass_context
def cli(ctx: click.Context, fmt: str, no_color: bool, quiet: bool) -> None:
    """Agent-oriented ETL CLI with machine-readable output."""
    ctx.ensure_object(dict)
    ctx.obj["fmt"] = fmt.lower()
    ctx.obj["quiet"] = quiet
    # no_color is reserved for future text formatting adjustments


@cli.command()
@click.option("--clinic-id", required=False)
@click.option("--pipeline", required=False)
@click.option("--since", required=False, help="ISO-8601 timestamp, e.g. 2025-01-01T00:00:00")
@click.option("--until", required=False, help="ISO-8601 timestamp.")
@click.pass_context
def status(ctx: click.Context, clinic_id: str, pipeline: str, since: str, until: str) -> None:
    """Return recent pipeline run summaries."""
    fmt: str = ctx.obj["fmt"]
    quiet: bool = ctx.obj["quiet"]

    try:
        since_dt = datetime.fromisoformat(since) if since else None
        until_dt = datetime.fromisoformat(until) if until else None

        runs = ops.get_status(
            clinic_id=clinic_id or None,
            pipeline=pipeline or None,
            since=since_dt,
            until=until_dt,
        )
        data = [r.to_dict() for r in runs]
        payload = _make_envelope_ok({"runs": data})
        _print_output(payload, fmt, quiet=quiet)
        sys.exit(0)
    except ops.EtlError as e:
        payload = _make_envelope_error(e.code, str(e), e.details)
        _print_output(payload, fmt, quiet=quiet)
        # Exit code mapping can be refined later; for now, non-zero on EtlError.
        sys.exit(1)
    except Exception as e:
        payload = _make_envelope_error("UNEXPECTED", str(e))
        _print_output(payload, fmt, quiet=quiet)
        sys.exit(1)


if __name__ == "__main__":
    cli()

