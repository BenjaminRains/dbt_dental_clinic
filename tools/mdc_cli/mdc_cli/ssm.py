"""AWS SSM port-forward and shell sessions (Phase 5.1)."""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Iterator, Optional

import typer

from mdc_cli.credentials import _dig, _norm, load_deployment_credentials
from mdc_cli.paths import REPO_ROOT
from mdc_cli.process_util import (
    find_executable,
    resolve_cmd,
    run_subprocess,
    run_subprocess_completed,
)
from mdc_cli.run_helper import is_local_tcp_port_open

logger = logging.getLogger(__name__)


class ClinicDbTunnelError(RuntimeError):
    """Clinic RDS SSM tunnel could not be started or did not become ready."""

SSM_PORT_FORWARD_DOCUMENT = "AWS-StartPortForwardingSessionToRemoteHost"


@dataclass(frozen=True)
class SsmContext:
    api_instance_id: Optional[str]
    clinic_api_instance_id: Optional[str]
    demo_db_instance_id: Optional[str]
    rds_endpoint: Optional[str]
    demo_db_host: Optional[str]
    demo_db_port: Optional[str]


def load_ssm_context() -> SsmContext:
    creds = load_deployment_credentials()
    if not creds:
        return SsmContext(None, None, None, None, None, None)

    api_id = _norm(_dig(creds, "backend_api", "ec2", "instance_id"))
    clinic_id = _norm(_dig(creds, "backend_api", "clinic_api", "ec2", "instance_id"))
    demo_db_id = _norm(_dig(creds, "demo_database", "ec2", "instance_id"))

    rds = _norm(
        _dig(creds, "backend_api", "clinic_database_reference", "rds", "endpoint")
    )
    if not rds:
        rds = _norm(
            _dig(creds, "backend_api", "production_database_reference", "rds", "endpoint")
        )

    demo_host = _norm(_dig(creds, "demo_database", "database_connection", "host"))
    demo_port = _norm(_dig(creds, "demo_database", "database_connection", "port"))

    return SsmContext(
        api_instance_id=api_id,
        clinic_api_instance_id=clinic_id,
        demo_db_instance_id=demo_db_id,
        rds_endpoint=rds,
        demo_db_host=demo_host,
        demo_db_port=demo_port or "5432",
    )


def port_forward_parameters_json(
    hostname: str,
    remote_port: str,
    local_port: str,
) -> str:
    """
    JSON string for ``aws ssm start-session --parameters``.

    Use ``json.dumps`` (valid JSON). The legacy PowerShell helper in
    ``scripts/ssm_tunnels.ps1`` uses backslash-escaped quotes because the
    Windows shell strips ``ConvertTo-Json`` output; Python ``subprocess`` passes
    argv directly to ``aws.exe`` and requires normal JSON.
    """
    return json.dumps(
        {
            "host": [hostname],
            "portNumber": [remote_port],
            "localPortNumber": [local_port],
        }
    )


def _require_aws() -> None:
    if find_executable("aws") is None:
        typer.echo("AWS CLI not found.", err=True)
        raise typer.Exit(code=127)
    plugin = find_executable("session-manager-plugin")
    if plugin is None:
        typer.echo(
            "Session Manager plugin not found. "
            "Install: winget install Amazon.SessionManagerPlugin",
            err=True,
        )


def _port_forward_cmd(
    target_instance_id: str,
    hostname: str,
    local_port: str,
    *,
    remote_port: str = "5432",
) -> list[str]:
    params = port_forward_parameters_json(hostname, remote_port, local_port)
    return [
        "aws",
        "ssm",
        "start-session",
        "--target",
        target_instance_id,
        "--document-name",
        SSM_PORT_FORWARD_DOCUMENT,
        "--parameters",
        params,
    ]


def wait_for_local_port(
    host: str,
    port: int,
    *,
    timeout_seconds: float = 90.0,
    poll_interval: float = 1.0,
) -> bool:
    """Poll until host:port accepts TCP or timeout."""
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if is_local_tcp_port_open(host, port, timeout=min(poll_interval, 3.0)):
            return True
        time.sleep(poll_interval)
    return False


def _start_port_forward_background(
    target_instance_id: str,
    hostname: str,
    local_port: str,
    *,
    remote_port: str = "5432",
) -> subprocess.Popen[str]:
    _require_aws()
    cmd = resolve_cmd(
        _port_forward_cmd(
            target_instance_id,
            hostname,
            local_port,
            remote_port=remote_port,
        )
    )
    popen_kwargs: dict = {
        "cwd": str(REPO_ROOT),
        "stdout": subprocess.DEVNULL,
        "stderr": subprocess.PIPE,
        "text": True,
    }
    if sys.platform == "win32":
        popen_kwargs["creationflags"] = getattr(subprocess, "CREATE_NO_WINDOW", 0)
    return subprocess.Popen(cmd, **popen_kwargs)


def _stop_port_forward_process(proc: subprocess.Popen[str]) -> None:
    if proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=15)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)


@contextmanager
def managed_clinic_db_tunnel(
    *,
    local_port: Optional[str] = None,
    wait_timeout_seconds: float = 90.0,
) -> Iterator[str]:
    """
    Ensure localhost port forwards to clinic RDS.

    Yields ``existing`` when the port was already open (does not stop it on exit).
    Yields ``started`` when this call started a background SSM session (stopped on exit).
    """
    port_str = local_port or os.environ.get("POSTGRES_PORT") or "5433"
    port = int(port_str)

    if is_local_tcp_port_open("127.0.0.1", port, timeout=1.0):
        logger.info("Clinic DB tunnel already listening on 127.0.0.1:%s", port)
        yield "existing"
        return

    ctx = load_ssm_context()
    if not ctx.clinic_api_instance_id:
        raise ClinicDbTunnelError(
            "dental-clinic-api-clinic instance ID missing "
            "(backend_api.clinic_api.ec2.instance_id)."
        )
    if not ctx.rds_endpoint:
        raise ClinicDbTunnelError("RDS endpoint not in deployment_credentials.json.")

    logger.info(
        "Starting background SSM tunnel to clinic RDS via %s (127.0.0.1:%s)",
        ctx.clinic_api_instance_id,
        port,
    )
    proc = _start_port_forward_background(
        ctx.clinic_api_instance_id,
        ctx.rds_endpoint,
        port_str,
    )
    try:
        if proc.poll() is not None:
            stderr = (proc.stderr.read() if proc.stderr else "") or ""
            raise ClinicDbTunnelError(
                f"SSM port-forward exited immediately (code {proc.returncode}). {stderr.strip()}"
            )
        if not wait_for_local_port("127.0.0.1", port, timeout_seconds=wait_timeout_seconds):
            stderr = ""
            if proc.stderr:
                try:
                    stderr = proc.stderr.read() or ""
                except Exception:
                    pass
            raise ClinicDbTunnelError(
                f"SSM tunnel did not open 127.0.0.1:{port} within {wait_timeout_seconds:.0f}s. "
                f"{stderr.strip()}"
            )
        logger.info("Clinic DB tunnel ready on 127.0.0.1:%s", port)
        yield "started"
    finally:
        _stop_port_forward_process(proc)


def start_port_forward_session(
    target_instance_id: str,
    hostname: str,
    local_port: str,
    *,
    remote_port: str = "5432",
    label: str = "",
) -> int:
    _require_aws()
    if label:
        typer.echo(f"TUNNEL  {label}")
    typer.echo(f"  Local port: {local_port}")
    typer.echo(f"  Remote: {hostname}:{remote_port}")
    typer.echo(f"  Via instance: {target_instance_id}")
    typer.echo("Keep this terminal open. Press Ctrl+C to stop forwarding.")
    return run_subprocess(_port_forward_cmd(target_instance_id, hostname, local_port, remote_port=remote_port), cwd=REPO_ROOT)


def start_ssm_shell_session(target_instance_id: str, label: str) -> int:
    _require_aws()
    typer.echo(f"SSM shell: {label} ({target_instance_id})")
    return run_subprocess(
        ["aws", "ssm", "start-session", "--target", target_instance_id],
        cwd=REPO_ROOT,
    )


def tunnel_clinic_db() -> int:
    ctx = load_ssm_context()
    if not ctx.clinic_api_instance_id:
        typer.echo(
            "dental-clinic-api-clinic instance ID missing "
            "(backend_api.clinic_api.ec2.instance_id).",
            err=True,
        )
        return 1
    if not ctx.rds_endpoint:
        typer.echo("RDS endpoint not in deployment_credentials.json.", err=True)
        return 1
    local_port = os.environ.get("POSTGRES_PORT") or "5433"
    return start_port_forward_session(
        ctx.clinic_api_instance_id,
        ctx.rds_endpoint,
        local_port,
        label="clinic-db (RDS via dental-clinic-api-clinic)",
    )


def tunnel_rds_demo() -> int:
    ctx = load_ssm_context()
    if not ctx.api_instance_id:
        typer.echo(
            "dental-clinic-api-demo instance ID missing (backend_api.ec2.instance_id).",
            err=True,
        )
        return 1
    if not ctx.rds_endpoint:
        typer.echo("RDS endpoint not in deployment_credentials.json.", err=True)
        return 1
    local_port = os.environ.get("POSTGRES_PORT") or "5433"
    return start_port_forward_session(
        ctx.api_instance_id,
        ctx.rds_endpoint,
        local_port,
        label="rds (via dental-clinic-api-demo)",
    )


def tunnel_demo_db() -> int:
    ctx = load_ssm_context()
    if not ctx.api_instance_id:
        typer.echo(
            "dental-clinic-api-demo instance ID missing (needed for demo DB tunnel).",
            err=True,
        )
        return 1
    if not ctx.demo_db_host:
        typer.echo("Demo DB host not in deployment_credentials.json.", err=True)
        return 1
    local_port = os.environ.get("DEMO_POSTGRES_PORT") or "5434"
    return start_port_forward_session(
        ctx.api_instance_id,
        ctx.demo_db_host,
        local_port,
        label="demo-db (via dental-clinic-api-demo)",
    )


def connect_api() -> int:
    ctx = load_ssm_context()
    if not ctx.api_instance_id:
        typer.echo("Demo API instance ID missing.", err=True)
        return 1
    return start_ssm_shell_session(ctx.api_instance_id, "dental-clinic-api-demo")


def connect_clinic_api() -> int:
    ctx = load_ssm_context()
    if not ctx.clinic_api_instance_id:
        typer.echo("Clinic API instance ID missing.", err=True)
        return 1
    return start_ssm_shell_session(
        ctx.clinic_api_instance_id,
        "dental-clinic-api-clinic",
    )


def connect_demo_db() -> int:
    ctx = load_ssm_context()
    if not ctx.demo_db_instance_id:
        typer.echo("Demo DB instance ID missing.", err=True)
        return 1
    return start_ssm_shell_session(ctx.demo_db_instance_id, "dental-clinic-demo-db")


def print_ssm_status() -> None:
    typer.echo("SSM environment status")
    aws = find_executable("aws")
    typer.echo(f"  AWS CLI: {'ok' if aws else 'missing'}")
    plugin = find_executable("session-manager-plugin")
    typer.echo(
        f"  Session Manager plugin: {'ok' if plugin else 'missing'}"
    )
    if aws:
        completed = run_subprocess_completed(
            ["aws", "sts", "get-caller-identity"],
            cwd=REPO_ROOT,
        )
        if completed.returncode != 0:
            typer.echo("  AWS credentials: not configured or invalid", err=True)
        else:
            try:
                identity = json.loads(completed.stdout)
                account = identity.get("Account", "?")
                arn = identity.get("Arn", "?")
                typer.echo(f"  AWS credentials: ok (account {account})")
                typer.echo(f"    {arn}")
            except json.JSONDecodeError:
                typer.echo("  AWS credentials: ok")

    if not load_deployment_credentials():
        typer.echo("  deployment_credentials.json: missing or invalid", err=True)
        return

    ctx = load_ssm_context()
    typer.echo("  Instance IDs (from deployment_credentials.json):")
    typer.echo(
        f"    dental-clinic-api-demo: {ctx.api_instance_id or 'not loaded'}"
    )
    typer.echo(
        f"    dental-clinic-api-clinic: {ctx.clinic_api_instance_id or 'not loaded'}"
    )
    typer.echo(
        f"    dental-clinic-demo-db: {ctx.demo_db_instance_id or 'not loaded'}"
    )
    typer.echo(f"    RDS endpoint: {ctx.rds_endpoint or 'not loaded'}")
    typer.echo(f"    Demo DB host: {ctx.demo_db_host or 'not loaded'}")
