"""Build and deploy frontend to S3/CloudFront (Phase 5.3)."""

from __future__ import annotations

import json
import os
import shutil
from pathlib import Path

import typer

from mdc_cli.credentials import FrontendDeployConfig, resolve_frontend_deploy_config
from mdc_cli.paths import FRONTEND_DIR, REPO_ROOT
from mdc_cli.process_util import find_executable, run_subprocess, run_subprocess_completed


def _run(cmd: list[str], *, cwd: Path, env: dict[str, str] | None = None) -> int:
    return run_subprocess(cmd, cwd=cwd, env=env)


def _which_or_fail(name: str) -> None:
    if find_executable(name) is None:
        typer.echo(f"Missing required tool: {name}", err=True)
        raise typer.Exit(code=127)


def check_deploy_prerequisites() -> None:
    _which_or_fail("aws")
    _which_or_fail("node")
    _which_or_fail("npm")
    code = _run(["aws", "sts", "get-caller-identity"], cwd=REPO_ROOT)
    if code != 0:
        typer.echo("AWS credentials not configured. Run aws configure.", err=True)
        raise typer.Exit(code=code)


def validate_s3_bucket(bucket_name: str) -> None:
    code = _run(["aws", "s3api", "head-bucket", "--bucket", bucket_name], cwd=REPO_ROOT)
    if code != 0:
        typer.echo(f"S3 bucket '{bucket_name}' not found or not accessible.", err=True)
        raise typer.Exit(code=code)


def validate_cloudfront_distribution(distribution_id: str) -> None:
    code = _run(
        ["aws", "cloudfront", "get-distribution", "--id", distribution_id],
        cwd=REPO_ROOT,
    )
    if code != 0:
        typer.echo(
            f"CloudFront distribution '{distribution_id}' not found or not accessible.",
            err=True,
        )
        raise typer.Exit(code=code)


def npm_install_if_needed(frontend_dir: Path) -> None:
    if (frontend_dir / "node_modules").is_dir():
        return
    typer.echo("Installing frontend dependencies...")
    code = _run(["npm", "install"], cwd=frontend_dir)
    if code != 0:
        typer.echo("Failed to install frontend dependencies.", err=True)
        raise typer.Exit(code=code)


def npm_build(frontend_dir: Path, config: FrontendDeployConfig) -> None:
    build_env = os.environ.copy()
    build_env["VITE_API_URL"] = config.api_url
    build_env["VITE_API_KEY"] = config.api_key
    build_env["VITE_IS_DEMO"] = "true" if config.vite_is_demo else "false"
    typer.echo(f"Building frontend (target={config.target}, API={config.api_url})...")
    code = _run(["npm", "run", "build"], cwd=frontend_dir, env=build_env)
    if code != 0:
        typer.echo("Frontend build failed.", err=True)
        raise typer.Exit(code=code)


def upload_dist_to_s3(dist_dir: Path, bucket_name: str) -> None:
    if not dist_dir.is_dir():
        typer.echo(f"Build directory not found: {dist_dir}", err=True)
        raise typer.Exit(code=1)
    if not any(dist_dir.iterdir()):
        typer.echo("dist directory is empty - nothing to upload.", err=True)
        raise typer.Exit(code=1)

    typer.echo("Uploading static assets to S3...")
    code = _run(
        [
            "aws",
            "s3",
            "sync",
            ".",
            f"s3://{bucket_name}/",
            "--delete",
            "--cache-control",
            "public, max-age=31536000, immutable",
            "--exclude",
            "*.html",
            "--exclude",
            "*.json",
        ],
        cwd=dist_dir,
    )
    if code != 0:
        typer.echo("Failed to upload static assets.", err=True)
        raise typer.Exit(code=code)

    typer.echo("Uploading HTML and JSON files...")
    code = _run(
        [
            "aws",
            "s3",
            "sync",
            ".",
            f"s3://{bucket_name}/",
            "--cache-control",
            "no-cache, no-store, must-revalidate",
            "--exclude",
            "*",
            "--include",
            "*.html",
            "--include",
            "*.json",
        ],
        cwd=dist_dir,
    )
    if code != 0:
        typer.echo("Failed to upload HTML/JSON files.", err=True)
        raise typer.Exit(code=code)


def invalidate_cloudfront(distribution_id: str, paths: list[str]) -> None:
    typer.echo("Invalidating CloudFront cache...")
    completed = run_subprocess_completed(
        [
            "aws",
            "cloudfront",
            "create-invalidation",
            "--distribution-id",
            distribution_id,
            "--paths",
            *paths,
        ],
        cwd=REPO_ROOT,
    )
    if completed.returncode != 0:
        typer.echo(
            "CloudFront invalidation failed (deployment may still be successful).",
            err=True,
        )
        return
    try:
        payload = json.loads(completed.stdout)
        invalidation_id = payload.get("Invalidation", {}).get("Id", "unknown")
        typer.echo(f"CloudFront invalidation created: {invalidation_id}")
    except json.JSONDecodeError:
        typer.echo("CloudFront invalidation submitted.")


def deploy_frontend_target(target: str) -> int:
    if not FRONTEND_DIR.is_dir():
        typer.echo(f"Frontend directory not found: {FRONTEND_DIR}", err=True)
        return 1
    if not (FRONTEND_DIR / "package.json").is_file():
        typer.echo("No package.json in frontend directory.", err=True)
        return 1

    try:
        config = resolve_frontend_deploy_config(target)
    except ValueError as exc:
        typer.echo(str(exc), err=True)
        return 1

    typer.echo(f"DEPLOY  frontend  {target}")
    typer.echo(f"  Bucket: {config.bucket_name}")
    typer.echo(f"  CloudFront: {config.distribution_id}")
    if config.domain:
        typer.echo(f"  Domain: {config.domain}")

    check_deploy_prerequisites()
    validate_s3_bucket(config.bucket_name)
    validate_cloudfront_distribution(config.distribution_id)

    npm_install_if_needed(FRONTEND_DIR)
    npm_build(FRONTEND_DIR, config)
    upload_dist_to_s3(FRONTEND_DIR / "dist", config.bucket_name)
    invalidate_cloudfront(config.distribution_id, ["/*"])

    typer.echo(f"Frontend deployment completed (target={target}).")
    if config.domain:
        typer.echo(f"Available at: {config.domain}")
    if target == "clinic":
        typer.echo(
            "Reminder: clinic frontend is IP-restricted (WAF). "
            "Verify clinic-office-ips and clinic-dev-ips.",
        )
    return 0
