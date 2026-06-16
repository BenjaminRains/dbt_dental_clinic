"""Deploy dbt docs to S3/CloudFront under dbt-docs/ prefix (Phase 5.3)."""

from __future__ import annotations

from pathlib import Path

import typer

from mdc_cli.credentials import resolve_demo_hosting_config
from mdc_cli.deploy_frontend import (
    invalidate_cloudfront,
    validate_cloudfront_distribution,
    validate_s3_bucket,
)
from mdc_cli.paths import DBT_DIR, REPO_ROOT
from mdc_cli.process_util import find_executable, run_subprocess
from mdc_cli.run_helper import run_dbt_command

DBT_DOCS_S3_PREFIX = "dbt-docs"


def _run(cmd: list[str], *, cwd: Path) -> int:
    return run_subprocess(cmd, cwd=cwd)


def check_aws_prerequisites() -> None:
    if find_executable("aws") is None:
        typer.echo("Missing required tool: aws", err=True)
        raise typer.Exit(code=127)
    code = _run(["aws", "sts", "get-caller-identity"], cwd=REPO_ROOT)
    if code != 0:
        typer.echo("AWS credentials not configured. Run aws configure.", err=True)
        raise typer.Exit(code=code)


def ensure_dbt_docs_generated(stage: str, *, skip_generate: bool) -> Path:
    target_dir = DBT_DIR / "target"
    index_html = target_dir / "index.html"

    if skip_generate:
        if not index_html.is_file():
            typer.echo(
                "No index.html in dbt_dental_models/target. "
                "Run without --skip-generate or mdc dbt docs --env local.",
                err=True,
            )
            raise typer.Exit(code=1)
        typer.echo(f"Using existing dbt docs: {target_dir}")
        return target_dir

    if index_html.is_file():
        typer.echo(f"Found existing dbt docs: {target_dir}")
        return target_dir

    typer.echo("dbt docs not found. Generating...")
    code = run_dbt_command(stage, ["docs", "generate"])
    if code != 0:
        typer.echo("Failed to generate dbt docs.", err=True)
        raise typer.Exit(code=code)
    if not index_html.is_file():
        typer.echo(
            "dbt docs generate completed but index.html not found in target/.",
            err=True,
        )
        raise typer.Exit(code=1)
    typer.echo("dbt docs generated successfully.")
    return target_dir


def upload_dbt_docs_to_s3(target_dir: Path, bucket_name: str) -> None:
    if not target_dir.is_dir():
        typer.echo(f"dbt target directory not found: {target_dir}", err=True)
        raise typer.Exit(code=1)

    s3_dest = f"s3://{bucket_name}/{DBT_DOCS_S3_PREFIX}/"
    typer.echo(f"Uploading dbt docs to {s3_dest}...")

    typer.echo("Uploading HTML and asset files...")
    code = _run(
        [
            "aws",
            "s3",
            "sync",
            ".",
            s3_dest,
            "--delete",
            "--cache-control",
            "public, max-age=3600",
            "--exclude",
            "*.json",
        ],
        cwd=target_dir,
    )
    if code != 0:
        typer.echo("Failed to upload dbt docs files.", err=True)
        raise typer.Exit(code=code)

    typer.echo("Uploading JSON files...")
    code = _run(
        [
            "aws",
            "s3",
            "sync",
            ".",
            s3_dest,
            "--cache-control",
            "public, max-age=300",
            "--exclude",
            "*",
            "--include",
            "*.json",
        ],
        cwd=target_dir,
    )
    if code != 0:
        typer.echo("Failed to upload dbt docs JSON files.", err=True)
        raise typer.Exit(code=code)


def deploy_dbt_docs(stage: str = "local", *, skip_generate: bool = False) -> int:
    if not DBT_DIR.is_dir():
        typer.echo(f"dbt project directory not found: {DBT_DIR}", err=True)
        return 1

    try:
        config = resolve_demo_hosting_config()
    except ValueError as exc:
        typer.echo(str(exc), err=True)
        return 1

    typer.echo("DEPLOY  dbt-docs")
    typer.echo(f"  Bucket: {config.bucket_name}")
    typer.echo(f"  CloudFront: {config.distribution_id}")
    typer.echo(f"  Target: s3://{config.bucket_name}/{DBT_DOCS_S3_PREFIX}/")
    if config.domain:
        typer.echo(f"  Domain: {config.domain}/{DBT_DOCS_S3_PREFIX}/")

    check_aws_prerequisites()
    validate_s3_bucket(config.bucket_name)
    validate_cloudfront_distribution(config.distribution_id)

    target_dir = ensure_dbt_docs_generated(stage, skip_generate=skip_generate)
    upload_dbt_docs_to_s3(target_dir, config.bucket_name)
    invalidate_cloudfront(config.distribution_id, [f"/{DBT_DOCS_S3_PREFIX}/*"])

    typer.echo("dbt docs deployment completed.")
    if config.domain:
        typer.echo(f"Available at: {config.domain}/{DBT_DOCS_S3_PREFIX}/")
    else:
        typer.echo(
            f"Available at: https://{config.bucket_name}.s3-website.amazonaws.com/"
            f"{DBT_DOCS_S3_PREFIX}/"
        )
    return 0
