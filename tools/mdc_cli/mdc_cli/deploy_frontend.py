"""Build and deploy frontend to S3/CloudFront (Phase 5.3)."""

from __future__ import annotations

import json
import os
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


# Deploy target → npm workspace package (apps/portfolio | apps/clinic).
FRONTEND_APP_BY_TARGET: dict[str, str] = {
    "demo": "portfolio",
    "clinic": "clinic",
}
FRONTEND_WORKSPACE_BY_TARGET: dict[str, str] = {
    "demo": "@mdc/portfolio",
    "clinic": "@mdc/clinic",
}


def frontend_app_dir(frontend_dir: Path, target: str) -> Path:
    app_name = FRONTEND_APP_BY_TARGET.get(target)
    if not app_name:
        raise ValueError(f"Unknown frontend deploy target: {target}")
    return frontend_dir / "apps" / app_name


def validate_workspace_app(frontend_dir: Path, target: str) -> Path:
    """Ensure apps/<name>/package.json exists and matches the expected workspace name."""
    app_dir = frontend_app_dir(frontend_dir, target)
    package_json = app_dir / "package.json"
    if not package_json.is_file():
        typer.echo(f"Workspace package missing: {package_json}", err=True)
        raise typer.Exit(code=1)

    expected_name = FRONTEND_WORKSPACE_BY_TARGET[target]
    try:
        payload = json.loads(package_json.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        typer.echo(f"Invalid package.json at {package_json}: {exc}", err=True)
        raise typer.Exit(code=1) from exc

    actual_name = payload.get("name")
    if actual_name != expected_name:
        typer.echo(
            f"Workspace name mismatch for target={target!r}: "
            f"expected {expected_name!r}, found {actual_name!r} in {package_json}",
            err=True,
        )
        raise typer.Exit(code=1)
    return app_dir


def validate_api_url_for_target(config: FrontendDeployConfig) -> None:
    """Reject obviously cross-wired API hosts before build/upload."""
    api = (config.api_url or "").lower()
    if config.target == "demo":
        if "api-clinic" in api:
            typer.echo(
                f"Deploy preflight failed: demo target must not use clinic API URL "
                f"({config.api_url!r}).",
                err=True,
            )
            raise typer.Exit(code=1)
        if "api.dbtdentalclinic.com" not in api and "localhost" not in api:
            typer.echo(
                f"Deploy preflight warning: unexpected demo API URL ({config.api_url!r}).",
                err=True,
            )
    elif config.target == "clinic":
        if "api-clinic" not in api and "localhost" not in api:
            typer.echo(
                f"Deploy preflight failed: clinic target expected api-clinic host, "
                f"got {config.api_url!r}.",
                err=True,
            )
            raise typer.Exit(code=1)


def npm_typecheck(frontend_dir: Path, workspace: str) -> None:
    typer.echo(f"Type-checking {workspace}...")
    code = _run(["npm", "run", "type-check", "-w", workspace], cwd=frontend_dir)
    if code != 0:
        typer.echo("Frontend type-check failed.", err=True)
        raise typer.Exit(code=code)


def write_last_deploy_record(
    frontend_dir: Path,
    config: FrontendDeployConfig,
    workspace: str,
) -> None:
    """Local (gitignored) hint for `mdc frontend status`."""
    from datetime import datetime, timezone

    record = {
        "target": config.target,
        "workspace": workspace,
        "bucket": config.bucket_name,
        "distribution_id": config.distribution_id,
        "domain": config.domain,
        "api_url": config.api_url,
        "vite_is_demo": config.vite_is_demo,
        "deployed_at": datetime.now(timezone.utc).isoformat(),
    }
    path = frontend_dir / f".last-deploy-{config.target}.json"
    path.write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")
    typer.echo(f"Wrote local deploy record: {path.relative_to(REPO_ROOT)}")


def read_last_deploy_record(frontend_dir: Path, target: str) -> dict[str, object] | None:
    path = frontend_dir / f".last-deploy-{target}.json"
    if not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None
    return payload if isinstance(payload, dict) else None


def npm_build(frontend_dir: Path, config: FrontendDeployConfig) -> Path:
    """Build the workspace app for the deploy target; return its dist directory."""
    workspace = FRONTEND_WORKSPACE_BY_TARGET.get(config.target)
    if not workspace:
        raise ValueError(f"Unknown frontend deploy target: {config.target}")

    validate_workspace_app(frontend_dir, config.target)
    validate_api_url_for_target(config)
    npm_typecheck(frontend_dir, workspace)

    dist_dir = frontend_app_dir(frontend_dir, config.target) / "dist"
    build_env = os.environ.copy()
    build_env["VITE_API_URL"] = config.api_url
    build_env["VITE_API_KEY"] = config.api_key
    build_env["VITE_IS_DEMO"] = "true" if config.vite_is_demo else "false"
    typer.echo(
        f"Building {workspace} (target={config.target}, API={config.api_url})..."
    )
    code = _run(
        ["npm", "run", "build", "-w", workspace],
        cwd=frontend_dir,
        env=build_env,
    )
    if code != 0:
        typer.echo("Frontend build failed.", err=True)
        raise typer.Exit(code=code)

    # Guard against uploading the wrong product bundle.
    expected_marker = "Benjamin Rains" if config.target == "demo" else "MDC & GLIC Analytics"
    forbidden_marker = "MDC & GLIC Analytics" if config.target == "demo" else "Benjamin Rains"
    index_html = dist_dir / "index.html"
    if not index_html.is_file():
        typer.echo(f"Deploy preflight failed: missing {index_html}", err=True)
        raise typer.Exit(code=1)

    html = index_html.read_text(encoding="utf-8", errors="replace")
    if expected_marker not in html:
        typer.echo(
            f"Deploy preflight failed: {dist_dir}/index.html missing "
            f"expected marker for target={config.target!r} ({expected_marker!r}).",
            err=True,
        )
        raise typer.Exit(code=1)
    if forbidden_marker in html:
        typer.echo(
            f"Deploy preflight failed: {dist_dir}/index.html contains "
            f"wrong-product marker ({forbidden_marker!r}) for target={config.target!r}.",
            err=True,
        )
        raise typer.Exit(code=1)

    expected_demo_flag = "true" if config.vite_is_demo else "false"
    if config.vite_is_demo != (config.target == "demo"):
        typer.echo(
            f"Deploy preflight failed: vite_is_demo={config.vite_is_demo} "
            f"incompatible with target={config.target!r}.",
            err=True,
        )
        raise typer.Exit(code=1)
    typer.echo(
        f"Preflight OK: workspace={workspace}, api={config.api_url}, "
        f"VITE_IS_DEMO={expected_demo_flag}"
    )
    return dist_dir



# React Router paths that must resolve when entered directly in the browser (S3 has no
# server-side rewrite; CloudFront custom errors are preferred — see configure_clinic_cloudfront_spa.ps1).
# Keep target-specific lists so demo does not get clinic deep-link keys and vice versa.
ANALYTICS_SPA_ROUTE_KEYS: tuple[str, ...] = (
    "dashboard",
    "revenue",
    "providers",
    "patients",
    "appointments",
    "ar-aging",
    "treatment-acceptance",
    "hygiene-retention",
    "referral-sources",
    "kpi-definitions",
)

PORTFOLIO_SPA_ROUTE_KEYS: tuple[str, ...] = (
    "agent-profile",
    *ANALYTICS_SPA_ROUTE_KEYS,
    "environment-manager",
    "schema-discovery",
)

CLINIC_SPA_ROUTE_KEYS: tuple[str, ...] = (
    "login",
    "home/practice-manager",
    "home/owner",
    "home/front-desk",
    "home/insurance",
    *ANALYTICS_SPA_ROUTE_KEYS,
)


def spa_route_keys_for_target(target: str) -> tuple[str, ...]:
    if target == "clinic":
        return CLINIC_SPA_ROUTE_KEYS
    if target == "demo":
        return PORTFOLIO_SPA_ROUTE_KEYS
    raise ValueError(f"Unknown frontend deploy target: {target}")


def upload_spa_route_fallbacks(
    dist_dir: Path,
    bucket_name: str,
    route_keys: tuple[str, ...],
) -> None:
    """Copy index.html to each client route key so /login etc. work without CloudFront error pages."""
    index_html = dist_dir / "index.html"
    if not index_html.is_file():
        typer.echo("index.html missing; skipping SPA route fallbacks.", err=True)
        return

    typer.echo(
        f"Uploading SPA route fallbacks ({len(route_keys)} routes; index.html copies for deep links)..."
    )
    cache = "no-cache, no-store, must-revalidate"
    for route in route_keys:
        for key in (route, f"{route}/index.html"):
            dest = f"s3://{bucket_name}/{key}"
            code = _run(
                [
                    "aws",
                    "s3",
                    "cp",
                    str(index_html),
                    dest,
                    "--content-type",
                    "text/html",
                    "--cache-control",
                    cache,
                ],
                cwd=REPO_ROOT,
            )
            if code != 0:
                typer.echo(f"Failed to upload SPA fallback: {dest}", err=True)
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
    dist_dir = npm_build(FRONTEND_DIR, config)
    upload_dist_to_s3(dist_dir, config.bucket_name)
    upload_spa_route_fallbacks(
        dist_dir,
        config.bucket_name,
        spa_route_keys_for_target(target),
    )
    invalidate_cloudfront(config.distribution_id, ["/*"])
    write_last_deploy_record(
        FRONTEND_DIR,
        config,
        FRONTEND_WORKSPACE_BY_TARGET[target],
    )

    typer.echo(f"Frontend deployment completed (target={target}).")
    if config.domain:
        typer.echo(f"Available at: {config.domain}")
    typer.echo(
        f"SPA deep links: uploaded route fallbacks for target={target} "
        f"({len(spa_route_keys_for_target(target))} routes)."
    )
    if target == "clinic":
        typer.echo(
            "Reminder: clinic frontend is IP-restricted (WAF). "
            "Verify clinic-office-ips and clinic-dev-ips.",
        )
        typer.echo(
            "For cleaner spa setup, also run scripts/deployment/configure_clinic_cloudfront_spa.ps1 "
            "(CloudFront 403/404 -> index.html).",
        )
    return 0
