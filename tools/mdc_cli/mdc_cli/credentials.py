"""Deployment credentials and env-file helpers (Phase 5.2)."""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from mdc_cli.paths import API_DIR, DEPLOYMENT_CREDENTIALS, FRONTEND_DEPLOY_JSON, REPO_ROOT


def _norm(value: Any) -> Optional[str]:
    if value is None:
        return None
    if not isinstance(value, str):
        value = str(value)
    stripped = value.strip()
    return stripped if stripped else None


def read_env_file_value(path: Path, key_name: str) -> Optional[str]:
    """Read KEY=value from a dotenv-style file (OS env wins if already set)."""
    env_key = key_name.upper()
    if os.environ.get(env_key):
        return os.environ[env_key]
    if not path.is_file():
        return None
    pattern = re.compile(
        rf"^{re.escape(key_name)}\s*=\s*(.+)$",
        re.IGNORECASE,
    )
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        match = pattern.match(line)
        if not match:
            continue
        val = match.group(1).strip()
        hash_idx = val.find("#")
        if hash_idx >= 0:
            val = val[:hash_idx].strip()
        if val:
            return val
    return None


def load_deployment_credentials() -> Optional[dict[str, Any]]:
    if not DEPLOYMENT_CREDENTIALS.is_file():
        return None
    try:
        return json.loads(DEPLOYMENT_CREDENTIALS.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None


def _dig(data: Any, *keys: str) -> Any:
    current = data
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


@dataclass(frozen=True)
class DemoHostingConfig:
    """Demo portfolio S3/CloudFront targets (no Vite API key required)."""

    bucket_name: str
    distribution_id: str
    domain: Optional[str]


@dataclass(frozen=True)
class FrontendDeployConfig:
    target: str
    bucket_name: str
    distribution_id: str
    domain: Optional[str]
    api_url: str
    api_key: str
    vite_is_demo: bool


@dataclass(frozen=True)
class FrontendStatusConfig:
    label: str
    bucket_name: Optional[str]
    distribution_id: Optional[str]
    domain: Optional[str]
    api_url: Optional[str]
    api_key_source: Optional[str]


def read_local_api_key_from_pem() -> Optional[str]:
    pem_path = REPO_ROOT / ".ssh" / "dbt-dental-clinic-api-key.pem"
    if not pem_path.is_file():
        return None
    try:
        raw = pem_path.read_text(encoding="utf-8").strip()
        raw = re.sub(r"-----BEGIN[^-]+-----", "", raw)
        raw = re.sub(r"-----END[^-]+-----", "", raw)
        raw = re.sub(r"\s", "", raw)
        return raw or None
    except OSError:
        return None


def _pick_json_key(data: dict[str, Any], *keys: str) -> Optional[str]:
    for key in keys:
        val = _norm(data.get(key))
        if val:
            return val
    return None


def _format_https_domain(value: str) -> str:
    if value.startswith("http://") or value.startswith("https://"):
        return value
    return f"https://{value}"


def _demo_frontend_section(creds: dict[str, Any]) -> Optional[dict[str, Any]]:
    """Live repo uses demo_frontend; template uses frontend."""
    section = creds.get("demo_frontend")
    if isinstance(section, dict):
        return section
    section = creds.get("frontend")
    if isinstance(section, dict):
        return section
    return None


def _load_frontend_deploy_json() -> Optional[dict[str, Any]]:
    if not FRONTEND_DEPLOY_JSON.is_file():
        return None
    try:
        data = json.loads(FRONTEND_DEPLOY_JSON.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None
    return data if isinstance(data, dict) else None


def _merge_demo_hosting_from_sources(
    bucket: Optional[str],
    dist_id: Optional[str],
    domain: Optional[str],
) -> tuple[Optional[str], Optional[str], Optional[str]]:
    deploy_json = _load_frontend_deploy_json()
    if deploy_json:
        if not bucket:
            bucket = _pick_json_key(
                deploy_json,
                "FRONTEND_BUCKET_NAME",
                "BucketName",
                "bucket_name",
            )
        if not dist_id:
            dist_id = _pick_json_key(
                deploy_json,
                "FRONTEND_DIST_ID",
                "DistributionId",
                "distribution_id",
            )
        if not domain:
            domain = _pick_json_key(
                deploy_json,
                "FRONTEND_DOMAIN",
                "Domain",
                "domain",
            )

    creds = load_deployment_credentials()
    section = _demo_frontend_section(creds) if creds else None
    if section:
        if not bucket:
            bucket = _norm(_dig(section, "s3_buckets", "frontend", "bucket_name"))
        if not dist_id:
            dist_id = _norm(_dig(section, "cloudfront", "distribution_id"))
        if not domain:
            fd = _norm(section.get("domain"))
            if fd:
                domain = _format_https_domain(fd)

    return bucket, dist_id, domain


def resolve_demo_hosting_config() -> DemoHostingConfig:
    """S3/CloudFront for demo portfolio site (dbt-docs, static hosting)."""
    bucket, dist_id, domain = _merge_demo_hosting_from_sources(
        _norm(os.environ.get("FRONTEND_BUCKET_NAME")),
        _norm(os.environ.get("FRONTEND_DIST_ID")),
        _norm(os.environ.get("FRONTEND_DOMAIN")),
    )

    if not bucket:
        raise ValueError(
            "FRONTEND_BUCKET_NAME not set. Set env var, .frontend-deploy.json "
            "(BucketName), or deployment_credentials.json "
            "demo_frontend.s3_buckets.frontend.bucket_name."
        )
    if not dist_id:
        raise ValueError(
            "FRONTEND_DIST_ID not set. Set env var, .frontend-deploy.json "
            "(DistributionId), or deployment_credentials.json "
            "demo_frontend.cloudfront.distribution_id."
        )

    return DemoHostingConfig(
        bucket_name=bucket,
        distribution_id=dist_id,
        domain=domain,
    )


def resolve_demo_frontend_config() -> FrontendDeployConfig:
    hosting = resolve_demo_hosting_config()

    demo_api_key = read_env_file_value(API_DIR / ".env_api_demo", "DEMO_API_KEY")
    if not demo_api_key:
        raise ValueError(
            "DEMO_API_KEY not found in api/.env_api_demo or environment variables."
        )

    return FrontendDeployConfig(
        target="demo",
        bucket_name=hosting.bucket_name,
        distribution_id=hosting.distribution_id,
        domain=hosting.domain,
        api_url="https://api.dbtdentalclinic.com",
        api_key=demo_api_key,
        vite_is_demo=True,
    )


def resolve_clinic_frontend_config() -> FrontendDeployConfig:
    bucket = _norm(os.environ.get("CLINIC_FRONTEND_BUCKET_NAME"))
    dist_id = _norm(os.environ.get("CLINIC_FRONTEND_DIST_ID"))
    domain = _norm(os.environ.get("CLINIC_FRONTEND_DOMAIN"))
    api_url = _norm(os.environ.get("CLINIC_API_URL"))
    api_key = _norm(os.environ.get("CLINIC_API_KEY"))

    if not api_key:
        api_key = read_env_file_value(API_DIR / ".env_api_clinic", "CLINIC_API_KEY")

    creds = load_deployment_credentials()
    if creds:
        clinic_front = creds.get("clinic_frontend")
        if clinic_front:
            if not bucket:
                bucket = _norm(
                    _dig(clinic_front, "s3_buckets", "clinic_frontend", "bucket_name")
                )
            if not dist_id:
                dist_id = _norm(_dig(clinic_front, "cloudfront", "distribution_id"))
            if not domain:
                cd = _norm(_dig(clinic_front, "domain"))
                if cd:
                    domain = f"https://{cd}"
        else:
            if not bucket:
                bucket = _norm(
                    _dig(creds, "frontend", "s3_buckets", "clinic_frontend", "bucket_name")
                )
            if not dist_id:
                dist_id = _norm(
                    _dig(creds, "frontend", "cloudfront", "clinic_distribution_id")
                )
            if not domain:
                root_domain = _norm(_dig(creds, "frontend", "domain"))
                if root_domain:
                    domain = f"https://clinic.{root_domain}"

        if not api_url:
            api_url = _norm(_dig(creds, "backend_api", "clinic_api", "api_url"))
            if not api_url:
                root_domain = _norm(_dig(creds, "frontend", "domain"))
                if root_domain:
                    api_url = f"https://api-clinic.{root_domain}"

        if not api_key:
            key_obj = _dig(creds, "backend_api", "clinic_api", "api_key")
            if isinstance(key_obj, dict) and key_obj.get("key"):
                api_key = _norm(str(key_obj["key"]))
            elif key_obj and not isinstance(key_obj, dict):
                raise ValueError(
                    "backend_api.clinic_api.api_key must be an object with a 'key' property."
                )

    if not api_url:
        api_url = "https://api-clinic.dbtdentalclinic.com"

    if not bucket:
        raise ValueError(
            "CLINIC_FRONTEND_BUCKET_NAME not set. Set env var or "
            "clinic_frontend.s3_buckets.clinic_frontend.bucket_name in deployment_credentials.json."
        )
    if not dist_id:
        raise ValueError(
            "CLINIC_FRONTEND_DIST_ID not set. Set env var or "
            "clinic_frontend.cloudfront.distribution_id in deployment_credentials.json."
        )
    if not api_key:
        raise ValueError(
            "CLINIC_API_KEY not set. Use env var, api/.env_api_clinic, or "
            "backend_api.clinic_api.api_key in deployment_credentials.json."
        )

    return FrontendDeployConfig(
        target="clinic",
        bucket_name=bucket,
        distribution_id=dist_id,
        domain=domain,
        api_url=api_url,
        api_key=api_key,
        vite_is_demo=False,
    )


def resolve_frontend_deploy_config(target: str) -> FrontendDeployConfig:
    if target == "demo":
        return resolve_demo_frontend_config()
    if target == "clinic":
        return resolve_clinic_frontend_config()
    raise ValueError(f"Unsupported frontend target: {target}")


def demo_frontend_status() -> FrontendStatusConfig:
    bucket, dist_id, domain = _merge_demo_hosting_from_sources(
        _norm(os.environ.get("FRONTEND_BUCKET_NAME")),
        _norm(os.environ.get("FRONTEND_DIST_ID")),
        _norm(os.environ.get("FRONTEND_DOMAIN")),
    )
    demo_key = read_env_file_value(API_DIR / ".env_api_demo", "DEMO_API_KEY")
    key_source = "api/.env_api_demo" if demo_key else None
    if not key_source and os.environ.get("DEMO_API_KEY"):
        key_source = "environment"
    return FrontendStatusConfig(
        label="demo (dbtdentalclinic.com)",
        bucket_name=bucket,
        distribution_id=dist_id,
        domain=domain,
        api_url="https://api.dbtdentalclinic.com",
        api_key_source=key_source,
    )


def clinic_frontend_status() -> FrontendStatusConfig:
    bucket = _norm(os.environ.get("CLINIC_FRONTEND_BUCKET_NAME"))
    dist_id = _norm(os.environ.get("CLINIC_FRONTEND_DIST_ID"))
    domain = _norm(os.environ.get("CLINIC_FRONTEND_DOMAIN"))
    api_url = _norm(os.environ.get("CLINIC_API_URL"))
    creds = load_deployment_credentials()
    if creds:
        clinic_front = creds.get("clinic_frontend")
        if clinic_front:
            if not bucket:
                bucket = _norm(
                    _dig(clinic_front, "s3_buckets", "clinic_frontend", "bucket_name")
                )
            if not dist_id:
                dist_id = _norm(_dig(clinic_front, "cloudfront", "distribution_id"))
            if not domain:
                cd = _norm(_dig(clinic_front, "domain"))
                if cd:
                    domain = f"https://{cd}"
        else:
            if not bucket:
                bucket = _norm(
                    _dig(creds, "frontend", "s3_buckets", "clinic_frontend", "bucket_name")
                )
            if not dist_id:
                dist_id = _norm(
                    _dig(creds, "frontend", "cloudfront", "clinic_distribution_id")
                )
            if not domain:
                root_domain = _norm(_dig(creds, "frontend", "domain"))
                if root_domain:
                    domain = f"https://clinic.{root_domain}"
        if not api_url:
            api_url = _norm(_dig(creds, "backend_api", "clinic_api", "api_url"))

    if not api_url:
        api_url = "https://api-clinic.dbtdentalclinic.com"

    key_source = None
    if os.environ.get("CLINIC_API_KEY"):
        key_source = "environment"
    elif read_env_file_value(API_DIR / ".env_api_clinic", "CLINIC_API_KEY"):
        key_source = "api/.env_api_clinic"
    elif creds and _dig(creds, "backend_api", "clinic_api", "api_key", "key"):
        key_source = "deployment_credentials.json"

    return FrontendStatusConfig(
        label="clinic (clinic.dbtdentalclinic.com)",
        bucket_name=bucket,
        distribution_id=dist_id,
        domain=domain,
        api_url=api_url,
        api_key_source=key_source,
    )
