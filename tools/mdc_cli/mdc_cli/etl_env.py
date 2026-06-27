"""Compose ETL child env with Phase 6 credential dedup."""

from __future__ import annotations

import logging
from typing import Optional

from mdc_cli import paths
from mdc_cli.postgres_env import (
    POSTGRES_ANALYTICS_PREFIX,
    apply_postgres_ssl_env,
    load_clinic_postgres_env_dict,
    load_local_warehouse_postgres_env_dict,
    read_env_file,
    strip_prefix_keys,
)
from mdc_cli.secrets_manager import overlay_clinic_rds_credentials

logger = logging.getLogger(__name__)

TEST_POSTGRES_PREFIX = "TEST_POSTGRES_ANALYTICS_"


def _read_etl_stage_file(stage: str) -> dict[str, str]:
    return read_env_file(paths.etl_env_file(stage))


def _overlay_analytics_authority(
    stage: str,
    env: dict[str, str],
    *,
    prefer_secrets_manager: bool = True,
) -> dict[str, str]:
    """Replace analytics creds with the Phase 6 authority for local/clinic."""
    if stage == "clinic":
        merged = strip_prefix_keys(env, POSTGRES_ANALYTICS_PREFIX)
        merged.update(load_clinic_postgres_env_dict())
        merged, resolution = overlay_clinic_rds_credentials(
            merged,
            prefer_secrets_manager=prefer_secrets_manager,
        )
        if resolution.warning:
            logger.warning(resolution.warning)
        logger.info("ETL clinic analytics: %s", resolution.source)
        return apply_postgres_ssl_env(merged)

    if stage == "local":
        merged = strip_prefix_keys(env, POSTGRES_ANALYTICS_PREFIX)
        merged.update(load_local_warehouse_postgres_env_dict())
        return apply_postgres_ssl_env(merged)

    return env


def compose_etl_env_dict(
    stage: str,
    profile: Optional[str] = None,
    *,
    tunnel_db: bool = False,
    tunnel_port: Optional[int] = None,
    prefer_secrets_manager: bool = True,
) -> dict[str, str]:
    """
    Build validated flat env for ETL subprocesses.

    Phase 6:
    - clinic/local analytics Postgres from shared authority (not etl_pipeline/.env_*)
    - etl_pipeline/.env_<stage> supplies OpenDental source + MySQL replication only
    - test stage unchanged (TEST_POSTGRES_ANALYTICS_* in etl_pipeline/.env_test)
    """
    ensure_etl_importable()
    from etl_pipeline.config.settings_v2 import (
        etl_settings_to_env_dict,
        load_etl_connection_settings_from_env,
        supplement_env_dict,
    )

    resolved_profile = profile or paths.default_etl_profile(stage)
    file_vals = _read_etl_stage_file(stage)

    if stage in ("clinic", "local"):
        stale = [key for key in file_vals if key.startswith(POSTGRES_ANALYTICS_PREFIX)]
        if stale:
            logger.warning(
                "Ignoring deprecated %s in %s — analytics creds come from Phase 6 authority "
                "(clinic: deployment_credentials.json + Secrets Manager; "
                "local: dbt_dental_models/.env_local).",
                ", ".join(sorted(stale)[:3]) + ("..." if len(stale) > 3 else ""),
                paths.etl_env_file(stage).name,
            )

    merged = dict(file_vals)
    merged["ETL_ENVIRONMENT"] = stage
    merged["ETL_PROFILE"] = resolved_profile

    if stage in ("clinic", "local"):
        merged = _overlay_analytics_authority(
            stage,
            merged,
            prefer_secrets_manager=prefer_secrets_manager,
        )

    settings = load_etl_connection_settings_from_env(
        merged,
        environment=stage,
        profile=resolved_profile,
        config_dir=paths.ETL_DIR,
    )
    env_dict = etl_settings_to_env_dict(settings)
    stage_env_path = paths.etl_env_file(stage)
    path_for_supplement = stage_env_path if stage_env_path.exists() else None
    env_dict = supplement_env_dict(stage, env_dict, path_for_supplement)

    for ssl_key in ("POSTGRES_ANALYTICS_SSLMODE", "PGSSLMODE"):
        if ssl_key in merged:
            env_dict[ssl_key] = merged[ssl_key]

    if tunnel_db and stage == "clinic":
        from mdc_cli.run_helper import apply_tunnel_db_overrides

        env_dict = apply_tunnel_db_overrides(env_dict, local_port=tunnel_port)

    return apply_postgres_ssl_env(env_dict)


def ensure_etl_importable() -> None:
    from mdc_cli.paths import ensure_etl_importable as _ensure

    _ensure()
