"""Source-only MySQL engine factory (no Settings / replication / analytics)."""

from __future__ import annotations

import logging
from typing import Union

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from .config import SourceMySQLConfig

logger = logging.getLogger(__name__)


def create_source_engine(config: Union[SourceMySQLConfig, dict]) -> Engine:
    """
    Create a read-oriented MySQL engine from plain config.

    Does not import MDC Settings. Skips GLOBAL session variables that require
    elevated privileges on clinic Open Dental hosts (same as for_source=True).
    """
    if isinstance(config, dict):
        config = SourceMySQLConfig.from_mapping(config)

    missing = [
        k
        for k, v in {
            "host": config.host,
            "port": config.port,
            "database": config.database,
            "user": config.user,
            "password": config.password,
        }.items()
        if v is None or v == ""
    ]
    if missing:
        raise ValueError(f"Missing required MySQL source connection parameters: {missing}")

    connection_string = (
        f"mysql+pymysql://{config.user}:{config.password}@"
        f"{config.host}:{config.port}/{config.database}"
    )
    engine = create_engine(
        connection_string,
        pool_pre_ping=True,
        pool_size=config.pool_size,
        max_overflow=config.max_overflow,
        pool_timeout=config.pool_timeout,
        pool_recycle=config.pool_recycle,
    )
    _apply_source_session_settings(engine)
    logger.info("Created Open Dental source MySQL engine for database %s", config.database)
    return engine


def _apply_source_session_settings(engine: Engine) -> None:
    """Lightweight session tweaks safe for readonly Open Dental users."""
    try:
        with engine.connect() as conn:
            conn.execute(text("SET SESSION sql_mode = 'TRADITIONAL'"))
            conn.commit()
    except Exception as exc:  # noqa: BLE001 — connection still usable
        logger.debug("Could not apply source session settings: %s", exc)
