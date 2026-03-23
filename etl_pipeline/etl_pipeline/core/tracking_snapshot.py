"""
State history tracking: snapshot ETL tracking tables before full refresh.

Captures etl_copy_status (MySQL) and etl_load_status (PostgreSQL) into
archive tables with a snapshot_at timestamp so you retain bookmark state
(last primary value, timestamps, row counts) before they are overwritten
by the full refresh.

Used when the pipeline is run with --full. One snapshot per full run,
taken before any table is processed.
"""

import logging
from datetime import datetime
from typing import Optional

from sqlalchemy import text

from etl_pipeline.config import get_settings
from etl_pipeline.config.settings import PostgresSchema
from etl_pipeline.core.connections import ConnectionFactory
from etl_pipeline.config.logging import get_logger

logger = get_logger(__name__)


def snapshot_tracking_before_full_refresh(settings=None) -> bool:
    """
    Snapshot current ETL tracking tables into archive tables before a full refresh.

    Creates snapshot tables if they do not exist, then copies all rows from
    etl_copy_status (MySQL) and etl_load_status (PostgreSQL) into
    etl_copy_status_snapshot and etl_load_status_snapshot with a shared
    snapshot_at timestamp for this run.

    Call this once at the start of a full pipeline run (--full), before
    processing any table.

    Args:
        settings: Settings instance (uses global if None).

    Returns:
        True if both snapshots succeeded, False otherwise.
    """
    settings = settings or get_settings()
    snapshot_at = datetime.utcnow()

    try:
        replication_engine = ConnectionFactory.get_replication_connection(settings)
        analytics_engine = ConnectionFactory.get_analytics_raw_connection(settings)
        analytics_config = settings.get_analytics_connection_config(PostgresSchema.RAW)
        analytics_schema = analytics_config.get("schema", "raw")
    except Exception as e:
        logger.error(f"Failed to get connections for tracking snapshot: {e}")
        return False

    mysql_ok = _snapshot_mysql_copy_status(replication_engine, snapshot_at)
    pg_ok = _snapshot_postgres_load_status(analytics_engine, analytics_schema, snapshot_at)

    if mysql_ok and pg_ok:
        logger.info(
            f"State history snapshot completed at {snapshot_at.isoformat()} "
            "(etl_copy_status + etl_load_status)"
        )
        return True
    logger.warning(
        f"State history snapshot incomplete: mysql={mysql_ok}, postgres={pg_ok}"
    )
    return False


def _snapshot_mysql_copy_status(engine, snapshot_at: datetime) -> bool:
    """Create etl_copy_status_snapshot table if needed and copy current rows."""
    try:
        with engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS etl_copy_status_snapshot (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    snapshot_at TIMESTAMP(6) NOT NULL,
                    table_name VARCHAR(255) NOT NULL,
                    last_copied TIMESTAMP NULL,
                    last_primary_value VARCHAR(255) NULL,
                    primary_column_name VARCHAR(255) NULL,
                    rows_copied INT DEFAULT 0,
                    copy_status VARCHAR(50) DEFAULT 'pending',
                    INDEX idx_etl_copy_status_snapshot_at (snapshot_at),
                    INDEX idx_etl_copy_status_snapshot_table (table_name)
                )
            """))
            conn.execute(text("""
                INSERT INTO etl_copy_status_snapshot (
                    snapshot_at, table_name, last_copied, last_primary_value,
                    primary_column_name, rows_copied, copy_status
                )
                SELECT
                    :snapshot_at, table_name, last_copied, last_primary_value,
                    primary_column_name, rows_copied, copy_status
                FROM etl_copy_status
            """), {"snapshot_at": snapshot_at})
            conn.commit()
        logger.info("MySQL etl_copy_status snapshotted to etl_copy_status_snapshot")
        return True
    except Exception as e:
        logger.error(f"Failed to snapshot MySQL etl_copy_status: {e}")
        return False


def _snapshot_postgres_load_status(
    engine, analytics_schema: str, snapshot_at: datetime
) -> bool:
    """Create etl_load_status_snapshot table if needed and copy current rows."""
    try:
        with engine.connect() as conn:
            conn.execute(text(f"""
                CREATE TABLE IF NOT EXISTS {analytics_schema}.etl_load_status_snapshot (
                    id SERIAL PRIMARY KEY,
                    snapshot_at TIMESTAMP(6) NOT NULL,
                    table_name VARCHAR(255) NOT NULL,
                    last_primary_value VARCHAR(255) NULL,
                    primary_column_name VARCHAR(255) NULL,
                    rows_loaded INTEGER DEFAULT 0,
                    load_status VARCHAR(50) DEFAULT 'pending',
                    _loaded_at TIMESTAMP NULL
                )
            """))
            conn.execute(text(f"""
                CREATE INDEX IF NOT EXISTS idx_etl_load_status_snapshot_at
                ON {analytics_schema}.etl_load_status_snapshot (snapshot_at)
            """))
            conn.execute(text(f"""
                CREATE INDEX IF NOT EXISTS idx_etl_load_status_snapshot_table
                ON {analytics_schema}.etl_load_status_snapshot (table_name)
            """))
            conn.execute(text(f"""
                INSERT INTO {analytics_schema}.etl_load_status_snapshot (
                    snapshot_at, table_name, last_primary_value, primary_column_name,
                    rows_loaded, load_status, _loaded_at
                )
                SELECT
                    :snapshot_at, table_name, last_primary_value, primary_column_name,
                    rows_loaded, load_status, _loaded_at
                FROM {analytics_schema}.etl_load_status
            """), {"snapshot_at": snapshot_at})
            conn.commit()
        logger.info(
            f"PostgreSQL {analytics_schema}.etl_load_status snapshotted to "
            f"{analytics_schema}.etl_load_status_snapshot"
        )
        return True
    except Exception as e:
        logger.error(f"Failed to snapshot PostgreSQL etl_load_status: {e}")
        return False
