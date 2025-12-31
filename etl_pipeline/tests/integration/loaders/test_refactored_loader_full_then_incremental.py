import os
import pytest
import logging
from sqlalchemy import text

from etl_pipeline.config import get_settings, DatabaseType, PostgresSchema as ConfigPostgresSchema
from etl_pipeline.core.connections import ConnectionFactory
from etl_pipeline.core.postgres_schema import PostgresSchema
from etl_pipeline.loaders.postgres_loader import PostgresLoader

logger = logging.getLogger(__name__)


@pytest.mark.integration
def test_refactored_loader_full_then_incremental():
    # Ensure test environment
    os.environ.setdefault("ETL_ENVIRONMENT", "test")

    settings = get_settings()

    # Connections
    replication_engine = ConnectionFactory.get_replication_connection(settings)
    analytics_engine = ConnectionFactory.get_analytics_raw_connection(settings)

    # Schema adapter (raw schema)
    schema_adapter = PostgresSchema(
        postgres_schema=ConfigPostgresSchema.RAW,
        settings=settings,
    )

    loader = PostgresLoader(
        replication_engine=replication_engine,
        analytics_engine=analytics_engine,
        settings=settings,
        schema_adapter=schema_adapter,
    )

    tables = ["patient", "appointment", "procedurelog"]

    with replication_engine.connect() as repl_conn, analytics_engine.connect() as ana_conn:
        # Determine DB/schema names
        replication_db = replication_engine.url.database
        analytics_cfg = settings.get_database_config(DatabaseType.ANALYTICS, ConfigPostgresSchema.RAW)
        analytics_schema = analytics_cfg.get('schema', 'raw')

        # Start from clean targets
        for table in tables:
            ana_conn.execute(text(f'TRUNCATE TABLE {analytics_schema}.{table}'))
        ana_conn.commit()

        # Check if replication has data - if not, skip the test
        # Note: Replication database is set up empty by setup_test_databases.py
        # This test requires data in replication, so we skip if empty to avoid hanging
        base_counts = {}
        for table in tables:
            repl_count = repl_conn.execute(
                text(f"SELECT COUNT(*) FROM `{replication_db}`.`{table}`")
            ).scalar() or 0
            
            if repl_count == 0:
                pytest.skip(f"Replication database is empty for {table}. "
                           f"Run SimpleMySQLReplicator to copy data from SOURCE to REPLICATION first, "
                           f"or use a test that populates replication.")

            success, meta = loader.load_table(table, force_full=True)
            assert success, f"Full load failed for {table}: {meta}"

            ana_count = ana_conn.execute(
                text(f"SELECT COUNT(*) FROM {analytics_schema}.{table}")
            ).scalar() or 0
            assert ana_count == repl_count, f"Full load mismatch for {table}: ana={ana_count}, repl={repl_count}"
            base_counts[table] = ana_count

            # Tracking updated
            track = ana_conn.execute(
                text(
                    f"""
                    SELECT rows_loaded, load_status
                    FROM {analytics_schema}.etl_load_status
                    WHERE table_name = :t
                    ORDER BY _loaded_at DESC
                    LIMIT 1
                    """
                ),
                {"t": table},
            ).fetchone()
            assert track is not None and track[0] >= ana_count and track[1] == "success"

        # Incremental run (no data changes expected; counts remain the same)
        for table in tables:
            success, meta = loader.load_table(table, force_full=False)
            assert success, f"Incremental load failed for {table}: {meta}"

            ana_count_after = ana_conn.execute(
                text(f"SELECT COUNT(*) FROM {analytics_schema}.{table}")
            ).scalar() or 0
            assert ana_count_after == base_counts[table], \
                f"Incremental changed counts for {table}: before={base_counts[table]}, after={ana_count_after}"


