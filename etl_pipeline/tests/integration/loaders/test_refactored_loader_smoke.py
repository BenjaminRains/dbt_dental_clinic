import os
import pytest
from sqlalchemy import text

from etl_pipeline.config import get_settings, DatabaseType, PostgresSchema as ConfigPostgresSchema
from etl_pipeline.core.connections import ConnectionFactory
from etl_pipeline.core.postgres_schema import PostgresSchema
from etl_pipeline.loaders.postgres_loader import PostgresLoader


@pytest.mark.integration
def test_refactored_loader_patient_appointment_procedurelog():
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
        # Determine DB names/schemas
        replication_db = replication_engine.url.database
        analytics_cfg = settings.get_database_config(DatabaseType.ANALYTICS, ConfigPostgresSchema.RAW)
        analytics_schema = analytics_cfg.get('schema', 'raw')
        # Truncate targets to make assertion deterministic
        for table in tables:
            ana_conn.execute(text(f'TRUNCATE TABLE {analytics_schema}.{table}'))
        ana_conn.commit()

        for table in tables:
            # Replication count should be > 0 from test fixtures
            repl_count = repl_conn.execute(
                text(f"SELECT COUNT(*) FROM `{replication_db}`.`{table}`")
            ).scalar() or 0
            if repl_count == 0:
                pytest.skip(f"Replication has no rows for {table}. "
                           f"Run SimpleMySQLReplicator to copy data from SOURCE to REPLICATION first.")

            success, meta = loader.load_table(table, force_full=True)
            assert success, f"Refactored loader failed for {table}: {meta}"

            # Verify analytics count matches replication
            ana_count = ana_conn.execute(
                text(f"SELECT COUNT(*) FROM {analytics_schema}.{table}")
            ).scalar() or 0
            assert ana_count == repl_count, f"Row count mismatch for {table}: ana={ana_count}, repl={repl_count}"

            # Verify tracking table updated
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


