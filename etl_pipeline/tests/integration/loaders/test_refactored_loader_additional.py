import os
import pytest
from sqlalchemy import text

from etl_pipeline.config import get_settings, DatabaseType, PostgresSchema as ConfigPostgresSchema
from etl_pipeline.core.connections import ConnectionFactory
from etl_pipeline.core.postgres_schema import PostgresSchema
from etl_pipeline.loaders.postgres_loader import PostgresLoader


@pytest.mark.integration
def test_noop_run_does_not_advance_loaded_at_for_patient():
    os.environ.setdefault("ETL_ENVIRONMENT", "test")
    settings = get_settings()

    replication_engine = ConnectionFactory.get_replication_connection(settings)
    analytics_engine = ConnectionFactory.get_analytics_raw_connection(settings)

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

    with replication_engine.connect() as repl_conn, analytics_engine.connect() as ana_conn:
        replication_db = replication_engine.url.database
        analytics_cfg = settings.get_database_config(DatabaseType.ANALYTICS, ConfigPostgresSchema.RAW)
        analytics_schema = analytics_cfg.get('schema', 'raw')

        # Clean slate target
        ana_conn.execute(text(f'TRUNCATE TABLE {analytics_schema}.patient'))
        ana_conn.execute(text(f"DELETE FROM {analytics_schema}.etl_load_status WHERE table_name = 'patient'"))
        ana_conn.commit()

        # Full load to populate
        full_ok, full_meta = loader.load_table('patient', force_full=True)
        assert full_ok, f"Full load failed: {full_meta}"

        # Capture loaded_at after full
        loaded_at_before = ana_conn.execute(text(
            f"SELECT _loaded_at FROM {analytics_schema}.etl_load_status WHERE table_name='patient' ORDER BY _loaded_at DESC LIMIT 1"
        )).scalar()
        assert loaded_at_before is not None

        # Incremental with no changes should be no-op and not advance timestamp
        inc_ok, inc_meta = loader.load_table('patient', force_full=False)
        assert inc_ok, f"Incremental load failed: {inc_meta}"

        loaded_at_after = ana_conn.execute(text(
            f"SELECT _loaded_at FROM {analytics_schema}.etl_load_status WHERE table_name='patient' ORDER BY _loaded_at DESC LIMIT 1"
        )).scalar()
        assert loaded_at_before == loaded_at_after, "_loaded_at advanced on 0-row incremental load"


@pytest.mark.integration
def test_incremental_handles_invalid_incremental_columns_for_appointment():
    os.environ.setdefault("ETL_ENVIRONMENT", "test")
    settings = get_settings()

    replication_engine = ConnectionFactory.get_replication_connection(settings)
    analytics_engine = ConnectionFactory.get_analytics_raw_connection(settings)

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

    with replication_engine.connect() as repl_conn, analytics_engine.connect() as ana_conn:
        replication_db = replication_engine.url.database
        analytics_cfg = settings.get_database_config(DatabaseType.ANALYTICS, ConfigPostgresSchema.RAW)
        analytics_schema = analytics_cfg.get('schema', 'raw')

        # Start from full load
        ana_conn.execute(text(f'TRUNCATE TABLE {analytics_schema}.appointment'))
        ana_conn.execute(text(f"DELETE FROM {analytics_schema}.etl_load_status WHERE table_name = 'appointment'"))
        ana_conn.commit()

        full_ok, full_meta = loader.load_table('appointment', force_full=True)
        assert full_ok, f"Full load failed: {full_meta}"

        # Incremental should not fail even if config includes a non-existent column (filtered by validation)
        inc_ok, inc_meta = loader.load_table('appointment', force_full=False)
        assert inc_ok, f"Incremental failed due to invalid incremental column handling: {inc_meta}"


@pytest.mark.integration
def test_recompute_last_primary_value_when_stored_as_timestamp_patient():
    os.environ.setdefault("ETL_ENVIRONMENT", "test")
    settings = get_settings()

    replication_engine = ConnectionFactory.get_replication_connection(settings)
    analytics_engine = ConnectionFactory.get_analytics_raw_connection(settings)

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

    with analytics_engine.connect() as ana_conn:
        analytics_cfg = settings.get_database_config(DatabaseType.ANALYTICS, ConfigPostgresSchema.RAW)
        analytics_schema = analytics_cfg.get('schema', 'raw')

        # Ensure full load exists
        ana_conn.execute(text(f'TRUNCATE TABLE {analytics_schema}.patient'))
        ana_conn.execute(text(f"DELETE FROM {analytics_schema}.etl_load_status WHERE table_name = 'patient'"))
        ana_conn.commit()

        ok, meta = loader.load_table('patient', force_full=True)
        assert ok, f"Full load failed: {meta}"

        # Corrupt tracking to simulate timestamp-stored value
        ana_conn.execute(text(
            f"""
            UPDATE {analytics_schema}.etl_load_status
            SET last_primary_value = '2025-01-01 00:00:00', primary_column_name = 'timestamp'
            WHERE table_name = 'patient'
            """
        ))
        ana_conn.commit()

        # Incremental should succeed; loader should recompute integer PK MAX
        inc_ok, inc_meta = loader.load_table('patient', force_full=False)
        assert inc_ok, f"Incremental failed with timestamp-stored last_primary_value: {inc_meta}"


