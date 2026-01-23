import os
import sys
import argparse
import json
from typing import Any, Dict

from sqlalchemy import text
from etl_pipeline.config import get_settings, DatabaseType, PostgresSchema as ConfigPostgresSchema
from etl_pipeline.core.connections import ConnectionFactory
from etl_pipeline.core.postgres_schema import PostgresSchema
from etl_pipeline.config.logging import get_logger

from etl_pipeline.loaders.postgres_loader import PostgresLoader


logger = get_logger(__name__)


def main() -> int:
    parser = argparse.ArgumentParser(description="Smoke test PostgresLoader for a single table")
    parser.add_argument("table", help="Table name to load from replication to analytics")
    parser.add_argument("--force-full", action="store_true", help="Force full load (truncate then load)")
    args = parser.parse_args()

    # Ensure environment is set to test unless explicitly clinic
    etl_env = os.environ.get("ETL_ENVIRONMENT", "test").lower()
    if etl_env not in ("test", "clinic"):
        # Special error message for deprecated "production" environment
        if etl_env == "production":
            print("❌ ETL_ENVIRONMENT='production' has been removed. Use 'clinic' for clinic deployment.")
        else:
            print("ETL_ENVIRONMENT must be 'test' or 'clinic'")
        return 2

    settings = get_settings()

    # Connections
    replication_engine = ConnectionFactory.get_replication_connection(settings)
    analytics_engine = ConnectionFactory.get_analytics_raw_connection(settings)

    # Schema adapter (raw schema)
    schema_adapter = PostgresSchema(
        postgres_schema=ConfigPostgresSchema.RAW,
        settings=settings
    )

    # Instantiate PostgresLoader
    loader = PostgresLoader(
        replication_engine=replication_engine,
        analytics_engine=analytics_engine,
        settings=settings,
        schema_adapter=schema_adapter
    )

    # Debug: show replication row count before load
    try:
        with replication_engine.connect() as conn:
            dbname = replication_engine.url.database
            sql = f"SELECT COUNT(*) FROM `{dbname}`.`{args.table}`"
            count = conn.execute(text(sql)).scalar()
            logger.info(f"[SMOKE DEBUG] Replication row count for {args.table}: {count}")
    except Exception as e:
        logger.warning(f"[SMOKE DEBUG] Failed to read replication count: {e}")

    success, meta = loader.load_table(args.table, force_full=args.force_full)
    output: Dict[str, Any] = {"success": success, "metadata": meta}
    print(json.dumps(output, default=str))
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
