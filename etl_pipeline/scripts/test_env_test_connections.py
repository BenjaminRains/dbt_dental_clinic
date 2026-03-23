#!/usr/bin/env python3
"""
Test .env_test credentials using connections.py

Uses ConnectionFactory and Settings to verify that the credentials in
etl_pipeline/.env_test can connect to:
  1. Test MySQL replication (TEST_MYSQL_REPLICATION_*)
  2. Test PostgreSQL analytics (TEST_POSTGRES_ANALYTICS_*)
  3. Optionally: Test OpenDental source (TEST_OPENDENTAL_SOURCE_*)

Run this to verify .env_test before integration tests or setup_test_databases.py.
Uses the same code path as the ETL pipeline (connections.py + Settings).

Usage:
    cd etl_pipeline
    python -m etl_pipeline.scripts.test_env_test_connections

    Or from repo root:
    python -m etl_pipeline.scripts.test_env_test_connections
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import text

# Ensure etl_pipeline package is on path
script_dir = Path(__file__).resolve().parent
etl_root = script_dir.parent
sys.path.insert(0, str(etl_root.parent))


def load_test_environment():
    """Load .env_test so Settings uses test env and TEST_* vars."""
    env_path = etl_root / ".env_test"
    if not env_path.exists():
        print(f"ERROR: {env_path} not found")
        sys.exit(1)
    load_dotenv(env_path, override=True)
    os.environ["ETL_ENVIRONMENT"] = "test"
    print(f"Loaded: {env_path}")


def test_engine(engine, label: str) -> bool:
    """Run a simple query on the engine. Returns True if OK."""
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1 AS ok"))
        engine.dispose()
        return True
    except Exception as e:
        print(f"  FAILED: {e}")
        return False


def main():
    load_test_environment()

    # Use the same Settings/ConnectionFactory as the pipeline
    from etl_pipeline.config.settings import reset_settings, get_settings
    from etl_pipeline.core.connections import ConnectionFactory

    reset_settings()
    settings = get_settings()
    if settings.environment != "test":
        print(f"ERROR: Expected environment 'test', got '{settings.environment}'")
        sys.exit(1)

    print("\n" + "=" * 70)
    print("Testing .env_test credentials via connections.py")
    print("=" * 70)

    ok = 0
    total = 0

    # 1. MySQL replication
    print("\n1. MySQL replication (TEST_MYSQL_REPLICATION_*)")
    total += 1
    try:
        engine = ConnectionFactory.get_replication_connection(settings)
        if test_engine(engine, "replication"):
            print("   OK")
            ok += 1
    except Exception as e:
        print(f"   FAILED: {e}")

    # 2. PostgreSQL analytics (raw schema)
    print("\n2. PostgreSQL analytics (TEST_POSTGRES_ANALYTICS_*)")
    total += 1
    try:
        engine = ConnectionFactory.get_analytics_raw_connection(settings)
        if test_engine(engine, "analytics"):
            print("   OK")
            ok += 1
    except Exception as e:
        print(f"   FAILED: {e}")

    # 3. Optional: OpenDental source (may be remote)
    print("\n3. OpenDental source (TEST_OPENDENTAL_SOURCE_*) [optional]")
    try:
        engine = ConnectionFactory.get_source_connection(settings)
        total += 1
        if test_engine(engine, "source"):
            print("   OK")
            ok += 1
    except Exception as e:
        print(f"   FAILED: {e}")
        total += 1

    print("\n" + "=" * 70)
    print(f"Result: {ok}/{total} connections OK")
    print("=" * 70)
    sys.exit(0 if ok == total else 1)


if __name__ == "__main__":
    main()
