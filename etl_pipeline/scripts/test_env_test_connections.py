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

import sys

from sqlalchemy import text

from etl_pipeline.core.connections import ConnectionFactory
from etl_pipeline.scripts.script_env import load_script_settings


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
    try:
        settings = load_script_settings("test")
    except ValueError as exc:
        print(f"ERROR: {exc}")
        sys.exit(1)

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
