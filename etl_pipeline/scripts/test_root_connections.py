#!/usr/bin/env python3
"""
Test MySQL Root Connections

Connects to MySQL as root user and runs a simple test query on:
  1. OpenDental source (clinic server) - for granting privileges to readonly_user
  2. Local MySQL replication - for granting privileges to replication_user

Use this script to verify root connectivity before running grant scripts.
Loads environment via create_settings (settings_v2 / FileConfigProvider).

Usage:
    set ETL_ENVIRONMENT=local   # or clinic
    python -m etl_pipeline.scripts.test_root_connections

Environment variables:
  Source (OpenDental):
    OPENDENTAL_SOURCE_HOST, OPENDENTAL_SOURCE_PORT, OPENDENTAL_SOURCE_DB
    OPENDENTAL_SOURCE_ROOT_USER (default: root)
    OPENDENTAL_SOURCE_ROOT_PASSWORD (default: empty - clinic source often has no root password)

  Replication (local MySQL):
    MYSQL_ROOT_HOST, MYSQL_ROOT_PORT, MYSQL_ROOT_DB, MYSQL_ROOT_USER, MYSQL_ROOT_PASSWORD
"""

import argparse
import os
import sys

from sqlalchemy import create_engine, text

from etl_pipeline.scripts.script_env import load_script_settings


def test_mysql_root(host: str, port: int, database: str, user: str, password: str) -> bool:
    """Connect as root and run test query. Returns True if OK."""
    try:
        url = f"mysql+pymysql://{user}:{password or ''}@{host}:{port}/{database}"
        engine = create_engine(url, connect_args={'connect_timeout': 10})
        with engine.connect() as conn:
            result = conn.execute(text(
                "SELECT 1 as test_value, @@version as version, current_user() as user"
            ))
            row = result.fetchone()
            ver = (row[1] or '')[:60]
            print(f"  Query: SELECT 1, @@version, current_user() -> {row[0]}, {ver}..., {row[2]}")
        engine.dispose()
        return True
    except Exception as e:
        print(f"  FAILED: {e}")
        return False


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Test MySQL root connectivity for ETL setup.")
    parser.add_argument(
        "--stage",
        choices=("local", "clinic"),
        default=None,
        help="ETL stage (default: ETL_ENVIRONMENT or local)",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    try:
        settings = load_script_settings(args.stage, default_stage="local")
    except ValueError as exc:
        print(f"ERROR: {exc}")
        sys.exit(1)

    print("=" * 70)
    print(f"MySQL Root Connection Tests (ETL_ENVIRONMENT={settings.environment})")
    print("=" * 70)

    ok_count = 0
    total = 0

    # 1. OpenDental source as root
    host = os.getenv('OPENDENTAL_SOURCE_HOST', '')
    port = int(os.getenv('OPENDENTAL_SOURCE_PORT', '3306'))
    database = os.getenv('OPENDENTAL_SOURCE_DB', 'opendental')
    user = os.getenv('OPENDENTAL_SOURCE_ROOT_USER', 'root')
    password = os.getenv('OPENDENTAL_SOURCE_ROOT_PASSWORD', '')

    print(f"\n1. OpenDental source (root) @ {host}:{port}/{database}")
    if host:
        total += 1
        if test_mysql_root(host, port, database, user, password):
            print("   OK")
            ok_count += 1
    else:
        print("   SKIPPED (OPENDENTAL_SOURCE_HOST not set)")

    # 2. Local MySQL replication as root
    host = os.getenv('MYSQL_ROOT_HOST', 'localhost')
    port = int(os.getenv('MYSQL_ROOT_PORT', '3305'))
    database = os.getenv('MYSQL_ROOT_DB', 'opendental_replication')
    user = os.getenv('MYSQL_ROOT_USER', 'root')
    password = os.getenv('MYSQL_ROOT_PASSWORD', '')

    print(f"\n2. Local MySQL replication (root) @ {host}:{port}/{database}")
    total += 1
    if test_mysql_root(host, port, database, user, password):
        print("   OK")
        ok_count += 1

    # Summary
    print("\n" + "=" * 70)
    print(f"Result: {ok_count}/{total} connections OK")
    print("=" * 70)
    sys.exit(0 if ok_count == total else 1)


if __name__ == '__main__':
    main()
