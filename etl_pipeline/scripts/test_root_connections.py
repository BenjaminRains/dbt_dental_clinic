#!/usr/bin/env python3
"""
Test MySQL Root Connections

Connects to MySQL as root user and runs a simple test query on:
  1. OpenDental source (clinic server) - for granting privileges to readonly_user
  2. Local MySQL replication - for granting privileges to replication_user

Use this script to verify root connectivity before running grant scripts.
Loads environment from .env_local or .env_clinic based on ETL_ENVIRONMENT.

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

import os
import sys
from pathlib import Path
from sqlalchemy import create_engine, text

# Add etl_pipeline to path
script_dir = Path(__file__).resolve().parent
etl_dir = script_dir.parent
sys.path.insert(0, str(etl_dir.parent))

# Load env from .env_local or .env_clinic
env_name = os.getenv('ETL_ENVIRONMENT', 'local')
env_file = etl_dir / f'.env_{env_name}'
if env_file.exists():
    from dotenv import load_dotenv
    # override=False: OS env (e.g. from environment_manager.ps1) wins over the file.
    load_dotenv(env_file, override=False)
    print(f"Loaded {env_file.name}")
else:
    print(f"Warning: {env_file} not found, using current environment")


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


def main():
    print("=" * 70)
    print("MySQL Root Connection Tests")
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
