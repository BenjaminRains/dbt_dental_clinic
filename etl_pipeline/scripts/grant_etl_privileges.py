#!/usr/bin/env python3
"""
Configure Local MySQL for ETL Bulk Performance

Sets GLOBAL variables on the local MySQL replication server for ETL bulk operation
performance (innodb_flush_log_at_trx_commit, sync_binlog). These variables are
GLOBAL-only on MySQL, so we set them at server level.

Source MySQL (OpenDental): No changes. We do not modify the clinic source server.

Local MySQL (replication): SET GLOBAL to optimized values for bulk loads.
Affects all connections to the local replication server.

Usage:
    set ETL_ENVIRONMENT=local   # or clinic
    python -m etl_pipeline.scripts.grant_etl_privileges

Prerequisites:
    - Run test_root_connections.py first to verify root connectivity
"""

import argparse
import os
import sys

from sqlalchemy import create_engine, text

from etl_pipeline.scripts.script_env import load_script_settings


def _set_global_on_local_mysql() -> tuple[bool, str]:
    """SET GLOBAL innodb_flush_log_at_trx_commit and sync_binlog on local MySQL. Returns (success, message)."""
    host = os.getenv('MYSQL_ROOT_HOST', 'localhost')
    port = int(os.getenv('MYSQL_ROOT_PORT', '3305'))
    database = os.getenv('MYSQL_ROOT_DB', 'opendental_replication')
    root_user = os.getenv('MYSQL_ROOT_USER', 'root')
    root_pass = os.getenv('MYSQL_ROOT_PASSWORD', '')

    url = f"mysql+pymysql://{root_user}:{root_pass or ''}@{host}:{port}/{database}"
    engine = create_engine(url, connect_args={'connect_timeout': 10})
    try:
        with engine.connect() as conn:
            conn.execute(text("SET GLOBAL innodb_flush_log_at_trx_commit = 0"))
            conn.execute(text("SET GLOBAL sync_binlog = 0"))
            conn.commit()
        return True, "SET GLOBAL innodb_flush_log_at_trx_commit = 0, sync_binlog = 0"
    except Exception as e:
        return False, str(e)
    finally:
        engine.dispose()


def _verify_local_mysql() -> tuple[bool, str]:
    """Verify GLOBAL values on local MySQL. Returns (success, message)."""
    host = os.getenv('MYSQL_REPLICATION_HOST', 'localhost')
    port = int(os.getenv('MYSQL_REPLICATION_PORT', '3305'))
    database = os.getenv('MYSQL_REPLICATION_DB', 'opendental_replication')
    user = os.getenv('MYSQL_REPLICATION_USER', 'replication_user')
    password = os.getenv('MYSQL_REPLICATION_PASSWORD', '')

    url = f"mysql+pymysql://{user}:{password or ''}@{host}:{port}/{database}"
    engine = create_engine(url, connect_args={'connect_timeout': 10})
    try:
        with engine.connect() as conn:
            row = conn.execute(text(
                "SELECT @@GLOBAL.innodb_flush_log_at_trx_commit as v1, @@GLOBAL.sync_binlog as v2"
            )).fetchone()
            v1, v2 = row[0], row[1]
        engine.dispose()
        if v1 == 0 and v2 == 0:
            return True, f"@@GLOBAL.innodb_flush_log_at_trx_commit={v1}, @@GLOBAL.sync_binlog={v2}"
        return False, f"Expected 0,0 but got {v1},{v2}"
    except Exception as e:
        return False, str(e)
    finally:
        engine.dispose()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Configure local MySQL for ETL bulk performance.")
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
    print(f"ETL Local MySQL Configuration (ETL_ENVIRONMENT={settings.environment})")
    print("=" * 70)
    print("\nSource MySQL (OpenDental): No changes - skipping.")
    print("Local MySQL (replication): Setting GLOBAL variables for bulk performance.\n")

    ok = True

    # 1. Set GLOBAL on local MySQL
    print("1. SET GLOBAL on local MySQL replication server (as root)")
    success, msg = _set_global_on_local_mysql()
    if success:
        print(f"   OK: {msg}")
    else:
        print(f"   FAILED: {msg}")
        ok = False

    # 2. Verify
    print("\n2. Verify GLOBAL values (as replication_user)")
    success, msg = _verify_local_mysql()
    if success:
        print(f"   OK: {msg}")
    else:
        print(f"   FAILED: {msg}")
        ok = False

    print("\n" + "=" * 70)
    print("PASS" if ok else "FAIL")
    print("=" * 70)
    sys.exit(0 if ok else 1)


if __name__ == '__main__':
    main()
