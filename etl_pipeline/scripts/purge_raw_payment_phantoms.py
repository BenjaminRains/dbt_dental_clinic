#!/usr/bin/env python3
"""Remove payment rows from raw when confirmed deleted in OpenDental (phantom cleanup)."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import text

from etl_pipeline.config import DatabaseType
from etl_pipeline.config.script_env import add_stage_argument, load_script_settings
from etl_pipeline.core.connections import ConnectionFactory


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="DELETE from raw.payment only when PayNum absent in OpenDental.",
    )
    add_stage_argument(parser)
    parser.add_argument(
        "pay_nums",
        nargs="+",
        type=int,
        help="PayNum values to purge from raw if missing in source",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Verify source absence only; do not delete from raw",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    settings = load_script_settings(args.stage)
    source = ConnectionFactory.get_source_connection(settings)
    raw = ConnectionFactory.get_analytics_raw_connection(settings)
    source_db = settings.get_database_config(DatabaseType.SOURCE).get("database", "opendental")

    still_in_source: list[int] = []
    with source.connect() as conn:
        for pay_num in args.pay_nums:
            row = conn.execute(
                text(f"SELECT PayNum FROM `{source_db}`.payment WHERE PayNum = :n"),
                {"n": pay_num},
            ).fetchone()
            if row:
                still_in_source.append(pay_num)

    if still_in_source:
        print(f"ERROR: PayNum(s) still in OpenDental — will not delete from raw: {still_in_source}")
        sys.exit(1)

    with raw.connect() as conn:
        existing = conn.execute(
            text(
                'SELECT "PayNum", "PayAmt", "PayDate" FROM raw.payment '
                'WHERE "PayNum" = ANY(:nums) ORDER BY "PayNum"'
            ),
            {"nums": args.pay_nums},
        ).fetchall()

    if not existing:
        print("No matching rows in raw.payment — nothing to do.")
        source.dispose()
        raw.dispose()
        return

    print("Phantom rows to remove from raw.payment:")
    for row in existing:
        print(f"  PayNum={row.PayNum} PayAmt={row.PayAmt} PayDate={row.PayDate}")

    if args.dry_run:
        print("Dry run — no delete performed.")
        source.dispose()
        raw.dispose()
        return

    with raw.begin() as conn:
        deleted = conn.execute(
            text(
                'DELETE FROM raw.payment WHERE "PayNum" = ANY(:nums) '
                'RETURNING "PayNum", "PayAmt", "PayDate"'
            ),
            {"nums": args.pay_nums},
        ).fetchall()

    print(f"Deleted {len(deleted)} row(s) from raw.payment.")
    source.dispose()
    raw.dispose()


if __name__ == "__main__":
    main()
