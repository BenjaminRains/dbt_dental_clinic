#!/usr/bin/env python3
"""Remove rows from raw.* when confirmed deleted in OpenDental (phantom cleanup)."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import text

from etl_pipeline.config import DatabaseType
from etl_pipeline.config.script_env import add_stage_argument, load_script_settings
from etl_pipeline.core.connections import ConnectionFactory

TABLE_PK = {
    "payment": "PayNum",
    "paysplit": "SplitNum",
    "claim": "ClaimNum",
    "claimproc": "ClaimProcNum",
    "adjustment": "AdjNum",
    "procedurelog": "ProcNum",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="DELETE from raw.<table> only when PK absent in OpenDental.",
    )
    add_stage_argument(parser)
    parser.add_argument("table", choices=sorted(TABLE_PK))
    parser.add_argument("ids", nargs="+", type=int, help="Primary key values to purge")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    pk = TABLE_PK[args.table]
    settings = load_script_settings(args.stage)
    source = ConnectionFactory.get_source_connection(settings)
    raw = ConnectionFactory.get_analytics_raw_connection(settings)
    source_db = settings.get_database_config(DatabaseType.SOURCE).get("database", "opendental")

    still_in_source: list[int] = []
    with source.connect() as conn:
        for row_id in args.ids:
            row = conn.execute(
                text(f"SELECT {pk} FROM `{source_db}`.{args.table} WHERE {pk} = :n"),
                {"n": row_id},
            ).fetchone()
            if row:
                still_in_source.append(row_id)

    if still_in_source:
        print(f"ERROR: still in OpenDental — will not delete from raw: {still_in_source}")
        sys.exit(1)

    with raw.connect() as conn:
        existing = conn.execute(
            text(
                f'SELECT "{pk}" FROM raw.{args.table} WHERE "{pk}" = ANY(:nums) ORDER BY "{pk}"'
            ),
            {"nums": args.ids},
        ).fetchall()

    if not existing:
        print(f"No matching rows in raw.{args.table}.")
        source.dispose()
        raw.dispose()
        return

    print(f"Phantom purge raw.{args.table} ({pk}):")
    for row in existing:
        print(f"  {pk}={row[0]}")

    if args.dry_run:
        print("Dry run — no delete.")
        source.dispose()
        raw.dispose()
        return

    with raw.begin() as conn:
        deleted = conn.execute(
            text(f'DELETE FROM raw.{args.table} WHERE "{pk}" = ANY(:nums) RETURNING "{pk}"'),
            {"nums": args.ids},
        ).fetchall()
    print(f"Deleted {len(deleted)} row(s).")
    source.dispose()
    raw.dispose()


if __name__ == "__main__":
    main()
