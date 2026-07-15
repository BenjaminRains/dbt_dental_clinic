#!/usr/bin/env python3
"""Read-only Layer 0 drift investigation for claim or paysplit (phantom row detection)."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import text

from etl_pipeline.config import DatabaseType
from etl_pipeline.config.script_env import add_stage_argument, load_script_settings
from etl_pipeline.core.connections import ConnectionFactory

TABLE_CONFIG = {
    "claim": {
        "pk": "ClaimNum",
        "mysql_window": """
            DateService >= DATE_SUB(CURDATE(), INTERVAL :lb DAY)
            AND DateService > '0001-01-01'
            AND (ClaimFee <> 0 OR InsPayAmt <> 0 OR WriteOff <> 0)
        """,
        "raw_window": """
            "DateService"::date >= CURRENT_DATE - make_interval(days => :lb)
            AND "DateService"::date > '0001-01-01'::date
            AND ("ClaimFee" <> 0 OR "InsPayAmt" <> 0 OR "WriteOff" <> 0)
        """,
    },
    "paysplit": {
        "pk": "SplitNum",
        "mysql_window": """
            DatePay >= DATE_SUB(CURDATE(), INTERVAL :lb DAY)
            AND DatePay > '0001-01-01'
            AND SplitAmt <> 0
        """,
        "raw_window": """
            "DatePay"::date >= CURRENT_DATE - make_interval(days => :lb)
            AND "DatePay"::date > '0001-01-01'::date
            AND "SplitAmt" <> 0
        """,
    },
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Investigate MySQL vs raw phantom rows.")
    add_stage_argument(parser)
    parser.add_argument("table", choices=sorted(TABLE_CONFIG))
    parser.add_argument("--lookback-days", type=int, default=30)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    cfg = TABLE_CONFIG[args.table]
    pk = cfg["pk"]
    settings = load_script_settings(args.stage)
    source = ConnectionFactory.get_source_connection(settings)
    raw = ConnectionFactory.get_analytics_raw_connection(settings)
    source_db = settings.get_database_config(DatabaseType.SOURCE).get("database", "opendental")

    with source.connect() as conn:
        mysql_rows = conn.execute(
            text(
                f"SELECT {pk} FROM `{source_db}`.{args.table} WHERE {cfg['mysql_window']}"
            ),
            {"lb": args.lookback_days},
        ).fetchall()
    mysql_nums = {int(r[0]) for r in mysql_rows}

    with raw.connect() as conn:
        raw_rows = conn.execute(
            text(f'SELECT "{pk}" FROM raw.{args.table} WHERE {cfg["raw_window"]}'),
            {"lb": args.lookback_days},
        ).fetchall()
    raw_nums = {int(r[0]) for r in raw_rows}

    phantom = sorted(raw_nums - mysql_nums)
    missing = sorted(mysql_nums - raw_nums)
    print(f"=== {args.table} ({args.lookback_days}-day window) ===")
    print(f"MySQL rows: {len(mysql_nums)}  raw rows: {len(raw_nums)}")
    print(f"phantom in raw (not in MySQL window): {len(phantom)}")
    print(f"missing from raw: {len(missing)}")

    if phantom:
        with raw.connect() as conn:
            rows = conn.execute(
                text(
                    f'SELECT * FROM raw.{args.table} WHERE "{pk}" = ANY(:nums) ORDER BY "{pk}"'
                ),
                {"nums": phantom},
            ).fetchall()
        print("\n--- Phantom rows in raw ---")
        for row in rows:
            print(dict(row._mapping))
        with source.connect() as conn:
            for num in phantom:
                exists = conn.execute(
                    text(f"SELECT {pk} FROM `{source_db}`.{args.table} WHERE {pk} = :n"),
                    {"n": num},
                ).fetchone()
                print(f"  {pk}={num} in OpenDental: {'YES' if exists else 'DELETED/ABSENT'}")

    source.dispose()
    raw.dispose()


if __name__ == "__main__":
    main()
