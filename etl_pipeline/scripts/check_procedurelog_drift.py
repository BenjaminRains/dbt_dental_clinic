#!/usr/bin/env python3
"""
Check procedurelog replica row drift (ETL-FND-001).

Compares complete-production totals (ProcStatus=2, DateComplete in lookback window)
between OpenDental MySQL and analytics raw.procedurelog.

Usage:
    mdc etl invoke --env local -- check-procedurelog-drift --warn-only
    mdc etl invoke --env local -- python scripts/check_procedurelog_drift.py --warn-only
    cd etl_pipeline && python scripts/check_procedurelog_drift.py --stage local

Exit codes:
    0 — in sync
    1 — drift detected (or connection error when not --warn-only)
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from etl_pipeline.config import DatabaseType
from etl_pipeline.config.script_env import add_stage_argument, load_script_settings
from etl_pipeline.core.connections import ConnectionFactory
from etl_pipeline.monitoring.procedurelog_drift import (
    PROCEDURELOG_LOOKBACK_DAYS,
    check_procedurelog_drift,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Detect procedurelog replica row drift (MySQL source vs raw).",
    )
    add_stage_argument(parser)
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=PROCEDURELOG_LOOKBACK_DAYS,
        help=f"Rolling window in days (default: {PROCEDURELOG_LOOKBACK_DAYS})",
    )
    parser.add_argument(
        "--warn-only",
        action="store_true",
        help="Print drift but exit 0 (for initial rollout)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    try:
        settings = load_script_settings(args.stage)
    except ValueError as exc:
        print(f"ERROR: {exc}")
        sys.exit(1)

    if args.lookback_days < 1:
        print("ERROR: --lookback-days must be at least 1")
        sys.exit(1)

    source_engine = None
    analytics_engine = None
    try:
        source_engine = ConnectionFactory.get_source_connection(settings)
        analytics_engine = ConnectionFactory.get_analytics_raw_connection(settings)
        source_db = settings.get_database_config(DatabaseType.SOURCE).get("database", "opendental")

        result = check_procedurelog_drift(
            source_engine,
            analytics_engine,
            source_database=source_db,
            lookback_days=args.lookback_days,
        )

        print("=" * 80)
        print("PROCEDURELOG REPLICA DRIFT CHECK (ETL-FND-001)")
        print("=" * 80)
        print(f"Lookback window : {result.lookback_days} days")
        print(f"MySQL source    : {result.source.complete_rows} rows / ${result.source.complete_fees}")
        print(f"raw.procedurelog: {result.raw.complete_rows} rows / ${result.raw.complete_fees}")
        print(f"Delta           : {result.row_delta} rows / ${result.fee_delta} fees")
        print()
        print(result.message)

        if result.drift_detected:
            print()
            print("DRIFT DETECTED")
            if not args.warn_only:
                sys.exit(1)
        else:
            print()
            print("IN SYNC")
    except Exception as exc:
        print(f"ERROR: {exc}")
        if not args.warn_only:
            sys.exit(1)
    finally:
        if source_engine is not None:
            source_engine.dispose()
        if analytics_engine is not None:
            analytics_engine.dispose()


if __name__ == "__main__":
    main()
