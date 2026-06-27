#!/usr/bin/env python3
"""
Layer 0 replica aggregate drift checks (Phase 3).

Usage:
    mdc etl invoke --env local -- check-replica-drift --tier A --warn-only
    mdc etl invoke --env local -- check-replica-drift --check L0-PAY-001 --warn-only
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from etl_pipeline.config import DatabaseType
from etl_pipeline.config.script_env import add_stage_argument, load_script_settings
from etl_pipeline.core.connections import ConnectionFactory
from etl_pipeline.monitoring.replica_aggregate_drift import run_replica_drift_checks


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run Layer 0 MySQL vs raw aggregate drift checks.",
    )
    add_stage_argument(parser)
    parser.add_argument(
        "--tier",
        choices=["A", "B", "C"],
        default=None,
        help="Run all enabled checks in this tier",
    )
    parser.add_argument(
        "--check",
        dest="checks",
        action="append",
        default=[],
        help="Run a specific check id (repeatable)",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=None,
        help="Override lookback window for selected checks",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help="Path to replica_drift_checks.yml",
    )
    parser.add_argument(
        "--warn-only",
        action="store_true",
        help="Print drift but exit 0 (for initial rollout)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.tier is None and not args.checks:
        print("ERROR: specify --tier or at least one --check")
        sys.exit(1)
    if args.lookback_days is not None and args.lookback_days < 1:
        print("ERROR: --lookback-days must be at least 1")
        sys.exit(1)

    try:
        settings = load_script_settings(args.stage)
    except ValueError as exc:
        print(f"ERROR: {exc}")
        sys.exit(1)

    source_engine = None
    analytics_engine = None
    try:
        source_engine = ConnectionFactory.get_source_connection(settings)
        analytics_engine = ConnectionFactory.get_analytics_raw_connection(settings)
        source_db = settings.get_database_config(DatabaseType.SOURCE).get("database", "opendental")

        summary = run_replica_drift_checks(
            source_engine,
            analytics_engine,
            source_database=source_db,
            config_path=args.config,
            tier=args.tier,
            check_ids=args.checks or None,
            lookback_days=args.lookback_days,
        )

        print("=" * 80)
        print("LAYER 0 REPLICA DRIFT CHECKS")
        print("=" * 80)
        for result in summary.results:
            print(result.message)
            print(
                f"  rows: source={result.source.row_count} raw={result.raw.row_count} "
                f"delta={result.row_delta}"
            )
            for name, delta in result.amount_deltas.items():
                print(
                    f"  {name}: source=${result.source.amounts[name]} "
                    f"raw=${result.raw.amounts[name]} delta=${delta}"
                )
            print()

        if summary.any_drift_detected:
            print(
                f"DRIFT DETECTED — {summary.checks_failed}/{summary.checks_run} check(s) failed"
            )
            if not args.warn_only:
                sys.exit(1)
        else:
            print(f"IN SYNC — all {summary.checks_run} check(s) passed")
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
