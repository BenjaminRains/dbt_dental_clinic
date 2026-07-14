"""
Smoke-test daily collections KPI API vs all OD-validated golden dates.

Prerequisites:
    - API server running (api-run)
    - Analytics published to the DB the API uses
    - Local API key: .ssh/dbt-dental-clinic-api-key.pem (same as verify_api_local_authentication.py)
      or set API_KEY / CLINIC_API_KEY / LOCAL_API_KEY

Usage (default — all three findings dates):
    python api/tests/kpi/verify_daily_collections.py

Single date (optional):
    python api/tests/kpi/verify_daily_collections.py --date 2025-11-08

Golden dates and expected nets: dbt_dental_models/validation/kpi/daily-payments/findings/
Compare SQL: dbt_dental_models/validation/kpi/daily-payments/compare/compare_daily_collections_api.sql
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

import requests

BASE_URL = os.environ.get("API_BASE_URL", "http://localhost:8000")

TOLERANCE_ABS = Decimal("10.00")
TOLERANCE_PCT = Decimal("0.005")


@dataclass(frozen=True)
class GoldenDate:
    payment_date: str
    expected_net: Decimal
    scenario: str


# All spot-check dates from validation/kpi/daily-payments/findings/ (keep in sync with registry)
GOLDEN_DATES: tuple[GoldenDate, ...] = (
    GoldenDate(
        "2026-06-24",
        Decimal("11197.40"),
        "Weekday; patient + insurance; mart fix + ETL",
    ),
    GoldenDate(
        "2025-10-07",
        Decimal("21747.30"),
        "Weekday; mixed PayTypes + insurance",
    ),
    GoldenDate(
        "2025-11-08",
        Decimal("3791.65"),
        "Saturday; patient-only (Credit Card)",
    ),
)

GOLDEN_BY_DATE = {g.payment_date: g for g in GOLDEN_DATES}


def get_api_key() -> str | None:
    """Match verify_api_local_authentication.py key resolution."""
    for env_name in ("API_KEY", "X_API_KEY", "LOCAL_API_KEY", "CLINIC_API_KEY"):
        value = (os.environ.get(env_name) or "").strip()
        if value:
            return value

    project_root = Path(__file__).resolve().parents[3]
    pem_file = project_root / ".ssh" / "dbt-dental-clinic-api-key.pem"
    if pem_file.is_file():
        try:
            lines = pem_file.read_text(encoding="utf-8").splitlines()
            key_lines = [
                line
                for line in lines
                if line and not line.startswith("-----BEGIN") and not line.startswith("-----END")
            ]
            if key_lines:
                return "".join(key_lines).strip()
        except OSError as exc:
            print(f"Could not read {pem_file}: {exc}")

    return None


def within_tolerance(actual: Decimal, expected: Decimal) -> bool:
    if expected == 0 and actual == 0:
        return True
    diff = abs(actual - expected)
    if diff <= TOLERANCE_ABS:
        return True
    if expected != 0 and diff / abs(expected) < TOLERANCE_PCT:
        return True
    return False


def fetch_daily_collections(payment_date: str, api_key: str) -> dict | None:
    url = f"{BASE_URL}/kpi/daily-collections"
    headers = {"X-API-Key": api_key}
    try:
        response = requests.get(
            url,
            params={"payment_date": payment_date},
            headers=headers,
            timeout=15,
        )
        response.raise_for_status()
        return response.json()
    except requests.RequestException as exc:
        print(f"  FAIL request: {exc}")
        return None


@dataclass
class VerifyResult:
    golden: GoldenDate
    passed: bool
    actual_net: Decimal | None
    has_data: bool
    message: str


def verify_golden(golden: GoldenDate, api_key: str, *, verbose: bool = True) -> VerifyResult:
    if verbose:
        print(f"\n--- {golden.payment_date} (expected net ${golden.expected_net}) ---")
        print(f"    {golden.scenario}")

    data = fetch_daily_collections(golden.payment_date, api_key)
    if data is None:
        return VerifyResult(golden, False, None, False, "request failed")

    if verbose:
        print(json.dumps(data, indent=2))

    if not data.get("has_data"):
        return VerifyResult(
            golden, False, None, False, "has_data=false — no mart row in API database"
        )

    actual = Decimal(str(data["net_collections_amount"]))
    ok = within_tolerance(actual, golden.expected_net)
    msg = f"net_collections_amount={actual} (expected {golden.expected_net})"
    if verbose:
        print(f"  {'PASS' if ok else 'FAIL'} {msg}")
    return VerifyResult(golden, ok, actual, True, msg)


def print_summary(results: list[VerifyResult]) -> None:
    print("\n" + "=" * 72)
    print("API smoke summary (GET /kpi/daily-collections)")
    print("=" * 72)
    print(f"{'Date':<12} {'Expected':>12} {'Actual':>12} {'Status':<6} Scenario")
    print("-" * 72)
    for r in results:
        actual_str = f"{r.actual_net:.2f}" if r.actual_net is not None else "—"
        status = "PASS" if r.passed else "FAIL"
        print(
            f"{r.golden.payment_date:<12} "
            f"{r.golden.expected_net:>12.2f} "
            f"{actual_str:>12} "
            f"{status:<6} "
            f"{r.golden.scenario}"
        )
    print("-" * 72)
    passed = sum(1 for r in results if r.passed)
    print(f"{passed}/{len(results)} dates PASS")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--date",
        action="append",
        dest="dates",
        metavar="YYYY-MM-DD",
        help="Restrict to one or more dates. Default: all three golden dates from findings.",
    )
    args = parser.parse_args()

    api_key = get_api_key()
    if not api_key:
        print("Local API key not found.")
        print("  Use one of:")
        print("    .ssh/dbt-dental-clinic-api-key.pem  (project root, same as api-run local dev)")
        print("    $env:API_KEY = '...'   # PowerShell")
        print("  See api/tests/verify_api_local_authentication.py")
        return 1

    print(f"Using API key: {'(set)' if api_key else '(missing)'}")
    print(f"Base URL: {BASE_URL}")

    if args.dates:
        selected: list[GoldenDate] = []
        for d in args.dates:
            if d not in GOLDEN_BY_DATE:
                print(f"Unknown date {d}. Known: {', '.join(GOLDEN_BY_DATE)}")
                return 1
            selected.append(GOLDEN_BY_DATE[d])
    else:
        selected = list(GOLDEN_DATES)
        print(f"\nVerifying all {len(selected)} golden dates from daily-payments/findings/")

    try:
        health = requests.get(f"{BASE_URL}/health", timeout=5)
        if health.status_code != 200:
            print(f"Warning: /health returned {health.status_code}")
    except requests.RequestException:
        print(f"Cannot reach {BASE_URL} — start the API with api-run")
        return 1

    results = [verify_golden(g, api_key) for g in selected]
    print_summary(results)

    if all(r.passed for r in results):
        print("\nAll API checks PASS.")
        return 0
    print("\nOne or more dates FAILED.")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
