"""
Smoke-test daily production KPI API vs OD-validated golden dates.

Prerequisites:
    - API server running against local warehouse with mart_daily_production_by_procedure built
    - Local API key: .ssh/dbt-dental-clinic-api-key.pem or API_KEY env

Usage:
    python api/tests/kpi/verify_daily_production.py
    python api/tests/kpi/verify_daily_production.py --date 2026-06-10
"""

from __future__ import annotations

import argparse
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
    production_date: str
    expected_fees: Decimal
    expected_qty: int
    expected_codes: int
    scenario: str


GOLDEN_DATES: tuple[GoldenDate, ...] = (
    GoldenDate("2026-06-10", Decimal("15239.00"), 140, 28, "Weekday baseline"),
    GoldenDate("2025-11-18", Decimal("36589.00"), 202, 48, "Heavy weekday"),
    GoldenDate("2026-02-07", Decimal("22344.00"), 79, 25, "Saturday"),
)

GOLDEN_BY_DATE = {g.production_date: g for g in GOLDEN_DATES}


def get_api_key() -> str | None:
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
                if line.strip() and not line.strip().startswith("-----")
            ]
            if key_lines:
                return "".join(key_lines).strip()
        except OSError:
            pass
    return None


def within_tolerance(actual: Decimal, expected: Decimal) -> bool:
    if abs(actual - expected) <= TOLERANCE_ABS:
        return True
    if expected == 0:
        return actual == 0
    return abs(actual - expected) / abs(expected) < TOLERANCE_PCT


def verify_date(session: requests.Session, golden: GoldenDate) -> bool:
    url = f"{BASE_URL}/kpi/daily-production"
    resp = session.get(url, params={"production_date": golden.production_date}, timeout=30)
    if resp.status_code != 200:
        print(f"FAIL {golden.production_date}: HTTP {resp.status_code} {resp.text[:200]}")
        return False

    data = resp.json()
    fees = Decimal(str(data.get("total_fees", 0)))
    qty = int(data.get("procedure_quantity", 0))
    codes = int(data.get("procedure_code_count", 0))
    has_data = bool(data.get("has_data"))

    ok = (
        has_data
        and within_tolerance(fees, golden.expected_fees)
        and qty == golden.expected_qty
        and codes == golden.expected_codes
    )
    status = "PASS" if ok else "FAIL"
    print(
        f"{status} {golden.production_date}: fees={fees} (exp {golden.expected_fees}) "
        f"qty={qty}/{golden.expected_qty} codes={codes}/{golden.expected_codes} "
        f"— {golden.scenario}"
    )
    return ok


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", help="Single YYYY-MM-DD golden date")
    args = parser.parse_args()

    api_key = get_api_key()
    if not api_key:
        print("No API key found (API_KEY or .ssh/dbt-dental-clinic-api-key.pem)", file=sys.stderr)
        return 2

    session = requests.Session()
    session.headers["X-API-Key"] = api_key

    dates = (
        [GOLDEN_BY_DATE[args.date]]
        if args.date
        else list(GOLDEN_DATES)
    )
    if args.date and args.date not in GOLDEN_BY_DATE:
        print(f"Unknown golden date: {args.date}", file=sys.stderr)
        return 2

    print(f"API smoke summary (GET /kpi/daily-production) @ {BASE_URL}")
    results = [verify_date(session, g) for g in dates]
    passed = sum(1 for r in results if r)
    print(f"{passed}/{len(results)} PASS")
    return 0 if all(results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
