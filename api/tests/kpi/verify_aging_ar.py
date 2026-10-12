"""
Smoke-test Aging of A/R API surface vs mart_ar_summary (in-scope endpoints only).

In scope:
    GET /ar/snapshot-dates
    GET /ar/kpi-summary  (total_ar_outstanding)
    GET /ar/aging-summary (bucket amounts)

Out of scope: priority-queue, risk-distribution, aging-trends, DSO fields.

Prerequisites:
    - API running against warehouse with mart_ar_summary built for today
    - API key: .ssh/dbt-dental-clinic-api-key.pem or API_KEY env

Usage:
    python api/tests/kpi/verify_aging_ar.py
    python api/tests/kpi/verify_aging_ar.py --expected-total 320427.04
"""

from __future__ import annotations

import argparse
import os
import sys
from decimal import Decimal
from pathlib import Path

import requests

BASE_URL = os.environ.get("API_BASE_URL", "http://localhost:8000")

TOLERANCE_ABS = Decimal("50.00")
TOLERANCE_PCT = Decimal("0.005")


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


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--expected-total",
        type=Decimal,
        default=None,
        help="Optional expected total_ar_outstanding (mart SUM). "
        "If omitted, only checks endpoint shape and snapshot consistency.",
    )
    parser.add_argument(
        "--base-url",
        default=BASE_URL,
        help="API base URL (default API_BASE_URL or http://localhost:8000)",
    )
    args = parser.parse_args()
    base = args.base_url.rstrip("/")

    api_key = get_api_key()
    if not api_key:
        print("FAIL: no API key (set API_KEY or place .ssh/dbt-dental-clinic-api-key.pem)")
        return 1

    session = requests.Session()
    session.headers["X-API-Key"] = api_key

    ok = True

    # snapshot-dates
    r = session.get(f"{base}/ar/snapshot-dates", timeout=30)
    if r.status_code != 200:
        print(f"FAIL /ar/snapshot-dates HTTP {r.status_code} {r.text[:200]}")
        return 1
    dates = r.json()
    if not dates:
        print("FAIL /ar/snapshot-dates: empty list")
        return 1
    latest = dates[0].get("snapshot_date") if isinstance(dates[0], dict) else dates[0]
    print(f"PASS /ar/snapshot-dates latest={latest} count={len(dates)}")

    # kpi-summary
    r = session.get(f"{base}/ar/kpi-summary", timeout=30)
    if r.status_code != 200:
        print(f"FAIL /ar/kpi-summary HTTP {r.status_code} {r.text[:200]}")
        return 1
    kpi = r.json()
    total = Decimal(str(kpi.get("total_ar_outstanding", 0)))
    print(
        f"INFO /ar/kpi-summary total_ar_outstanding={total} "
        f"current={kpi.get('current_amount')} over_90={kpi.get('over_90_amount')}"
    )
    if total <= 0:
        print("FAIL /ar/kpi-summary: total_ar_outstanding <= 0")
        ok = False
    elif args.expected_total is not None:
        if within_tolerance(total, args.expected_total):
            print(f"PASS /ar/kpi-summary vs expected {args.expected_total}")
        else:
            print(
                f"FAIL /ar/kpi-summary: {total} vs expected {args.expected_total} "
                f"(tol ±{TOLERANCE_ABS} or {TOLERANCE_PCT})"
            )
            ok = False
    else:
        print("PASS /ar/kpi-summary shape (no --expected-total)")

    # aging-summary
    r = session.get(f"{base}/ar/aging-summary", timeout=30)
    if r.status_code != 200:
        print(f"FAIL /ar/aging-summary HTTP {r.status_code} {r.text[:200]}")
        return 1
    aging = r.json()
    if not isinstance(aging, list) or len(aging) < 4:
        print(f"FAIL /ar/aging-summary: expected 4 buckets, got {aging!r}")
        ok = False
    else:
        bucket_sum = sum(Decimal(str(b.get("amount", 0))) for b in aging)
        labels = [b.get("aging_bucket") for b in aging]
        print(f"INFO /ar/aging-summary buckets={labels} sum={bucket_sum}")
        if within_tolerance(bucket_sum, total):
            print("PASS /ar/aging-summary bucket sum ~= total_ar_outstanding")
        else:
            print(f"FAIL /ar/aging-summary bucket sum {bucket_sum} vs total {total}")
            ok = False

    print("API smoke summary (Aging of A/R in-scope) @", base)
    print("PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
