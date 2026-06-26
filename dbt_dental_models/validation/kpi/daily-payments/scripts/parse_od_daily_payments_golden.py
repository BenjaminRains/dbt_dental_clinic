#!/usr/bin/env python3
"""
Parse OpenDental Daily Payments CSV export into section snapshot YAML.

Golden CSV naming: od_daily_payments_{mm}{dd}{yyyy}_{mm}{dd}{yyyy}.csv
  Single day: od_daily_payments_06242026_06242026.csv
  Date range: od_daily_payments_07012025_06122025.csv

Reads the full PHI golden CSV on the clinic secure site. The snapshot contains
section subtotals and row counts only (for compare SQL); the golden CSV remains
the source of truth for row-level validation.

Usage (from this KPI folder):
  cd dbt_dental_models/validation/kpi/daily-payments
  python scripts/parse_od_daily_payments_golden.py golden/od_daily_payments_10072025_10072025.csv

See scripts/README.md in this folder for full documentation.

Writes golden/snapshots/od_daily_payments_10072025_10072025.snapshot.yml by default
(same stem as the golden CSV).
"""

from __future__ import annotations

import argparse
import csv
import io
import sys
from pathlib import Path


def parse_amount(raw: str) -> float:
    cleaned = raw.strip().strip('"').replace(",", "")
    if not cleaned:
        return 0.0
    return float(cleaned)


def parse_csv_row(row: str) -> list[str]:
    return next(csv.reader(io.StringIO(row)))


def is_total_row(fields: list[str]) -> bool:
    """Section subtotal row: leading columns empty, amount in last non-empty field."""
    if not fields:
        return False
    non_empty = [f for f in fields if f and f.strip()]
    if len(non_empty) != 1:
        return False
    first_nonempty_idx = next(i for i, f in enumerate(fields) if f and f.strip())
    return first_nonempty_idx >= 3


def parse_golden_csv(path: Path) -> dict:
    lines = path.read_text(encoding="utf-8-sig").splitlines()
    sections: list[dict] = []
    i = 0

    while i < len(lines):
        line = lines[i].strip()
        if not line:
            i += 1
            continue

        if line.startswith("Date,"):
            i += 1
            continue

        section_name = line
        i += 1
        if i >= len(lines) or not lines[i].startswith("Date,"):
            continue

        header = lines[i]
        has_carrier = "Carrier" in header
        i += 1
        detail_rows = 0
        section_total: float | None = None

        while i < len(lines):
            row = lines[i].strip()
            if not row:
                break
            if not row.startswith(",") and row.startswith("Date,"):
                break

            fields = parse_csv_row(row)
            if is_total_row(fields):
                section_total = parse_amount(fields[-1])
            elif fields and fields[0].strip():
                detail_rows += 1
            i += 1

        sections.append(
            {
                "od_section": section_name,
                "amount": round(section_total or 0.0, 2),
                "detail_row_count": detail_rows,
                "grain": "carrier_split" if has_carrier else "provider_split",
            }
        )

    patient_sections = [
        s for s in sections if s["grain"] == "provider_split" and s["od_section"] != "Income Transfer"
    ]
    insurance_sections = [s for s in sections if s["grain"] == "carrier_split"]
    income_transfer = next((s for s in sections if s["od_section"] == "Income Transfer"), None)

    od_patient_total = round(sum(s["amount"] for s in patient_sections), 2)
    od_insurance_total = round(sum(s["amount"] for s in insurance_sections), 2)
    od_net_collections = round(
        sum(s["amount"] for s in sections if s["od_section"] != "Income Transfer"),
        2,
    )

    return {
        "source_file": path.name,
        "od_sections": sections,
        "od_totals": {
            "patient_sections_amount": od_patient_total,
            "insurance_sections_amount": od_insurance_total,
            "income_transfer_amount": income_transfer["amount"] if income_transfer else 0.0,
            "income_transfer_detail_rows": income_transfer["detail_row_count"] if income_transfer else 0,
            "net_collections": od_net_collections,
        },
    }


def to_yaml(data: dict) -> str:
    lines = [
        f"# Section snapshot from {data['source_file']} (golden CSV retains PHI on clinic site)",
        f"source_file: {data['source_file']}",
        "od_totals:",
    ]
    for key, value in data["od_totals"].items():
        lines.append(f"  {key}: {value}")
    lines.append("od_sections:")
    for section in data["od_sections"]:
        lines.append(f"  - od_section: {section['od_section']!r}")
        lines.append(f"    amount: {section['amount']}")
        lines.append(f"    detail_row_count: {section['detail_row_count']}")
        lines.append(f"    grain: {section['grain']}")
    return "\n".join(lines) + "\n"


def snapshot_path_for_csv(csv_path: Path) -> Path:
    """Mirror golden CSV stem: od_daily_payments_MMDDYYYY_MMDDYYYY.snapshot.yml"""
    return csv_path.parent / "snapshots" / f"{csv_path.stem}.snapshot.yml"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("csv_path", type=Path)
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        help="Write snapshot YAML here (default: golden/snapshots/{csv_stem}.snapshot.yml)",
    )
    args = parser.parse_args()

    if not args.csv_path.exists():
        print(f"File not found: {args.csv_path}", file=sys.stderr)
        return 1

    data = parse_golden_csv(args.csv_path)
    yaml_text = to_yaml(data)
    output = args.output or snapshot_path_for_csv(args.csv_path)

    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(yaml_text, encoding="utf-8")
    print(f"Wrote {output}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
