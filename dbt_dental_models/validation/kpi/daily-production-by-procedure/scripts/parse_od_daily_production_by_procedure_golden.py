#!/usr/bin/env python3
"""
Parse OpenDental Daily Production by Procedure CSV into a PHI-free snapshot YAML.

Golden CSV naming: od_daily_production_by_procedure_{mm}{dd}{yyyy}_{mm}{dd}{yyyy}.csv
  Example: od_daily_production_by_procedure_06102026_06102026.csv

Report date comes from the filename stem (authoritative). The CSV `Date:` line is
ignored when it disagrees — OD exports sometimes show the wrong year.

Usage (from this KPI folder):
  cd dbt_dental_models/validation/kpi/daily-production-by-procedure
  python scripts/parse_od_daily_production_by_procedure_golden.py \\
      golden/od_daily_production_by_procedure_06102025_06102025.csv

See scripts/README.md for full documentation.
"""

from __future__ import annotations

import argparse
import csv
import io
import re
import sys
from pathlib import Path


def parse_amount(raw: str) -> float:
    cleaned = raw.strip().strip('"').replace(",", "")
    if not cleaned:
        return 0.0
    return float(cleaned)


def parse_csv_row(row: str) -> list[str]:
    return next(csv.reader(io.StringIO(row)))


def parse_report_date(lines: list[str]) -> str | None:
    for line in lines[:5]:
        match = re.match(r"^Date:\s*(\d{1,2}/\d{1,2}/\d{4})\s*$", line.strip())
        if match:
            month, day, year = match.group(1).split("/")
            return f"{year}-{int(month):02d}-{int(day):02d}"
    return None


def parse_date_from_filename(path: Path) -> str | None:
    match = re.search(
        r"od_daily_production_by_procedure_(\d{2})(\d{2})(\d{4})_",
        path.name,
    )
    if not match:
        return None
    month, day, year = match.groups()
    return f"{year}-{month}-{day}"


def parse_golden_csv(path: Path) -> dict:
    lines = path.read_text(encoding="utf-8-sig").splitlines()
    report_date = parse_report_date(lines)

    rows: list[dict] = []
    report_total: float | None = None

    for line in lines:
        if not line.strip() or line.startswith("Date:"):
            continue
        if line.startswith("Category,"):
            continue

        fields = parse_csv_row(line)
        if len(fields) < 6:
            continue

        category, code, description, qty_raw, avg_raw, total_raw = fields[:6]
        category = category.strip()
        code = code.strip()

        if not code and not category:
            report_total = parse_amount(total_raw)
            continue

        if not code:
            continue

        rows.append(
            {
                "od_category": category,
                "procedure_code": code,
                "description": description.strip(),
                "quantity": int(parse_amount(qty_raw)),
                "average_fee": round(parse_amount(avg_raw), 2),
                "total_fees": round(parse_amount(total_raw), 2),
            }
        )

    if report_total is None:
        report_total = round(sum(r["total_fees"] for r in rows), 2)

    return {
        "source_file": path.name,
        "report_date": report_date,
        "od_totals": {
            "total_fees": round(report_total, 2),
            "procedure_code_count": len(rows),
            "procedure_quantity": sum(r["quantity"] for r in rows),
        },
        "od_rows": rows,
    }


def to_yaml(data: dict) -> str:
    lines = [
        f"# Snapshot from {data['source_file']} (golden CSV may contain descriptions only)",
        f"source_file: {data['source_file']}",
        f"report_date: {data['report_date']!r}",
        "od_totals:",
    ]
    for key, value in data["od_totals"].items():
        lines.append(f"  {key}: {value}")
    lines.append("od_rows:")
    for row in data["od_rows"]:
        lines.append(f"  - od_category: {row['od_category']!r}")
        lines.append(f"    procedure_code: {row['procedure_code']!r}")
        lines.append(f"    quantity: {row['quantity']}")
        lines.append(f"    average_fee: {row['average_fee']}")
        lines.append(f"    total_fees: {row['total_fees']}")
    return "\n".join(lines) + "\n"


def snapshot_path_for_csv(csv_path: Path) -> Path:
    return csv_path.parent / "snapshots" / f"{csv_path.stem}.snapshot.yml"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("csv_path", type=Path)
    parser.add_argument("-o", "--output", type=Path)
    args = parser.parse_args()

    if not args.csv_path.exists():
        print(f"File not found: {args.csv_path}", file=sys.stderr)
        return 1

    data = parse_golden_csv(args.csv_path)
    filename_date = parse_date_from_filename(args.csv_path)
    if filename_date:
        csv_date = data["report_date"]
        if csv_date and csv_date != filename_date:
            print(
                f"Warning: CSV Date line ({csv_date}) != filename ({filename_date}); "
                "using filename.",
                file=sys.stderr,
            )
        data["report_date"] = filename_date
    output = args.output or snapshot_path_for_csv(args.csv_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(to_yaml(data), encoding="utf-8")
    print(f"Wrote {output}")
    print(f"Report date: {data['report_date']}  Total fees: {data['od_totals']['total_fees']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
