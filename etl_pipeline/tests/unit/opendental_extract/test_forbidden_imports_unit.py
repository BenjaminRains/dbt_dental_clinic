"""Guard: opendental_extract must not import MDC Settings / loaders / Airflow."""

import re
from pathlib import Path

EXTRACT_ROOT = (
    Path(__file__).resolve().parents[3] / "etl_pipeline" / "opendental_extract"
)

FORBIDDEN_PATTERNS = (
    re.compile(r"\bget_settings\b"),
    re.compile(r"from\s+etl_pipeline\.config\.settings\b"),
    re.compile(r"from\s+etl_pipeline\.config\s+import\b"),
    re.compile(r"import\s+etl_pipeline\.config\.settings\b"),
    re.compile(r"\bfrom\s+etl_pipeline\.loaders\b"),
    re.compile(r"\bimport\s+.*PostgresLoader\b"),
    re.compile(r"\bfrom\s+airflow\b"),
    re.compile(r"\bimport\s+airflow\b"),
)


def test_extract_package_has_no_forbidden_imports():
    assert EXTRACT_ROOT.is_dir(), f"missing package at {EXTRACT_ROOT}"
    offenders = []
    for path in EXTRACT_ROOT.rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        for pattern in FORBIDDEN_PATTERNS:
            if pattern.search(text):
                offenders.append(f"{path.name}: {pattern.pattern}")
        if "conn.execute" in text and (
            "etl_copy_status" in text or "etl_load_status" in text
        ):
            offenders.append(f"{path.name}: tracking-table write")
    assert not offenders, "Forbidden coupling in opendental_extract:\n" + "\n".join(
        offenders
    )
