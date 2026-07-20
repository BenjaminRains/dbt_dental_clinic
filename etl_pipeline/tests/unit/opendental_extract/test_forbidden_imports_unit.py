"""Guard: opendental_extract must not import MDC Settings / loaders / Airflow."""

from pathlib import Path

EXTRACT_ROOT = (
    Path(__file__).resolve().parents[3] / "etl_pipeline" / "opendental_extract"
)

FORBIDDEN_IMPORT_NEEDLES = (
    "get_settings",
    "from etl_pipeline.config.settings",
    "from etl_pipeline.config import",
    "PostgresLoader",
    "import airflow",
    "from airflow",
)


def test_extract_package_has_no_forbidden_imports():
    assert EXTRACT_ROOT.is_dir(), f"missing package at {EXTRACT_ROOT}"
    offenders = []
    for path in EXTRACT_ROOT.rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        for needle in FORBIDDEN_IMPORT_NEEDLES:
            if needle in text:
                offenders.append(f"{path.name}: {needle}")
        # Persistence writes must not live in extract modules
        if "conn.execute" in text and (
            "etl_copy_status" in text or "etl_load_status" in text
        ):
            offenders.append(f"{path.name}: tracking-table write")
    assert not offenders, "Forbidden coupling in opendental_extract:\n" + "\n".join(
        offenders
    )
