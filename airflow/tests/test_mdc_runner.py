"""Unit tests for airflow/dags/lib/mdc_runner helpers."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_DAGS_LIB = Path(__file__).resolve().parents[1] / "dags"
if str(_DAGS_LIB) not in sys.path:
    sys.path.insert(0, str(_DAGS_LIB))

from lib.mdc_runner import (  # noqa: E402
    dbt_registry_connection_failed,
    locked_dbt_packages_present,
    parse_dbt_run_summary,
)


def test_parse_dbt_run_summary_done_line():
    stdout = (
        "some noise\n"
        "Done. PASS=2773 WARN=253 ERROR=4 SKIP=1980 NO-OP=1 TOTAL=5011\n"
    )
    summary = parse_dbt_run_summary(stdout)
    assert summary["pass"] == 2773
    assert summary["warn"] == 253
    assert summary["error"] == 4
    assert summary["skip"] == 1980
    assert summary["total"] == 5011
    assert summary["has_errors"] is True


def test_parse_dbt_run_summary_clean():
    stdout = "Done. PASS=10 WARN=0 ERROR=0 SKIP=0 TOTAL=10\n"
    summary = parse_dbt_run_summary(stdout)
    assert summary["has_errors"] is False
    assert summary["error"] == 0


def test_parse_dbt_run_summary_completed_with():
    stdout = "Completed with 4 errors, 0 partial successes, and 253 warnings:\n"
    summary = parse_dbt_run_summary(stdout)
    assert summary["error"] == 4
    assert summary["warn"] == 253
    assert summary["has_errors"] is True


def test_parse_dbt_run_summary_empty():
    summary = parse_dbt_run_summary("no summary here")
    assert summary["has_errors"] is False
    assert summary["error"] == 0


def test_dbt_registry_connection_failed_matches_hub_reset():
    output = (
        "External connection exception occurred: "
        "('Connection aborted.', ConnectionResetError(10054, 'WinError 10054'))"
    )
    assert dbt_registry_connection_failed(output) is True
    assert dbt_registry_connection_failed("Compilation Error in model foo") is False


def test_locked_dbt_packages_present(tmp_path: Path):
    (tmp_path / "package-lock.yml").write_text(
        "packages:\n- package: dbt-labs/dbt_utils\n  name: dbt_utils\n  version: 1.3.0\n",
        encoding="utf-8",
    )
    pkg = tmp_path / "dbt_packages" / "dbt_utils"
    pkg.mkdir(parents=True)
    assert locked_dbt_packages_present(tmp_path) is False
    (pkg / "dbt_project.yml").write_text("name: dbt_utils\n", encoding="utf-8")
    assert locked_dbt_packages_present(tmp_path) is True
