"""Report catalog for the QuickBooks Online reports API."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


def catalog_path() -> Path:
    return Path(__file__).resolve().parents[1] / "config" / "reports.yml"


@dataclass(frozen=True)
class ReportSpec:
    key: str
    api_name: str
    description: str
    date_param: str

    def query_params(
        self,
        *,
        start_date: str | None,
        end_date: str | None,
        as_of: str | None,
        minor_version: int,
    ) -> dict[str, str]:
        params = {"minorversion": str(minor_version)}
        if self.date_param == "range":
            if not start_date or not end_date:
                raise ValueError(f"{self.key} needs --start and --end")
            params["start_date"] = start_date
            params["end_date"] = end_date
        elif self.date_param == "as_of":
            if not as_of:
                raise ValueError(f"{self.key} needs --as-of")
            params["report_date"] = as_of
        else:
            raise ValueError(f"Unknown date_param {self.date_param} for {self.key}")
        return params


@dataclass(frozen=True)
class ReportCatalog:
    minor_version: int
    defaults: tuple[str, ...]
    reports: dict[str, ReportSpec]

    def get(self, key: str) -> ReportSpec:
        try:
            return self.reports[key]
        except KeyError as exc:
            known = ", ".join(sorted(self.reports))
            raise KeyError(f"Unknown report {key}. Known reports: {known}") from exc


def load_report_catalog(path: Path | None = None) -> ReportCatalog:
    catalog_file = path or catalog_path()
    raw: dict[str, Any] = yaml.safe_load(catalog_file.read_text(encoding="utf-8"))
    metadata = raw.get("metadata") or {}
    reports: dict[str, ReportSpec] = {}
    for key, spec in (raw.get("reports") or {}).items():
        reports[key] = ReportSpec(
            key=key,
            api_name=spec["api_name"],
            description=str(spec.get("description") or "").strip(),
            date_param=spec["date_param"],
        )
    defaults = tuple(raw.get("defaults") or [])
    missing = [key for key in defaults if key not in reports]
    if missing:
        raise ValueError(f"Default reports are not defined: {', '.join(missing)}")
    return ReportCatalog(
        minor_version=int(metadata.get("minor_version") or 75),
        defaults=defaults,
        reports=reports,
    )
