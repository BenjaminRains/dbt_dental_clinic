from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Optional, Dict, Any, List


class EtlError(Exception):
    """Base ETL error with a machine-readable code."""

    def __init__(self, code: str, message: str, *, details: Optional[Dict[str, Any]] = None) -> None:
        super().__init__(message)
        self.code = code
        self.details: Dict[str, Any] = details or {}


@dataclass
class PipelineRunSummary:
    run_id: str
    pipeline: str
    clinic_id: Optional[str]
    started_at: datetime
    finished_at: Optional[datetime]
    status: str  # e.g. "success", "failed", "running"
    metadata: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class LagStatus:
    clinic_id: str
    metric: str  # e.g. "claims", "appointments"
    lag_minutes: int
    source_row_count: int
    target_row_count: int
    as_of: datetime

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def get_status(
    clinic_id: Optional[str] = None,
    pipeline: Optional[str] = None,
    since: Optional[datetime] = None,
    until: Optional[datetime] = None,
) -> List[PipelineRunSummary]:
    """Return recent pipeline run summaries for the given filters."""
    raise NotImplementedError("get_status is not yet implemented")


def run_pipeline(
    pipeline: str,
    clinic_id: str,
    as_of_date: Optional[datetime] = None,
    backfill_range: Optional[str] = None,
    dry_run: bool = False,
) -> PipelineRunSummary:
    """Trigger a pipeline run and return a summary."""
    raise NotImplementedError("run_pipeline is not yet implemented")


def inspect_lag(
    clinic_id: str,
    metric: str = "claims",
) -> LagStatus:
    """Compute replication/analytics lag metrics for a clinic."""
    raise NotImplementedError("inspect_lag is not yet implemented")


def get_logs(
    run_id: str,
    tail: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """Return structured logs for a given run."""
    raise NotImplementedError("get_logs is not yet implemented")

