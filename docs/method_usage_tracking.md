### Method Usage Tracking Implementation Guide

This guide proposes a pragmatic strategy to monitor which `PostgresLoader` methods are actually 
executed during ETL runs, so we can objectively identify unused code for refactoring and removal. 
It avoids real-time monitoring; results are reviewed from artifacts (JSON reports and/or existing log files).

---

## Objectives
- Identify which `PostgresLoader` methods are invoked across ETL runs
- Quantify call counts and last-seen timestamps per method
- Keep overhead low, no production downtime, and no telemetry dependencies
- Make results easy to review and diff over time

---

## Strategy A (Recommended): Lightweight Decorator-Based Tracking

Add a small utility that decorates methods to record invocations into a JSON file. 
This is minimally invasive, opt-in per method via annotation, and reviewable post-run.

### Files to add
- `etl_pipeline/scripts/method_tracker.py` (new)

```python
"""
Method usage tracking for identifying unused code paths.
"""
import functools
import logging
import threading
from datetime import datetime
from pathlib import Path
import json
from typing import Dict, Any

logger = logging.getLogger(__name__)


class MethodTracker:
    """Tracks method calls across ETL runs and persists to JSON."""

    def __init__(self, output_file: str = "etl_pipeline/logs/method_usage.json"):
        self.output_file = Path(output_file)
        self.tracked_methods: Dict[str, Dict[str, Any]] = {}
        self.current_run_id = datetime.now().isoformat()
        self._lock = threading.Lock()

    def track(self, func):
        """Decorator to track method usage."""

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Determine class-qualified method id when available
            class_name = None
            if args and hasattr(args[0], "__class__"):
                class_name = args[0].__class__.__name__
            method_id = f"{class_name}.{func.__name__}" if class_name else func.__name__

            # Thread-safe tracking updates
            with self._lock:
                # Initialize record entry
                if method_id not in self.tracked_methods:
                    self.tracked_methods[method_id] = {
                        "call_count": 0,
                        "first_seen": self.current_run_id,
                        "last_seen": self.current_run_id,
                        "run_ids": set(),
                    }

                # Update metrics
                record = self.tracked_methods[method_id]
                record["call_count"] += 1
                record["last_seen"] = datetime.now().isoformat()
                record["run_ids"].add(self.current_run_id)

            logger.debug(f"[METHOD_TRACK] {method_id} called")
            return func(*args, **kwargs)

        return wrapper

    def save_report(self):
        """Persist tracking data to JSON (merges with prior runs)."""
        # Thread-safe access to tracked methods
        with self._lock:
            # Convert sets to lists for JSON serialization
            serializable = {}
            for method_id, data in self.tracked_methods.items():
                serializable[method_id] = {
                    "call_count": data["call_count"],
                    "first_seen": data["first_seen"],
                    "last_seen": data["last_seen"],
                    "run_ids": sorted(list(data["run_ids"])),
                }

        # Merge with existing file if present
        if self.output_file.exists():
            try:
                existing = json.loads(self.output_file.read_text(encoding="utf-8"))
            except Exception:
                existing = {}
            for method_id, data in serializable.items():
                if method_id in existing:
                    existing[method_id]["call_count"] = existing[method_id].get("call_count", 0) + data["call_count"]
                    existing[method_id]["last_seen"] = data["last_seen"]
                    # Merge run_ids (dedupe)
                    existing_runs = set(existing[method_id].get("run_ids", []))
                    existing[method_id]["run_ids"] = sorted(list(existing_runs.union(set(data["run_ids"]))))
                else:
                    existing[method_id] = data
            serializable = existing

        self.output_file.parent.mkdir(parents=True, exist_ok=True)
        self.output_file.write_text(json.dumps(serializable, indent=2), encoding="utf-8")
        logger.info(f"Method tracking data saved to {self.output_file}")

    def print_report(self):
        """Print a compact, human-readable report for quick review."""
        print("\n" + "=" * 60)
        print("METHOD USAGE REPORT")
        print("=" * 60)
        
        # Thread-safe access to tracked methods
        with self._lock:
            sorted_methods = sorted(self.tracked_methods.items(), key=lambda x: x[1]["call_count"], reverse=True)
            for method_id, data in sorted_methods:
                print(f"\n{method_id}")
                print(f"  Calls: {data['call_count']}")
                print(f"  First seen: {data['first_seen']}")
                print(f"  Last seen:  {data['last_seen']}")


# Global tracker helpers
_tracker = MethodTracker()


def track_method(func):
    """Convenience decorator using the global tracker instance."""
    return _tracker.track(func)


def save_tracking_report():
    _tracker.save_report()


def print_tracking_report():
    _tracker.print_report()
```

### Apply to `PostgresLoader`
- File: `etl_pipeline/etl_pipeline/loaders/postgres_loader.py`
- Add: `from method_tracker import track_method, save_tracking_report, print_tracking_report`
- Annotate methods you want to track with `@track_method`. Start with public methods and any private helpers relevant to load strategies and query building. Examples:

```python
from method_tracker import track_method

class PostgresLoader:
    """PostgreSQL loader with method tracking enabled."""

    @track_method
    def load_table(self, table_name: str, force_full: bool = False):
        ...

    @track_method
    def load_table_standard(self, table_name: str, force_full: bool = False):
        ...

    @track_method
    def load_table_streaming(self, table_name: str, force_full: bool = False):
        ...

    @track_method
    def load_table_chunked(self, table_name: str, force_full: bool = False, chunk_size: int = 25000):
        ...

    @track_method
    def load_table_copy_csv(self, table_name: str, force_full: bool = False):
        ...

    @track_method
    def load_table_parallel(self, table_name: str, force_full: bool = False):
        ...

    @track_method
    def verify_load(self, table_name: str) -> bool:
        ...

    @track_method
    def _build_load_query(self, table_name: str, incremental_columns, force_full: bool = False):
        ...
```

Tip: You can annotate all ~50+ methods, but a phased approach is fine. Start with the methods that
 the orchestrator invokes and any internal methods that influence strategy selection and query generation.

### Persisting the report reliably
Ensure the report is saved even if runs exit early:

- Option 1: In your orchestrator (e.g., `etl_pipeline/scripts/<runner>.py`), call on shutdown:

```python
from method_tracker import save_tracking_report, print_tracking_report

try:
    # orchestrate ETL
    ...
finally:
    save_tracking_report()
    print_tracking_report()
```

- Option 2: Register an `atexit` hook inside `method_tracker.py` (optional):

```python
import atexit
atexit.register(save_tracking_report)
```

Output file (default): `etl_pipeline/logs/method_usage.json`

---

## Strategy B: Pure Log Analysis (No Code Changes)

If you prefer zero code changes, parse existing ETL logs to infer method usage. 
This approach is best-effort and depends on consistent log patterns. It’s useful for historical analysis and quick wins.

### File to add
- `etl_pipeline/scripts/analyze_method_usage.py`

```python
"""
Analyze ETL logs to identify which methods are called.
"""
import re
from collections import Counter
from pathlib import Path
from typing import Dict


def analyze_logs(log_file: Path) -> Dict[str, int]:
    """Extract naive method mentions from a log file."""
    patterns = [
        r"PostgresLoader\.(\w+)",      # direct method refs
        r"Calling (\w+) for",            # e.g., "Calling load_table for ..."
        r"Starting (\w+)\(",            # e.g., "Starting load_table("
        r"(\w+) completed in",          # e.g., "load_table completed in ..."
    ]

    method_counts = Counter()
    with log_file.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            for pattern in patterns:
                matches = re.findall(pattern, line)
                method_counts.update(matches)
    return dict(method_counts)


def compare_with_codebase(method_counts: Dict[str, int], code_file: Path):
    """Compare referenced methods vs. defined methods in code."""
    content = code_file.read_text(encoding="utf-8", errors="ignore")
    all_methods = set(re.findall(r"def (\w+)\(self", content))
    called_methods = set(method_counts.keys())
    unused_methods = all_methods - called_methods

    print("\n" + "=" * 60)
    print("METHOD USAGE ANALYSIS FROM LOGS")
    print("=" * 60)
    print(f"\nTotal methods defined: {len(all_methods)}")
    print(f"Methods called: {len(called_methods)}")
    print(f"Methods never called: {len(unused_methods)}")

    if unused_methods:
        print("\nPotentially unused methods:")
        for method in sorted(unused_methods):
            print(f"  - {method}")

    print("\nMost frequently called methods:")
    for method, count in sorted(method_counts.items(), key=lambda x: x[1], reverse=True)[:10]:
        print(f"  {method}: {count} calls")


if __name__ == "__main__":
    # Adjust paths to your environment/log layout
    log_file = Path("etl_pipeline/logs/etl_pipeline/etl_pipeline_run_latest.log")
    if not log_file.exists():
        # Fallback example path
        log_file = Path("logs/etl_pipeline.log")

    code_file = Path("etl_pipeline/etl_pipeline/loaders/postgres_loader.py")

    counts = analyze_logs(log_file)
    compare_with_codebase(counts, code_file)
```

Notes:
- This method is heuristic and will miss methods without clear log lines.
- Consider standardizing log messages (e.g., "Starting {method}(...)", "{method} completed in …") to improve accuracy without decorators.

---

## Recommended Rollout
1) Phase 1 (1 day):
   - Add `method_tracker.py` and annotate primary public methods (`load_table*`, `verify_load`, data streaming, and bulk insert helpers).
   - Integrate `save_tracking_report()` in the orchestrator’s `finally` block.
   - Run one full ETL cycle; archive `etl_pipeline/logs/method_usage.json`.

2) Phase 2 (1–2 days):
   - Expand annotations to internal helpers that impact query building and strategy decisions.
   - Add a CI artifact retention step to keep `method_usage.json` per run.

3) Phase 3 (ongoing):
   - Use accumulated reports to flag unused methods for deletion and simplify code paths.
   - Gate removals with a final log-analysis pass (Strategy B) for corroboration.

---

## Review Checklist
- Are the tracked methods the ones we care most about for pruning/refactor decisions?
- Is `logs/method_usage.json` produced for every run (including failures)?
- Do we have enough runs (and variety) to avoid false negatives before removal?
- Do we corroborate findings with log analysis for high-signal confidence?

---

## Windows and Paths
- Default output path is `etl_pipeline/logs/method_usage.json`. Ensure `etl_pipeline/logs/` exists or let the tracker create it.
- Paths in examples use repo-relative locations and should work in PowerShell-driven runs on Windows 10+.


