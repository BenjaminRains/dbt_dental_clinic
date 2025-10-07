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

    def __init__(self, output_file: str = "logs/method_usage.json"):
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