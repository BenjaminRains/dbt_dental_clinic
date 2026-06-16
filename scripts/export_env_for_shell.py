#!/usr/bin/env python3
"""
Export validated environment variables as JSON for PowerShell api-init / etl-init.

Phase 3: environment_manager.ps1 delegates .env loading to Python pydantic-settings
instead of parsing KEY=VALUE in PowerShell.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent


def export_env(component: str, stage: str, profile: str | None = None) -> dict[str, str]:
    if component == "api":
        api_dir = REPO_ROOT / "api"
        if str(api_dir) not in sys.path:
            sys.path.insert(0, str(api_dir))
        from settings import export_api_env_dict

        return export_api_env_dict(environment=stage)

    if component == "etl":
        etl_dir = REPO_ROOT / "etl_pipeline"
        if str(etl_dir) not in sys.path:
            sys.path.insert(0, str(etl_dir))
        os.environ["ETL_ENVIRONMENT"] = stage
        from etl_pipeline.config.settings_v2 import load_etl_env_dict

        return load_etl_env_dict(environment=stage, config_dir=etl_dir, profile=profile)

    raise ValueError(f"Unsupported component: {component}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Export validated env vars for PowerShell")
    parser.add_argument(
        "--component",
        choices=("api", "etl"),
        required=True,
        help="Subproject to export (api or etl)",
    )
    parser.add_argument(
        "--stage",
        required=True,
        help="Stage name (api: local|demo|clinic|test; etl: local|clinic|test)",
    )
    parser.add_argument(
        "--profile",
        choices=("load", "full"),
        default=None,
        help="ETL only: connection subset to validate (default: local→load, clinic/test→full)",
    )
    args = parser.parse_args()

    try:
        data = export_env(args.component, args.stage, profile=args.profile)
        json.dump(data, sys.stdout, sort_keys=True)
        return 0
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
