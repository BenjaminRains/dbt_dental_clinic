"""Start local API with isolated mdc env (includes CLINIC_PORTAL_SESSION_SECRET)."""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "tools" / "mdc_cli"))
sys.path.insert(0, str(ROOT / "api"))

from mdc_cli.env import load_api_env_dict  # noqa: E402
from mdc_cli.paths import discover_component_python  # noqa: E402
from mdc_cli.run_helper import build_isolated_child_env, scrub_parent_stage_env  # noqa: E402


def main() -> int:
    port = "8000"
    if "--port" in sys.argv:
        idx = sys.argv.index("--port")
        if idx + 1 >= len(sys.argv):
            print("usage: start_local_api_with_portal.py [--port N]", file=sys.stderr)
            return 2
        port = sys.argv[idx + 1]

    with scrub_parent_stage_env():
        settings = load_api_env_dict("local")
    child = build_isolated_child_env(settings)
    has_secret = bool(
        (child.get("CLINIC_PORTAL_SESSION_SECRET") or "").strip()
        or (child.get("CLINIC_API_KEY") or "").strip()
    )
    print(f"portal_secret_present={has_secret}")
    if not has_secret:
        print(
            "CLINIC_PORTAL_SESSION_SECRET or CLINIC_API_KEY missing from API local env.",
            file=sys.stderr,
        )
        return 2

    py = discover_component_python("api")
    cmd = [
        str(py),
        "-m",
        "uvicorn",
        "main:app",
        "--host",
        "127.0.0.1",
        "--port",
        port,
    ]
    # No --reload: keeps env stable for portal login smoke
    proc = subprocess.Popen(cmd, cwd=str(ROOT / "api"), env=child)
    print(f"spawned_pid={proc.pid} port={port}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
