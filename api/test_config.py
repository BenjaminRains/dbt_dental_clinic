#!/usr/bin/env python3
"""
Test script to verify API environment configuration.

Tests:
1. Stage env files exist under api/ (not repo root)
2. APIConfig loads and validates for the current API_ENVIRONMENT
3. Phase 0 precedence: OS env wins over on-disk .env_api_*; file loads when host var unset

Usage:
    python test_config.py              # env file inventory only if API_ENVIRONMENT unset
    python test_config.py local        # full tests for stage (no api-init required)
    python test_config.py --stage test

After api-init, API_ENVIRONMENT is already set — run with no arguments.
"""

import os
import sys
from pathlib import Path

api_dir = Path(__file__).resolve().parent
sys.path.insert(0, str(api_dir))

STAGES = ("test", "demo", "clinic", "local")

# host env var per stage (matches api/config.py ENV_MAPPINGS)
HOST_VARS = {
    "test": "TEST_POSTGRES_ANALYTICS_HOST",
    "demo": "DEMO_POSTGRES_HOST",
    "clinic": "POSTGRES_ANALYTICS_HOST",
    "local": "POSTGRES_ANALYTICS_HOST",
}


def parse_stage_arg() -> str | None:
    """Optional stage from CLI: test_config.py [local] or --stage local."""
    args = [a for a in sys.argv[1:] if a and not a.startswith("-")]
    for i, arg in enumerate(sys.argv[1:]):
        if arg in ("--stage", "-s") and i + 2 <= len(sys.argv[1:]):
            candidate = sys.argv[i + 2]
            if candidate in STAGES:
                return candidate
        if arg.startswith("--stage="):
            candidate = arg.split("=", 1)[1].strip()
            if candidate in STAGES:
                return candidate
    if len(args) == 1 and args[0] in STAGES:
        return args[0]
    return None


def ensure_api_environment(cli_stage: str | None) -> tuple[bool, bool]:
    """
    Resolve API_ENVIRONMENT for tests.

    Returns (ready_for_full_tests, stage_was_set_by_this_script).
    """
    if os.getenv("API_ENVIRONMENT") in STAGES:
        return True, False

    if cli_stage:
        os.environ["API_ENVIRONMENT"] = cli_stage
        print(f"Using stage from argument: API_ENVIRONMENT={cli_stage}")
        return True, True

    return False, False


def test_environment_files() -> bool:
    """Check api/.env_api_* files (and templates where committed)."""
    print("\n" + "=" * 60)
    print("ENVIRONMENT FILES CHECK (api/)")
    print("=" * 60)

    for stage in STAGES:
        env_path = api_dir / f".env_api_{stage}"
        template_path = api_dir / f".env_api_{stage}.template"
        if env_path.exists():
            print(f"[OK] api/.env_api_{stage} present")
        else:
            print(f"[--] api/.env_api_{stage} missing (create from template if needed)")
        if template_path.exists():
            print(f"  template: api/.env_api_{stage}.template")

    return True


def test_api_config() -> bool:
    """Load APIConfig for current API_ENVIRONMENT."""
    try:
        from config import APIConfig, DatabaseType

        print("\n" + "=" * 60)
        print("API CONFIGURATION TEST")
        print("=" * 60)

        current_env = os.getenv("API_ENVIRONMENT", "NOT SET")
        print(f"Current API_ENVIRONMENT: {current_env}")

        print("\n1. Testing configuration loading...")
        config = APIConfig()
        print(f"   OK Environment detected: {config.environment}")

        print("\n2. Testing database configuration...")
        db_config = config.get_database_config(DatabaseType.ANALYTICS)
        print(f"   OK Host: {db_config['host']}")
        print(f"   OK Port: {db_config['port']}")
        print(f"   OK Database: {db_config['database']}")
        print(f"   OK User: {db_config['user']}")
        print(f"   OK Password: {'*' * len(db_config['password']) if db_config['password'] else 'NOT SET'}")

        print("\n3. Testing database URL generation...")
        db_url = config.get_database_url(DatabaseType.ANALYTICS)
        if "@" in db_url:
            user_part = db_url.split("@", 1)[0]
            rest = db_url.split("@", 1)[1]
            safe_url = f"{user_part.split('://')[0]}://***@{rest}"
        else:
            safe_url = db_url
        print(f"   OK Database URL: {safe_url}")

        print("\n" + "=" * 60)
        print("PASS: API CONFIGURATION TEST")
        print("=" * 60)
        return True

    except Exception as e:
        print(f"\nFAIL: API CONFIGURATION TEST: {e}")
        print("\nTroubleshooting:")
        print("1. Run api-init, or pass a stage: python test_config.py local")
        print("2. Create api/.env_api_<stage> from the matching template")
        print("3. Ensure required vars are set in that file")
        return False


def test_env_precedence() -> bool:
    """
    Phase 0: OS process env wins over api/.env_api_<stage>; file is used when host var unset.
    """
    from dotenv import dotenv_values

    from config import APIConfig, DatabaseType, reset_config

    print("\n" + "=" * 60)
    print("ENV PRECEDENCE TEST (Phase 0)")
    print("=" * 60)

    stage = os.getenv("API_ENVIRONMENT")
    if not stage or stage not in HOST_VARS:
        print("SKIP — API_ENVIRONMENT not set")
        return True

    host_var = HOST_VARS[stage]
    env_file = api_dir / f".env_api_{stage}"
    bogus = "bogus-host-precedence-test"
    saved_host = os.environ.get(host_var)

    print(f"   Stage: {stage}")
    print(f"   Host var: {host_var}")
    print(f"   Env file: {env_file}")

    def _seed_non_host_vars_from_file() -> None:
        """Simulate api-init: creds in OS, only host is under test for Part 1."""
        if not env_file.exists():
            return
        for key, value in dotenv_values(env_file).items():
            if value and key != host_var and not os.getenv(key):
                os.environ[key] = value

    try:
        _seed_non_host_vars_from_file()
        os.environ[host_var] = bogus
        reset_config()
        host = APIConfig().get_database_config(DatabaseType.ANALYTICS)["host"]
        if host != bogus:
            print(f"FAIL Part 1: expected OS host {bogus!r}, got {host!r}")
            return False
        print(f"OK Part 1: OS env wins ({host_var}={bogus!r})")

        os.environ.pop(host_var, None)
        reset_config()
        host = APIConfig().get_database_config(DatabaseType.ANALYTICS)["host"]

        if not env_file.exists():
            print(f"SKIP Part 2 — {env_file.name} not found")
            return True

        file_vals = dotenv_values(env_file)
        expected = file_vals.get(host_var)
        if not expected:
            print(f"SKIP Part 2 — {host_var} not in {env_file.name}")
            return True

        if host == bogus:
            print(f"FAIL Part 2: still using bogus host after unsetting {host_var}")
            return False
        if host != expected:
            print(
                f"FAIL Part 2: expected host from file {expected!r}, got {host!r} "
                f"(other shell vars may still override non-host keys)"
            )
            return False

        print(f"OK Part 2: file loads when OS host unset (host={host!r})")
        print("\nPASS: ENV PRECEDENCE TEST")
        return True

    finally:
        if saved_host is None:
            os.environ.pop(host_var, None)
        else:
            os.environ[host_var] = saved_host
        reset_config()


def print_usage_hint() -> None:
    print("\n" + "-" * 60)
    print("Config/precedence tests skipped - API_ENVIRONMENT is not set.")
    print("Either:")
    print("  - Run api-init (then api-run / re-run this script), or")
    print("  - Pass a stage (no api-init needed):")
    print("      python test_config.py local")
    print("      python test_config.py --stage test")
    print("-" * 60)


if __name__ == "__main__":
    print("Testing API Environment Configuration...")

    cli_stage = parse_stage_arg()
    ready, _ = ensure_api_environment(cli_stage)

    test_environment_files()

    if not ready:
        print_usage_hint()
        print("\nEnv file inventory complete.")
        sys.exit(0)

    config_ok = test_api_config()
    precedence_ok = test_env_precedence()

    if config_ok and precedence_ok:
        print("\nAll tests passed.")
        sys.exit(0)

    print("\nTests failed. Fix the issues above.")
    sys.exit(1)
