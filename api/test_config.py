#!/usr/bin/env python3
"""
Test script to verify API environment configuration.

Tests:
1. Stage env files exist under api/ (not repo root)
2. APIConfig loads and validates for the current API_ENVIRONMENT
3. Phase 0 precedence: OS env wins over on-disk .env_api_*; file loads when host var unset
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


def test_environment_files() -> bool:
    """Check api/.env_api_* files (and templates where committed)."""
    print("\n" + "=" * 60)
    print("ENVIRONMENT FILES CHECK (api/)")
    print("=" * 60)

    ok = True
    for stage in STAGES:
        env_path = api_dir / f".env_api_{stage}"
        template_path = api_dir / f".env_api_{stage}.template"
        if env_path.exists():
            print(f"[OK] api/.env_api_{stage} present")
        else:
            print(f"[--] api/.env_api_{stage} missing (create from template if needed)")
        if template_path.exists():
            print(f"  template: api/.env_api_{stage}.template")

    return ok


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
        print("1. Set API_ENVIRONMENT to test, demo, clinic, or local (e.g. api-init)")
        print("2. Create api/.env_api_<stage> from the matching template")
        print("3. Ensure required vars are set in that file or exported in the shell")
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
        print("SKIP Part 1 — set API_ENVIRONMENT (test/demo/clinic/local) first")
        return True

    host_var = HOST_VARS[stage]
    env_file = api_dir / f".env_api_{stage}"
    bogus = "bogus-host-precedence-test"
    saved_host = os.environ.get(host_var)

    print(f"   Stage: {stage}")
    print(f"   Host var: {host_var}")
    print(f"   Env file: {env_file}")

    try:
        # Part 1: OS env wins — config skips loading the file when host is already set
        os.environ[host_var] = bogus
        reset_config()
        host = APIConfig().get_database_config(DatabaseType.ANALYTICS)["host"]
        if host != bogus:
            print(f"FAIL Part 1: expected OS host {bogus!r}, got {host!r}")
            return False
        print(f"OK Part 1: OS env wins ({host_var}={bogus!r})")

        # Part 2: unset host → load from file (when file exists and defines host)
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


if __name__ == "__main__":
    print("Testing API Environment Configuration...")

    test_environment_files()
    config_ok = test_api_config()
    precedence_ok = test_env_precedence()

    if config_ok and precedence_ok:
        print("\nAll tests passed.")
        sys.exit(0)

    print("\nTests failed. Fix the issues above.")
    sys.exit(1)
