"""Rewrite localhost DB hosts when running inside Docker (Compose Phase A)."""

from __future__ import annotations

import os
from pathlib import Path


def running_in_docker() -> bool:
    return Path("/.dockerenv").exists() or os.environ.get("MDC_IN_DOCKER", "").lower() in (
        "1",
        "true",
        "yes",
    )


def host_gateway() -> str:
    return os.environ.get("MDC_HOST_GATEWAY", "host.docker.internal")


def rewrite_localhost_hosts(env: dict[str, str]) -> dict[str, str]:
    """
    Point *_HOST=localhost/127.0.0.1 at the Docker Desktop host gateway.

    Stage files keep localhost for native mdc; Compose LocalExecutor tasks need
    the host's Postgres/MySQL. Clinic nightly remains native (out of scope).
    """
    if not running_in_docker():
        return env
    gateway = host_gateway()
    out = dict(env)
    for key, value in list(out.items()):
        if not key.endswith("_HOST"):
            continue
        if value in ("localhost", "127.0.0.1"):
            out[key] = gateway
    return out
