"""Minimal resource stub so python-daemon imports on Windows (Airflow dev only)."""
from __future__ import annotations

RLIMIT_CORE = 4
RLIMIT_NOFILE = 7
RLIM_INFINITY = -1

_DEFAULT_SOFT = 1024
_DEFAULT_HARD = 4096


def getrlimit(which: int) -> tuple[int, int]:
    if which == RLIMIT_CORE:
        return (0, 0)
    if which == RLIMIT_NOFILE:
        return (_DEFAULT_SOFT, _DEFAULT_HARD)
    raise ValueError(f"invalid resource limit: {which}")


def setrlimit(which: int, limits: tuple[int, int]) -> None:
    return None
