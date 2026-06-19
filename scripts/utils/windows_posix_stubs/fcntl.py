"""Minimal fcntl stub so gunicorn imports on Windows (Airflow dev only)."""
from __future__ import annotations

F_GETFD = 1
F_SETFD = 2
F_GETFL = 3
F_SETFL = 4
FD_CLOEXEC = 1


def fcntl(fd: int, cmd: int, arg: int = 0) -> int:
    return 0
