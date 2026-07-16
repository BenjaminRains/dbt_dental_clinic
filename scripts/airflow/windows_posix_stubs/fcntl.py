"""Minimal fcntl stub so Airflow / gunicorn import on Windows (dev only)."""
from __future__ import annotations

F_GETFD = 1
F_SETFD = 2
F_GETFL = 3
F_SETFL = 4
FD_CLOEXEC = 1

# flock constants (posix); no-op locking is acceptable for single-user Windows laptop
LOCK_SH = 1
LOCK_EX = 2
LOCK_NB = 4
LOCK_UN = 8


def fcntl(fd: int, cmd: int, arg: int = 0) -> int:
    return 0


def flock(fd: int, operation: int) -> None:
    """No-op file lock — Windows lacks POSIX flock; fine for local Airflow dev."""
    return None


def lockf(fd: int, cmd: int, len: int = 0, start: int = 0, whence: int = 0) -> None:
    return None
