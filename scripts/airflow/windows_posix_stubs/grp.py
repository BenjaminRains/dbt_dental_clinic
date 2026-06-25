"""Minimal grp stub so python-daemon imports on Windows (Airflow dev only)."""
from __future__ import annotations

from collections import namedtuple

struct_group = namedtuple("struct_group", ["gr_name", "gr_passwd", "gr_gid", "gr_mem"])


def getgrnam(name: str) -> struct_group:
    return struct_group(name, "x", 1000, [])


def getgrgid(gid: int) -> struct_group:
    return getgrnam(str(gid))
