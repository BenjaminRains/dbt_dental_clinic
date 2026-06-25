"""Minimal pwd stub so python-daemon imports on Windows (Airflow dev only)."""
from __future__ import annotations

from collections import namedtuple

struct_passwd = namedtuple(
    "struct_passwd",
    ["pw_name", "pw_passwd", "pw_uid", "pw_gid", "pw_gecos", "pw_dir", "pw_shell"],
)


def getpwnam(name: str) -> struct_passwd:
    return struct_passwd(name, "x", 1000, 1000, name, str(__import__("os").expanduser("~")), "")


def getpwuid(uid: int) -> struct_passwd:
    return getpwnam(str(uid))
