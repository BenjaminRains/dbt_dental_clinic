"""Plain source MySQL configuration (no MDC Settings)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Mapping, Union


@dataclass(frozen=True)
class SourceMySQLConfig:
    """Connection parameters for a read-only Open Dental MySQL source."""

    host: str
    port: int
    database: str
    user: str
    password: str
    connect_timeout: int = 10
    read_timeout: int = 30
    write_timeout: int = 30
    charset: str = "utf8mb4"
    pool_size: int = 20
    max_overflow: int = 40
    pool_timeout: int = 300
    pool_recycle: int = 1800

    @classmethod
    def from_mapping(cls, data: Mapping[str, Any]) -> "SourceMySQLConfig":
        """Build from a plain dict (Airbyte-style or Settings.get_source_connection_config())."""
        required = ("host", "port", "database", "user", "password")
        missing = [k for k in required if not data.get(k) and data.get(k) != 0]
        if missing:
            raise ValueError(f"SourceMySQLConfig missing required keys: {missing}")
        return cls(
            host=str(data["host"]),
            port=int(data["port"]),
            database=str(data["database"]),
            user=str(data["user"]),
            password=str(data["password"]),
            connect_timeout=int(data.get("connect_timeout", 10)),
            read_timeout=int(data.get("read_timeout", 30)),
            write_timeout=int(data.get("write_timeout", 30)),
            charset=str(data.get("charset", "utf8mb4")),
            pool_size=int(data.get("pool_size", 20)),
            max_overflow=int(data.get("max_overflow", 40)),
            pool_timeout=int(data.get("pool_timeout", 300)),
            pool_recycle=int(data.get("pool_recycle", 1800)),
        )

    def to_dict(self) -> Dict[str, Union[str, int]]:
        return {
            "host": self.host,
            "port": self.port,
            "database": self.database,
            "user": self.user,
            "password": self.password,
            "connect_timeout": self.connect_timeout,
            "read_timeout": self.read_timeout,
            "write_timeout": self.write_timeout,
            "charset": self.charset,
            "pool_size": self.pool_size,
            "max_overflow": self.max_overflow,
            "pool_timeout": self.pool_timeout,
            "pool_recycle": self.pool_recycle,
        }
