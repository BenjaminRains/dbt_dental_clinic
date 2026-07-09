# database connection handling

# api/database.py
from sqlalchemy import create_engine, event
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine import Engine
import logging

from config import get_config

logger = logging.getLogger(__name__)

config = get_config()
logger.info("Connecting to database for environment: %s", config.environment)


def _sslmode_for_config(cfg: dict) -> str:
    host = str(cfg.get("host") or "").lower()
    explicit = (config._settings.sslmode_env or "").strip()
    if explicit:
        return explicit
    if "rds.amazonaws.com" in host:
        return "require"
    return "prefer"


def _clinic_connection_creator():
    """Open a psycopg2 connection using the live (TTL-cached) clinic RDS password."""
    import psycopg2

    cfg = config.get_database_config()
    return psycopg2.connect(
        host=str(cfg["host"]),
        port=int(cfg["port"]),
        dbname=str(cfg["database"]),
        user=str(cfg["user"]),
        password=str(cfg["password"]),
        sslmode=_sslmode_for_config(cfg),
        connect_timeout=10,
    )


# pool_pre_ping: reconnect if RDS closed idle connections
# pool_recycle: clinic refreshes pooled connections within SM password TTL window
_pool_recycle = 300 if config.environment == "clinic" else 3600

if config.environment == "clinic":
    # Password is resolved per connect (Secrets Manager with TTL cache), not baked into URL.
    engine = create_engine(
        "postgresql+psycopg2://",
        creator=_clinic_connection_creator,
        pool_pre_ping=True,
        pool_recycle=_pool_recycle,
    )
    DATABASE_URL = "(clinic live Secrets Manager password)"
else:
    DATABASE_URL = config.get_database_url()
    engine = create_engine(
        DATABASE_URL,
        pool_pre_ping=True,
        pool_recycle=_pool_recycle,
    )

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


@event.listens_for(Engine, "handle_error")
def _on_db_auth_error(exception_context):
    """On clinic password auth failure, drop cached SM password for the next reconnect."""
    if config.environment != "clinic":
        return
    exc = exception_context.original_exception
    if exc is None:
        return
    message = str(exc).lower()
    if "password authentication failed" not in message and "28p01" not in message:
        return
    try:
        from clinic_rds_secret import invalidate_clinic_rds_password_cache

        invalidate_clinic_rds_password_cache()
        logger.warning(
            "Clinic RDS auth failed; invalidated live password cache for next reconnect"
        )
    except Exception:
        pass


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
