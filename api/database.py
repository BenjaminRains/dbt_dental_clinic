# database connection handling

# api/database.py
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import logging

from config import get_config

logger = logging.getLogger(__name__)

# Get database configuration using proper environment handling
config = get_config()
DATABASE_URL = config.get_database_url()

logger.info(f"Connecting to database for environment: {config.environment}")

# pool_pre_ping: reconnect if RDS closed idle connections; reduces OperationalError bursts
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_recycle=3600,
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Dependency to get DB session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()