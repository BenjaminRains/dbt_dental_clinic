import os
from sqlalchemy import create_engine

def get_source_connection():
    """
    Returns a SQLAlchemy engine for the production (source) MySQL/MariaDB database.
    Enforces use of the read-only user. Source database is the live client opendental database.
    """
    return create_engine(
        f"mysql+pymysql://{os.getenv('SOURCE_DB_READONLY_USER')}:{os.getenv('SOURCE_DB_READONLY_PASSWORD')}@"
        f"{os.getenv('SOURCE_DB_HOST')}:{os.getenv('SOURCE_DB_PORT')}/"
        f"{os.getenv('SOURCE_DB_NAME')}"
    )

def get_target_connection():
    """
    Returns a SQLAlchemy engine for the analytics (target) PostgreSQL database.
    """
    return create_engine(
        f"postgresql+psycopg2://{os.getenv('TARGET_DB_USER')}:{os.getenv('TARGET_DB_PASSWORD')}@"
        f"{os.getenv('TARGET_DB_HOST')}:{os.getenv('TARGET_DB_PORT')}/"
        f"{os.getenv('TARGET_DB_NAME')}"
    )
