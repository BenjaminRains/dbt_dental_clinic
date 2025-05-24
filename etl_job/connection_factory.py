import os
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import logging
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("connection.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

def get_source_connection(readonly=True):
    """
    Create source connection with mandatory readonly flag.
    Enforces read-only access to the OpenDental operational database.
    NEVER uses root user - only uses the dedicated readonly_user.
    """
    if not readonly:
        raise ValueError("Write access to source database is not allowed")
    
    # Prevent accidental use of root user
    if os.getenv('OPENDENTAL_SOURCE_USER', '').lower() in ['root', 'admin']:
        raise ValueError("Root/admin user access is not allowed for ETL operations")
    
    try:
        engine = create_engine(
            f"mysql+pymysql://{os.getenv('OPENDENTAL_SOURCE_USER')}:{os.getenv('OPENDENTAL_SOURCE_PW')}@"
            f"{os.getenv('OPENDENTAL_SOURCE_HOST')}:{os.getenv('OPENDENTAL_SOURCE_PORT')}/"
            f"{os.getenv('OPENDENTAL_SOURCE_DB')}"
        )
        
        # Test connection and verify read-only access
        with engine.connect() as conn:
            # Try to execute a SELECT query
            conn.execute(text("SELECT 1"))
            
            # Try to execute a write operation (should fail)
            try:
                conn.execute(text("CREATE TABLE test_readonly (id INT)"))
                raise ValueError("Write access is not properly restricted")
            except SQLAlchemyError:
                logger.info("Read-only access verified successfully")
            
            # Verify we're not using root
            result = conn.execute(text("SELECT CURRENT_USER()")).scalar()
            if 'root' in result.lower():
                raise ValueError("Connection is using root user - this is not allowed")
            
            logger.info(f"Connected as user: {result}")
        
        return engine
    except Exception as e:
        logger.error(f"Failed to create source connection: {str(e)}")
        raise

def get_staging_connection():
    """
    Create staging MySQL connection for intermediate data storage.
    This database should be local and have full read/write access.
    """
    try:
        engine = create_engine(
            f"mysql+pymysql://{os.getenv('STAGING_MYSQL_USER')}:{os.getenv('STAGING_MYSQL_PASSWORD')}@"
            f"{os.getenv('STAGING_MYSQL_HOST')}:{os.getenv('STAGING_MYSQL_PORT')}/"
            f"{os.getenv('STAGING_MYSQL_DATABASE')}"
        )
        
        # Test connection and verify write access
        with engine.connect() as conn:
            # Test basic connection
            conn.execute(text("SELECT 1"))
            
            # Test write access
            try:
                conn.execute(text("CREATE TABLE IF NOT EXISTS test_write (id INT)"))
                conn.execute(text("DROP TABLE IF EXISTS test_write"))
                logger.info("Write access verified successfully")
            except SQLAlchemyError as e:
                logger.error(f"Write access test failed: {str(e)}")
                raise
            
            logger.info("Staging connection verified successfully")
        
        return engine
    except Exception as e:
        logger.error(f"Failed to create staging connection: {str(e)}")
        raise

def get_target_connection():
    """
    Create target PostgreSQL connection for analytics database.
    """
    try:
        engine = create_engine(
            f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@"
            f"{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/"
            f"{os.getenv('POSTGRES_DATABASE')}"
        )
        
        # Test connection
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
            logger.info("Target connection verified successfully")
        
        return engine
    except Exception as e:
        logger.error(f"Failed to create target connection: {str(e)}")
        raise

def execute_source_query(query, params=None):
    """
    Execute read-only queries against source database.
    Enforces SELECT-only operations.
    """
    if not query.strip().upper().startswith("SELECT"):
        raise ValueError("Only SELECT queries are allowed against source database")
    
    source_conn = get_source_connection(readonly=True)
    try:
        with source_conn.connect() as conn:
            return conn.execute(text(query), params)
    except Exception as e:
        logger.error(f"Query execution failed: {str(e)}")
        raise

def test_connections():
    """
    Test all database connections.
    Returns True if all connections are successful.
    """
    try:
        # Test source connection
        source_conn = get_source_connection(readonly=True)
        with source_conn.connect() as conn:
            conn.execute(text("SELECT 1"))
            logger.info("Source connection test successful")
        
        # Test staging connection
        staging_conn = get_staging_connection()
        with staging_conn.connect() as conn:
            conn.execute(text("SELECT 1"))
            logger.info("Staging connection test successful")
        
        # Test target connection
        target_conn = get_target_connection()
        with target_conn.connect() as conn:
            conn.execute(text("SELECT 1"))
            logger.info("Target connection test successful")
        
        return True
    except Exception as e:
        logger.error(f"Connection test failed: {str(e)}")
        return False
