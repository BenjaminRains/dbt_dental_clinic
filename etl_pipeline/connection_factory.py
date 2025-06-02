import os
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import logging
from dotenv import load_dotenv
from pathlib import Path

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

def load_etl_env():
    """Load environment variables for ETL pipeline."""
    # Get the directory of this file
    current_dir = Path(__file__).parent
    
    # Try to load from etl_pipeline/.env first
    etl_env_path = current_dir / '.env'
    if etl_env_path.exists():
        logger.info(f"Loading environment from {etl_env_path}")
        load_dotenv(etl_env_path)
    else:
        # Fall back to parent directory .env
        parent_env_path = current_dir.parent / '.env'
        if parent_env_path.exists():
            logger.info(f"Loading environment from {parent_env_path}")
            load_dotenv(parent_env_path)
        else:
            logger.warning("No .env file found in etl_pipeline or parent directory")

def validate_config():
    """Validate that all required environment variables are set."""
    required_vars = {
        'Source': ['OPENDENTAL_SOURCE_HOST', 'OPENDENTAL_SOURCE_PORT', 
                  'OPENDENTAL_SOURCE_DB', 'OPENDENTAL_SOURCE_USER', 
                  'OPENDENTAL_SOURCE_PW'],  # Changed from OPENDENTAL_SOURCE_PASSWORD
        'Staging': ['STAGING_MYSQL_HOST', 'STAGING_MYSQL_PORT', 
                   'STAGING_MYSQL_DB', 'STAGING_MYSQL_USER', 
                   'STAGING_MYSQL_PASSWORD'],
        'Target': ['TARGET_POSTGRES_HOST', 'TARGET_POSTGRES_PORT', 
                  'TARGET_POSTGRES_DB', 'TARGET_POSTGRES_USER', 
                  'TARGET_POSTGRES_PASSWORD', 'TARGET_POSTGRES_SCHEMA']
    }
    
    missing_vars = []
    empty_vars = []
    
    for env_type, vars_list in required_vars.items():
        for var in vars_list:
            value = os.getenv(var)
            if not value:
                missing_vars.append(f"{env_type}: {var}")
            elif value.lower() in ['none', 'null', '']:
                empty_vars.append(f"{env_type}: {var} = '{value}'")
    
    if missing_vars or empty_vars:
        logger.error("Environment variable issues found:")
        if missing_vars:
            logger.error("Missing variables:")
            for var in missing_vars:
                logger.error(f"  - {var}")
        if empty_vars:
            logger.error("Empty/invalid variables:")
            for var in empty_vars:
                logger.error(f"  - {var}")
        
        logger.info("Please check your .env file and ensure all variables are properly set")
        return False
    
    return True

# Load environment variables
load_etl_env()

def get_source_connection(readonly=True):
    """
    Create source connection with mandatory readonly flag.
    Enforces read-only access to the OpenDental operational database.
    Handles SSL/TLS connection issues.
    """
    if not readonly:
        raise ValueError("Write access to source database is not allowed")
    
    # Prevent accidental use of root user
    user = os.getenv('OPENDENTAL_SOURCE_USER', '')
    if user.lower() in ['root', 'admin']:
        raise ValueError("Root/admin user access is not allowed for ETL operations")
    
    try:
        # Build connection string with SSL handling
        host = os.getenv('OPENDENTAL_SOURCE_HOST')
        port = os.getenv('OPENDENTAL_SOURCE_PORT')
        database = os.getenv('OPENDENTAL_SOURCE_DB')
        password = os.getenv('OPENDENTAL_SOURCE_PW')  # Changed from OPENDENTAL_SOURCE_PASSWORD
        
        # Validate required parameters
        if not all([host, port, database, user, password]):
            missing = [k for k, v in {
                'host': host, 'port': port, 'database': database, 
                'user': user, 'password': password
            }.items() if not v]
            raise ValueError(f"Missing required connection parameters: {', '.join(missing)}")
        
        # Connection string with SSL disabled (common for internal networks)
        # Handle the specific SSL error you encountered
        connection_string = (
            f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
            f"?ssl_disabled=true&charset=utf8mb4"
        )
        
        logger.info(f"Attempting connection to {host}:{port}/{database} as {user}")
        
        engine = create_engine(
            connection_string,
            connect_args={
                "ssl_disabled": True,
                "ssl_verify_cert": False,
                "ssl_verify_identity": False,
                "charset": "utf8mb4"
            }
        )
        
        # Test connection and verify read-only access
        with engine.connect() as conn:
            # Try to execute a SELECT query
            conn.execute(text("SELECT 1"))
            
            # Try to execute a write operation (should fail)
            try:
                conn.execute(text("CREATE TABLE test_readonly (id INT)"))
                logger.warning("Write access is not properly restricted!")
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
        logger.info("Troubleshooting tips:")
        logger.info("1. Check if MySQL server allows connections from your IP")
        logger.info("2. Verify user credentials and permissions")
        logger.info("3. Try connecting with SSL disabled: mysql -h HOST -P PORT -u USER -p --ssl-mode=DISABLED")
        raise

def get_staging_connection():
    """
    Create staging MySQL connection for intermediate data storage.
    This database should be local and have full read/write access.
    """
    try:
        host = os.getenv('STAGING_MYSQL_HOST')
        port = os.getenv('STAGING_MYSQL_PORT')
        database = os.getenv('STAGING_MYSQL_DB')
        user = os.getenv('STAGING_MYSQL_USER')
        password = os.getenv('STAGING_MYSQL_PASSWORD')
        
        # Validate required parameters
        if not all([host, port, database, user, password]):
            missing = [k for k, v in {
                'host': host, 'port': port, 'database': database, 
                'user': user, 'password': password
            }.items() if not v]
            raise ValueError(f"Missing required staging connection parameters: {', '.join(missing)}")
        
        logger.info(f"Attempting staging connection to {host}:{port}/{database} as {user}")
        
        engine = create_engine(
            f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
            f"?ssl_disabled=true&charset=utf8mb4",
            connect_args={
                "ssl_disabled": True,
                "ssl_verify_cert": False,
                "ssl_verify_identity": False,
                "charset": "utf8mb4"
            }
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
        host = os.getenv('TARGET_POSTGRES_HOST')
        port = os.getenv('TARGET_POSTGRES_PORT')
        database = os.getenv('TARGET_POSTGRES_DB')
        user = os.getenv('TARGET_POSTGRES_USER')
        password = os.getenv('TARGET_POSTGRES_PASSWORD')
        
        # Validate required parameters
        if not all([host, port, database, user, password]):
            missing = [k for k, v in {
                'host': host, 'port': port, 'database': database, 
                'user': user, 'password': password
            }.items() if not v]
            raise ValueError(f"Missing required PostgreSQL connection parameters: {', '.join(missing)}")
        
        logger.info(f"Attempting PostgreSQL connection to {host}:{port}/{database} as {user}")
        
        engine = create_engine(
            f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
        )
        
        # Test connection and table access
        with engine.connect() as conn:
            # Test basic connection
            conn.execute(text("SELECT 1"))
            
            # Test table access - try to read from etl_load_status
            try:
                conn.execute(text("SELECT COUNT(*) FROM raw.etl_load_status"))
                logger.info("Table read access verified successfully")
                
                # Test table write access
                conn.execute(text("""
                    INSERT INTO raw.etl_load_status (table_name, last_extracted, rows_extracted, extraction_status)
                    VALUES ('test_table', CURRENT_TIMESTAMP, 0, 'test')
                    ON CONFLICT (table_name) DO NOTHING
                """))
                logger.info("Table write access verified successfully")
            except SQLAlchemyError as e:
                logger.warning(f"Table access test failed: {str(e)}")
                logger.warning("PostgreSQL connection works but may lack table permissions")
            
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
    # First validate environment variables
    if not validate_config():
        logger.error("Environment validation failed")
        return False

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