import logging
from sqlalchemy import text
from connection_factory import get_target_connection

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("postgres_target_test.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def test_postgres_connection():
    """Test PostgreSQL connection and verify target configuration."""
    try:
        # Get target connection
        engine = get_target_connection()
        logger.info("Successfully created PostgreSQL connection")
        
        with engine.connect() as conn:
            # Test basic connection
            conn.execute(text("SELECT 1"))
            logger.info("Basic connection test successful")
            
            # Get database info
            db_info = conn.execute(text("SELECT current_database(), current_user, version()")).fetchone()
            logger.info(f"Connected to database: {db_info[0]}")
            logger.info(f"Connected as user: {db_info[1]}")
            logger.info(f"PostgreSQL version: {db_info[2]}")
            
            # Check if etl_sync_status table exists
            sync_status_exists = conn.execute(text(
                "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'etl_sync_status')"
            )).scalar()
            
            if sync_status_exists:
                logger.info("etl_sync_status table exists")
                # Show table structure
                columns = conn.execute(text(
                    "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'etl_sync_status'"
                )).fetchall()
                logger.info("etl_sync_status table structure:")
                for col in columns:
                    logger.info(f"  {col[0]}: {col[1]}")
            else:
                logger.warning("etl_sync_status table does not exist")
            
            # List all existing tables
            tables = conn.execute(text(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
            )).fetchall()
            
            logger.info("\nExisting tables in public schema:")
            for table in tables:
                logger.info(f"  {table[0]}")
                
            return True
            
    except Exception as e:
        logger.error(f"PostgreSQL connection test failed: {str(e)}")
        return False

if __name__ == "__main__":
    logger.info("Starting PostgreSQL target configuration test...")
    success = test_postgres_connection()
    if success:
        logger.info("PostgreSQL target configuration test completed successfully")
    else:
        logger.error("PostgreSQL target configuration test failed") 