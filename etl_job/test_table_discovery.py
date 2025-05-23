import os
from dotenv import load_dotenv
import logging
from mysql_postgre_incremental import get_all_tables, get_metadata_columns
from connection_factory import get_source_connection

# Configure logging
log_file = os.path.join(os.path.dirname(__file__), "test_table_discovery.log")
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

def test_table_discovery():
    """Test the table discovery functionality."""
    try:
        # Get all tables
        table_configs = get_all_tables()
        
        # Print summary
        logger.info(f"Found {len(table_configs)} tables in the database")
        
        # Print details for each table
        for table_name, config in table_configs.items():
            logger.info(f"\nTable: {table_name}")
            logger.info(f"Created at column: {config.created_at_column}")
            logger.info(f"Updated at column: {config.updated_at_column}")
            
            # Verify the columns exist in the table
            with get_source_connection().connect() as conn:
                if config.created_at_column:
                    created_at, updated_at = get_metadata_columns(table_name, conn)
                    logger.info(f"Verified metadata columns - Created: {created_at}, Updated: {updated_at}")
        
        return True
    except Exception as e:
        logger.error(f"Error in test_table_discovery: {str(e)}")
        return False

if __name__ == "__main__":
    logger.info("Starting table discovery test...")
    success = test_table_discovery()
    if success:
        logger.info("Table discovery test completed successfully")
    else:
        logger.error("Table discovery test failed") 