#!/usr/bin/env python3
"""
Script to set up the local replication database from a MySQL dump file.
This script:
1. Creates the replication database if it doesn't exist
2. Imports data from a MySQL dump file
3. Verifies the import
"""

import os
import sys
import logging
from pathlib import Path
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def load_config():
    """Load configuration from environment variables."""
    load_dotenv()
    
    config = {
        'host': os.getenv('MYSQL_REPLICATION_HOST', 'localhost'),
        'port': int(os.getenv('MYSQL_REPLICATION_PORT', '3306')),
        'user': os.getenv('MYSQL_REPLICATION_USER'),
        'password': os.getenv('MYSQL_REPLICATION_PASSWORD'),
        'database': os.getenv('MYSQL_REPLICATION_DB', 'opendental_replication')
    }
    
    # Validate required config
    if not all([config['user'], config['password']]):
        raise ValueError("Missing required environment variables. Please check .env file.")
    
    return config

def create_database(config):
    """Create the replication database if it doesn't exist."""
    try:
        # Connect without database
        conn = mysql.connector.connect(
            host=config['host'],
            port=config['port'],
            user=config['user'],
            password=config['password']
        )
        
        cursor = conn.cursor()
        
        # Create database if not exists
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {config['database']}")
        logger.info(f"Database {config['database']} created or already exists")
        
        cursor.close()
        conn.close()
        
    except Error as e:
        logger.error(f"Error creating database: {e}")
        raise

def import_dump(config, dump_file):
    """Import data from MySQL dump file."""
    if not os.path.exists(dump_file):
        raise FileNotFoundError(f"Dump file not found: {dump_file}")
    
    try:
        # Import using mysql command line
        import_cmd = f"mysql -h {config['host']} -P {config['port']} -u {config['user']} -p{config['password']} {config['database']} < {dump_file}"
        logger.info(f"Importing dump file: {dump_file}")
        os.system(import_cmd)
        
    except Exception as e:
        logger.error(f"Error importing dump: {e}")
        raise

def verify_import(config):
    """Verify the import by checking table counts."""
    try:
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()
        
        # Get list of tables
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        
        logger.info(f"Found {len(tables)} tables in {config['database']}")
        
        # Check counts for key tables
        key_tables = ['patient', 'appointment', 'procedurelog', 'provider']
        for table in key_tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                logger.info(f"Table {table}: {count} rows")
            except Error:
                logger.warning(f"Table {table} not found or error counting rows")
        
        cursor.close()
        conn.close()
        
    except Error as e:
        logger.error(f"Error verifying import: {e}")
        raise

def main():
    """Main function to set up replication database."""
    if len(sys.argv) != 2:
        print("Usage: python setup_replication_db.py <path_to_dump_file>")
        sys.exit(1)
    
    dump_file = sys.argv[1]
    
    try:
        # Load configuration
        config = load_config()
        
        # Create database
        create_database(config)
        
        # Import dump
        import_dump(config, dump_file)
        
        # Verify import
        verify_import(config)
        
        logger.info("Replication database setup completed successfully")
        
    except Exception as e:
        logger.error(f"Setup failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 