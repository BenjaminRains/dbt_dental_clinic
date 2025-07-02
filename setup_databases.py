#!/usr/bin/env python3
"""
Test Database Setup Script

This script sets up the test databases for the ETL pipeline integration tests.
Run this script once to create the test databases and load sample data.

Usage:
    python setup_test_databases.py

Requirements:
    - PostgreSQL server running on localhost:5432
    - MySQL server running on localhost:3305 (for replication)
    - analytics_test_user and replication_test_user with appropriate permissions
"""

import os
import sys
import logging
from sqlalchemy import create_engine, text
from datetime import datetime
from dotenv import load_dotenv

# Add the project root to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables from .env file in the same directory as .env.template
base_dir = os.path.dirname(os.path.abspath(__file__))
env_path = os.path.join(base_dir, '..', '.env')
load_dotenv(dotenv_path=env_path)

if os.path.exists(env_path):
    logger.info(f"Loaded .env file from: {env_path}")
else:
    logger.warning(f".env file not found at: {env_path}")

logger.info(f"POSTGRES_ANALYTICS_TEST_DB from env: {os.environ.get('POSTGRES_ANALYTICS_TEST_DB')}")

def setup_postgresql_test_database():
    """Set up PostgreSQL test analytics database using environment variables."""
    logger.info("Setting up PostgreSQL test analytics database...")
    
    # Database configuration from environment variables
    config = {
        'host': os.environ.get('POSTGRES_ANALYTICS_TEST_HOST', 'localhost'),
        'port': int(os.environ.get('POSTGRES_ANALYTICS_TEST_PORT', 5432)),
        'database': os.environ.get('POSTGRES_ANALYTICS_TEST_DB', 'opendental_analytics_test'),
        'user': os.environ.get('POSTGRES_ANALYTICS_TEST_USER', 'analytics_test_user'),
        'password': os.environ.get('POSTGRES_ANALYTICS_TEST_PASSWORD', 'test_password'),
        'schema': os.environ.get('POSTGRES_ANALYTICS_TEST_SCHEMA', 'raw')
    }
    
    try:
        # Connect to PostgreSQL server (not specific database)
        admin_connection_string = (
            f"postgresql+psycopg2://{config['user']}:{config['password']}"
            f"@{config['host']}:{config['port']}/postgres"
        )
        
        admin_engine = create_engine(admin_connection_string)
        
        with admin_engine.connect() as conn:
            # Check if database exists
            result = conn.execute(text("""
                SELECT 1 FROM pg_database WHERE datname = :db_name
            """), {'db_name': config['database']})
            
            if not result.fetchone():
                # Create database if it doesn't exist
                conn.execute(text(f"CREATE DATABASE {config['database']}"))
                logger.info(f"Created test database: {config['database']}")
            else:
                logger.info(f"Test database {config['database']} already exists")
        
        # Connect to the test database
        test_connection_string = (
            f"postgresql+psycopg2://{config['user']}:{config['password']}"
            f"@{config['host']}:{config['port']}/{config['database']}"
        )
        
        test_engine = create_engine(test_connection_string)

        with test_engine.connect() as conn:
            # Create all required schemas if they don't exist
            schemas = ['public', 'public_staging', 'public_intermediate', 'public_marts', 'raw']
            for schema in schemas:
                conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
                logger.info(f"Ensured schema '{schema}' exists")
            
            # Create test tables in raw schema
            conn.execute(text(f"""
                CREATE TABLE IF NOT EXISTS raw.patient (
                    "PatNum" INTEGER PRIMARY KEY,
                    "LName" VARCHAR(100) DEFAULT '',
                    "FName" VARCHAR(100) DEFAULT '',
                    "MiddleI" VARCHAR(100) DEFAULT '',
                    "Preferred" VARCHAR(100) DEFAULT '',
                    "PatStatus" SMALLINT DEFAULT 0,
                    "Gender" SMALLINT DEFAULT 0,
                    "Position" SMALLINT DEFAULT 0,
                    "Birthdate" DATE,
                    "SSN" VARCHAR(100) DEFAULT ''
                )
            """))
            
            conn.execute(text(f"""
                CREATE TABLE IF NOT EXISTS raw.appointment (
                    "AptNum" SERIAL PRIMARY KEY,
                    "PatNum" INTEGER NOT NULL,
                    "AptDateTime" TIMESTAMP NOT NULL,
                    "AptStatus" SMALLINT DEFAULT 0,
                    "DateTStamp" TIMESTAMP NOT NULL,
                    "Notes" TEXT
                )
            """))
            
            conn.execute(text(f"""
                CREATE TABLE IF NOT EXISTS raw.procedure (
                    "ProcNum" SERIAL PRIMARY KEY,
                    "PatNum" INTEGER NOT NULL,
                    "ProcDate" DATE NOT NULL,
                    "ProcCode" VARCHAR(100) DEFAULT '',
                    "ProcFee" DECIMAL(10,2) DEFAULT 0.00,
                    "DateTStamp" TIMESTAMP NOT NULL
                )
            """))
            
            # Insert sample data
            # Clear any existing test data first
            conn.execute(text("DELETE FROM raw.patient WHERE \"PatNum\" IN (1, 2, 3)"))
            
            test_patients = [
                (1, 'Doe', 'John', 'M', 'Johnny', 0, 0, 0, '1980-01-01', '123-45-6789'),
                (2, 'Smith', 'Jane', 'A', 'Janey', 0, 1, 0, '1985-05-15', '234-56-7890'),
                (3, 'Johnson', 'Bob', 'R', 'Bobby', 0, 0, 0, '1975-12-10', '345-67-8901')
            ]
            
            for patient in test_patients:
                conn.execute(text(f"""
                    INSERT INTO raw.patient VALUES (
                        :patnum, :lname, :fname, :middlei, :preferred, :patstatus, :gender, :position, :birthdate, :ssn
                    )
                """), {
                    'patnum': patient[0], 'lname': patient[1], 'fname': patient[2], 'middlei': patient[3], 
                    'preferred': patient[4], 'patstatus': patient[5], 'gender': patient[6], 'position': patient[7],
                    'birthdate': patient[8], 'ssn': patient[9]
                })
            
            conn.commit()
            logger.info(f"Successfully created patient table in raw schema of {config['database']}")
        
        admin_engine.dispose()
        test_engine.dispose()
        
    except Exception as e:
        logger.error(f"Failed to create patient table in {config['database']}: {e}")
        sys.exit(1) # exit if error


def setup_mysql_test_database():
    """Set up MySQL test replication database using environment variables."""
    logger.info("Setting up MySQL test replication database...")
    
    # Database configuration from environment variables
    config = {
        'host': os.environ.get('MYSQL_REPLICATION_TEST_HOST', 'localhost'),
        'port': int(os.environ.get('MYSQL_REPLICATION_TEST_PORT', 3305)),
        'database': os.environ.get('MYSQL_REPLICATION_TEST_DB', 'opendental_replication_test'),
        'user': os.environ.get('MYSQL_REPLICATION_TEST_USER', 'replication_test_user'),
        'password': os.environ.get('MYSQL_REPLICATION_TEST_PASSWORD', 'test_password')
    }
    
    try:
        # Connect to MySQL server (not specific database)
        admin_connection_string = (
            f"mysql+pymysql://{config['user']}:{config['password']}"
            f"@{config['host']}:{config['port']}/"
        )
        
        admin_engine = create_engine(admin_connection_string)
        
        with admin_engine.connect() as conn:
            # Check if database exists
            result = conn.execute(text("""
                SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA 
                WHERE SCHEMA_NAME = :db_name
            """), {'db_name': config['database']})
            
            if not result.fetchone():
                # Create database if it doesn't exist
                conn.execute(text(f"CREATE DATABASE {config['database']}"))
                logger.info(f"Created test replication database: {config['database']}")
            else:
                logger.info(f"Test replication database {config['database']} already exists")
        
        # Connect to the test database
        test_connection_string = (
            f"mysql+pymysql://{config['user']}:{config['password']}"
            f"@{config['host']}:{config['port']}/{config['database']}"
        )
        
        test_engine = create_engine(test_connection_string)
        
        with test_engine.connect() as conn:
            # Create tables
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS patient (
                    PatNum BIGINT(20) NOT NULL AUTO_INCREMENT,
                    LName VARCHAR(100) DEFAULT '',
                    FName VARCHAR(100) DEFAULT '',
                    MiddleI VARCHAR(100) DEFAULT '',
                    Preferred VARCHAR(100) DEFAULT '',
                    PatStatus TINYINT(3) UNSIGNED NOT NULL DEFAULT 0,
                    Gender TINYINT(3) UNSIGNED NOT NULL DEFAULT 0,
                    Position TINYINT(3) UNSIGNED NOT NULL DEFAULT 0,
                    Birthdate DATE NOT NULL DEFAULT '0001-01-01',
                    SSN VARCHAR(100) DEFAULT '',
                    PRIMARY KEY (PatNum)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
            """))
            
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS appointment (
                    AptNum BIGINT(20) NOT NULL AUTO_INCREMENT,
                    PatNum BIGINT(20) NOT NULL,
                    AptDateTime DATETIME NOT NULL,
                    AptStatus TINYINT(3) UNSIGNED NOT NULL DEFAULT 0,
                    DateTStamp DATETIME NOT NULL,
                    Notes TEXT,
                    PRIMARY KEY (AptNum),
                    KEY PatNum (PatNum)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
            """))
            
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS `procedure` (
                    ProcNum BIGINT(20) NOT NULL AUTO_INCREMENT,
                    PatNum BIGINT(20) NOT NULL,
                    ProcDate DATE NOT NULL,
                    ProcCode VARCHAR(100) DEFAULT '',
                    ProcFee DECIMAL(10,2) DEFAULT 0.00,
                    DateTStamp DATETIME NOT NULL,
                    PRIMARY KEY (ProcNum),
                    KEY PatNum (PatNum)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
            """))
            
            # Insert sample data
            # Clear any existing test data first
            conn.execute(text("DELETE FROM patient WHERE PatNum IN (1, 2, 3)"))
            
            test_patients = [
                (1, 'Doe', 'John', 'M', 'Johnny', 0, 0, 0, '1980-01-01', '123-45-6789'),
                (2, 'Smith', 'Jane', 'A', 'Janey', 0, 1, 0, '1985-05-15', '234-56-7890'),
                (3, 'Johnson', 'Bob', 'R', 'Bobby', 0, 0, 0, '1975-12-10', '345-67-8901')
            ]
            
            for patient in test_patients:
                conn.execute(text("""
                    INSERT INTO patient (
                        PatNum, LName, FName, MiddleI, Preferred, PatStatus, Gender, Position, Birthdate, SSN
                    ) VALUES (
                        :patnum, :lname, :fname, :middlei, :preferred, :patstatus, :gender, :position, :birthdate, :ssn
                    )
                """), {
                    'patnum': patient[0], 'lname': patient[1], 'fname': patient[2], 'middlei': patient[3], 
                    'preferred': patient[4], 'patstatus': patient[5], 'gender': patient[6], 'position': patient[7],
                    'birthdate': patient[8], 'ssn': patient[9]
                })
            
            conn.commit()
            logger.info(f"Successfully set up MySQL test database: {config['database']}")
        
        admin_engine.dispose()
        test_engine.dispose()
        
    except Exception as e:
        logger.error(f"Failed to set up MySQL test database: {e}")
        sys.exit(1) # exit if error


def main():
    """Main function to set up all test databases."""
    logger.info("Starting test database setup...")
    # Debug: Print relevant environment variables
    logger.info(f"POSTGRES_ANALYTICS_TEST_DB: {os.environ.get('POSTGRES_ANALYTICS_TEST_DB')}")
    logger.info(f"POSTGRES_ANALYTICS_TEST_USER: {os.environ.get('POSTGRES_ANALYTICS_TEST_USER')}")
    logger.info(f"POSTGRES_ANALYTICS_TEST_HOST: {os.environ.get('POSTGRES_ANALYTICS_TEST_HOST')}")
    logger.info(f"POSTGRES_ANALYTICS_TEST_PORT: {os.environ.get('POSTGRES_ANALYTICS_TEST_PORT')}")
    logger.info(f"POSTGRES_ANALYTICS_TEST_SCHEMA: {os.environ.get('POSTGRES_ANALYTICS_TEST_SCHEMA')}")
    logger.info(f"MYSQL_REPLICATION_TEST_DB: {os.environ.get('MYSQL_REPLICATION_TEST_DB')}")
    logger.info(f"MYSQL_REPLICATION_TEST_USER: {os.environ.get('MYSQL_REPLICATION_TEST_USER')}")
    logger.info(f"MYSQL_REPLICATION_TEST_HOST: {os.environ.get('MYSQL_REPLICATION_TEST_HOST')}")
    logger.info(f"MYSQL_REPLICATION_TEST_PORT: {os.environ.get('MYSQL_REPLICATION_TEST_PORT')}")
    try:
        # Set up PostgreSQL test database
        setup_postgresql_test_database()
        
        # Set up MySQL test database
        setup_mysql_test_database()
        
        logger.info("Test database setup completed successfully!")
        logger.info("You can now run integration tests with: pytest -m integration")
        
    except Exception as e:
        logger.error(f"Test database setup failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 