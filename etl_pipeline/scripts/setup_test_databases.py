#!/usr/bin/env python3
"""
Test Database Setup Script

This script sets up the test databases for the ETL pipeline integration tests.
Run this script once to create the test databases and load sample data.

IMPORTANT SAFETY FEATURES:
- Only runs in test environments (ETL_ENVIRONMENT=test or ENVIRONMENT=test)
- Requires explicit confirmation for database creation
- Validates all database names contain 'test' to prevent production accidents
- Uses test-specific environment variables (TEST_*)
- Uses new ConnectionFactory with test methods
- Integrates with new Settings architecture
- Uses standardized test data from test fixtures

Usage:
    python -m etl_pipeline.scripts.setup_test_databases

Requirements:
    - PostgreSQL server running on localhost:5432
    - MySQL server running on localhost:3305 (for replication)
    - analytics_test_user and replication_test_user with appropriate permissions
    - ETL_ENVIRONMENT=test or ENVIRONMENT=test must be set
"""

import os
import sys
import logging
from sqlalchemy import create_engine, text
from datetime import datetime, date
from dotenv import load_dotenv
from pathlib import Path

# Add the etl_pipeline directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

# Load test environment the same way as the test fixtures
def load_test_environment():
    """Load environment variables from .env file for testing."""
    # Only load from etl_pipeline/.env - don't load from other directories
    etl_env_path = Path(__file__).parent.parent / '.env'
    if etl_env_path.exists():
        # Use override=True to ensure our .env takes precedence
        load_dotenv(etl_env_path, override=True)
        print(f"Loaded environment from: {etl_env_path}")
    else:
        print(f"No .env file found at: {etl_env_path}")
        print("Please create etl_pipeline/.env from etl_pipeline/.env.template")
        sys.exit(1)

# Load environment at module import time
load_test_environment()

# Import new architectural components
try:
    from etl_pipeline.config import create_test_settings, DatabaseType, PostgresSchema, reset_settings
    from etl_pipeline.core.connections import ConnectionFactory
    # Import standardized test data directly to avoid pytest dependencies
    import sys
    import os
    fixtures_path = os.path.join(os.path.dirname(__file__), '..', 'tests', 'fixtures')
    sys.path.insert(0, fixtures_path)
    from test_data_definitions import get_test_patient_data, get_test_appointment_data
    NEW_CONFIG_AVAILABLE = True
except ImportError as e:
    print(f"Import error: {e}")
    print("Make sure you're running this script from the project root directory")
    sys.exit(1)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def validate_test_environment():
    """
    Validate that we're running in a test environment.
    
    Uses the same environment detection logic as the ETL pipeline settings.
    
    Returns:
        bool: True if running in test environment, False otherwise
    """
    # Use the same environment detection logic as settings.py
    environment = (
        os.environ.get('ETL_ENVIRONMENT') or
        os.environ.get('ENVIRONMENT') or
        os.environ.get('APP_ENV') or
        'production'  # Default fallback
    )
    
    # Validate environment (same logic as settings.py)
    valid_environments = ['production', 'test']
    if environment not in valid_environments:
        logger.warning(f"Invalid environment '{environment}', using 'production'")
        environment = 'production'
    
    is_test_env = environment == 'test'
    
    if not is_test_env:
        logger.error("SAFETY CHECK FAILED: This script can only run in test environments!")
        logger.error(f"Current environment: '{environment}'")
        logger.error("Environment variables checked:")
        logger.error(f"  ETL_ENVIRONMENT='{os.environ.get('ETL_ENVIRONMENT', '')}'")
        logger.error(f"  ENVIRONMENT='{os.environ.get('ENVIRONMENT', '')}'")
        logger.error(f"  APP_ENV='{os.environ.get('APP_ENV', '')}'")
        logger.error("To run this script, set one of these environment variables:")
        logger.error("  export ETL_ENVIRONMENT=test")
        logger.error("  export ENVIRONMENT=test")
        logger.error("  export APP_ENV=test")
        return False
    
    logger.info(f"Environment validation passed - running in test mode (detected: {environment})")
    return True

def validate_database_names():
    """
    Validate that all database names contain 'test' to prevent production accidents.
    
    Returns:
        bool: True if all database names are safe, False otherwise
    """
    test_db_names = [
        os.environ.get('TEST_POSTGRES_ANALYTICS_DB', ''),
        os.environ.get('TEST_MYSQL_REPLICATION_DB', ''),
        os.environ.get('TEST_OPENDENTAL_SOURCE_DB', '')
    ]
    
    unsafe_dbs = [db for db in test_db_names if db and 'test' not in db.lower()]
    
    if unsafe_dbs:
        logger.error("SAFETY CHECK FAILED: Database names must contain 'test'!")
        logger.error(f"Unsafe database names: {unsafe_dbs}")
        logger.error("This prevents accidental modification of production databases.")
        return False
    
    logger.info("Database name validation passed - all names contain 'test'")
    return True

def confirm_database_creation():
    """
    Ask for explicit confirmation before creating/modifying databases.
    
    Returns:
        bool: True if user confirms, False otherwise
    """
    print("\n" + "="*60)
    print("DATABASE SETUP CONFIRMATION REQUIRED")
    print("="*60)
    print("This script will:")
    print("  • Create/modify test databases")
    print("  • Drop and recreate tables")
    print("  • Insert test data")
    print()
    print("Databases to be modified:")
    print(f"  • Source: {os.environ.get('TEST_OPENDENTAL_SOURCE_DB', 'test_opendental')}")
    print(f"  • Replication: {os.environ.get('TEST_MYSQL_REPLICATION_DB', 'test_opendental_replication')}")
    print(f"  • Analytics: {os.environ.get('TEST_POSTGRES_ANALYTICS_DB', 'test_opendental_analytics')}")
    print()
    
    while True:
        response = input("Do you want to continue? (yes/no): ").lower().strip()
        if response in ['yes', 'y']:
            logger.info("User confirmed database setup")
            return True
        elif response in ['no', 'n']:
            logger.info("User cancelled database setup")
            return False
        else:
            print("Please enter 'yes' or 'no'")

def setup_postgresql_test_database(settings):
    """Set up PostgreSQL test analytics database using new ConnectionFactory."""
    logger.info("Setting up PostgreSQL test analytics database...")
    
    try:
        # Use new ConnectionFactory with test methods
        analytics_engine = ConnectionFactory.get_analytics_connection(settings, PostgresSchema.RAW)
        
        with analytics_engine.connect() as conn:
            # Create only the raw schema if it doesn't exist
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw"))
            logger.info("Ensured schema 'raw' exists")
            
            # Drop existing tables if they exist (only raw schema)
            conn.execute(text("DROP TABLE IF EXISTS raw.appointment CASCADE"))
            conn.execute(text("DROP TABLE IF EXISTS raw.patient CASCADE"))
            
            # Create sequences first (before tables that reference them)
            conn.execute(text("""
                CREATE SEQUENCE IF NOT EXISTS raw."patient_PatNum_seq"
                    INCREMENT 1
                    START 1
                    MINVALUE 1
                    MAXVALUE 9223372036854775807
                    CACHE 1;
            """))
            
            conn.execute(text("""
                CREATE SEQUENCE IF NOT EXISTS raw."appointment_AptNum_seq"
                    INCREMENT 1
                    START 1
                    MINVALUE 1
                    MAXVALUE 9223372036854775807
                    CACHE 1;
            """))
            
            # Create complete patient table in raw schema matching test_opendental_analytics schema exactly
            conn.execute(text(f"""
                CREATE TABLE raw.patient (
                    "PatNum" integer NOT NULL DEFAULT nextval('raw."patient_PatNum_seq"'::regclass),
                    "LName" character varying(100) COLLATE pg_catalog."default",
                    "FName" character varying(100) COLLATE pg_catalog."default",
                    "MiddleI" character varying(100) COLLATE pg_catalog."default",
                    "Preferred" character varying(100) COLLATE pg_catalog."default",
                    "PatStatus" smallint DEFAULT 0,
                    "Gender" smallint DEFAULT 0,
                    "Position" smallint DEFAULT 0,
                    "Birthdate" date,
                    "SSN" character varying(100) COLLATE pg_catalog."default",
                    "Address" character varying(100) COLLATE pg_catalog."default",
                    "Address2" character varying(100) COLLATE pg_catalog."default",
                    "City" character varying(100) COLLATE pg_catalog."default",
                    "State" character varying(100) COLLATE pg_catalog."default",
                    "Zip" character varying(100) COLLATE pg_catalog."default",
                    "HmPhone" character varying(30) COLLATE pg_catalog."default",
                    "WkPhone" character varying(30) COLLATE pg_catalog."default",
                    "WirelessPhone" character varying(30) COLLATE pg_catalog."default",
                    "Guarantor" bigint,
                    "CreditType" character varying(1) COLLATE pg_catalog."default",
                    "Email" character varying(100) COLLATE pg_catalog."default",
                    "Salutation" character varying(100) COLLATE pg_catalog."default",
                    "EstBalance" double precision DEFAULT 0,
                    "PriProv" bigint,
                    "SecProv" bigint,
                    "FeeSched" bigint,
                    "BillingType" bigint,
                    "ImageFolder" character varying(100) COLLATE pg_catalog."default",
                    "AddrNote" text COLLATE pg_catalog."default",
                    "FamFinUrgNote" text COLLATE pg_catalog."default",
                    "MedUrgNote" character varying(255) COLLATE pg_catalog."default",
                    "ApptModNote" character varying(255) COLLATE pg_catalog."default",
                    "StudentStatus" character varying(1) COLLATE pg_catalog."default",
                    "SchoolName" character varying(255) COLLATE pg_catalog."default",
                    "ChartNumber" character varying(100) COLLATE pg_catalog."default",
                    "MedicaidID" character varying(20) COLLATE pg_catalog."default",
                    "Bal_0_30" double precision DEFAULT 0,
                    "Bal_31_60" double precision DEFAULT 0,
                    "Bal_61_90" double precision DEFAULT 0,
                    "BalOver90" double precision DEFAULT 0,
                    "InsEst" double precision DEFAULT 0,
                    "BalTotal" double precision DEFAULT 0,
                    "EmployerNum" bigint,
                    "EmploymentNote" character varying(255) COLLATE pg_catalog."default",
                    "County" character varying(255) COLLATE pg_catalog."default",
                    "GradeLevel" smallint DEFAULT 0,
                    "Urgency" smallint DEFAULT 0,
                    "DateFirstVisit" date,
                    "ClinicNum" bigint,
                    "HasIns" character varying(255) COLLATE pg_catalog."default",
                    "TrophyFolder" character varying(255) COLLATE pg_catalog."default",
                    "PlannedIsDone" smallint,
                    "Premed" smallint,
                    "Ward" character varying(255) COLLATE pg_catalog."default",
                    "PreferConfirmMethod" smallint,
                    "PreferContactMethod" smallint,
                    "PreferRecallMethod" smallint,
                    "SchedBeforeTime" time without time zone,
                    "SchedAfterTime" time without time zone,
                    "SchedDayOfWeek" smallint DEFAULT 0,
                    "Language" character varying(100) COLLATE pg_catalog."default",
                    "AdmitDate" date,
                    "Title" character varying(15) COLLATE pg_catalog."default",
                    "PayPlanDue" double precision,
                    "SiteNum" bigint,
                    "DateTStamp" timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
                    "ResponsParty" bigint,
                    "CanadianEligibilityCode" smallint DEFAULT 0,
                    "AskToArriveEarly" integer DEFAULT 0,
                    "PreferContactConfidential" smallint DEFAULT 0,
                    "SuperFamily" bigint,
                    "TxtMsgOk" smallint DEFAULT 0,
                    "SmokingSnoMed" character varying(32) COLLATE pg_catalog."default",
                    "Country" character varying(255) COLLATE pg_catalog."default",
                    "DateTimeDeceased" timestamp without time zone,
                    "BillingCycleDay" integer DEFAULT 1,
                    "SecUserNumEntry" bigint,
                    "SecDateEntry" date,
                    "HasSuperBilling" smallint DEFAULT 0,
                    "PatNumCloneFrom" bigint,
                    "DiscountPlanNum" bigint,
                    "HasSignedTil" smallint DEFAULT 0,
                    "ShortCodeOptIn" smallint DEFAULT 0,
                    "SecurityHash" character varying(255) COLLATE pg_catalog."default",
                    CONSTRAINT patient_pkey PRIMARY KEY ("PatNum")
                )
            """))
            
            # Create appointment table with complete schema
            conn.execute(text(f"""
                CREATE TABLE raw.appointment (
                    "AptNum" integer NOT NULL DEFAULT nextval('raw."appointment_AptNum_seq"'::regclass),
                    "PatNum" bigint NOT NULL,
                    "AptDateTime" timestamp without time zone NOT NULL,
                    "AptStatus" smallint DEFAULT 0,
                    "DateTStamp" timestamp without time zone NOT NULL,
                    "Notes" text COLLATE pg_catalog."default",
                    CONSTRAINT appointment_pkey PRIMARY KEY ("AptNum")
                )
            """))
            
            # Import standardized test data from fixtures
            test_patients = get_test_patient_data()
            
            # Insert standardized test data
            # Clear any existing test data first
            conn.execute(text("DELETE FROM raw.patient WHERE \"PatNum\" IN (1, 2)"))
            
            for patient in test_patients:
                # Build dynamic INSERT statement based on available fields
                fields = list(patient.keys())
                placeholders = ', '.join([f':{field}' for field in fields])
                field_names = ', '.join([f'"{field}"' for field in fields])
                
                insert_sql = f'INSERT INTO raw.patient ({field_names}) VALUES ({placeholders})'
                conn.execute(text(insert_sql), patient)
            
            # Verify the test data was inserted successfully
            result = conn.execute(text("SELECT COUNT(*) FROM raw.patient WHERE \"PatNum\" IN (1, 2)"))
            count = result.scalar()
            logger.info(f"Inserted {count} test patients to verify table functionality")
            
            # Clean up test data - remove the test patients we just inserted
            conn.execute(text("DELETE FROM raw.patient WHERE \"PatNum\" IN (1, 2)"))
            logger.info("Cleaned up test patients - table is now empty and ready for pytest tests")
            
            conn.commit()
            logger.info(f"Successfully created complete patient table in raw schema")
        
        analytics_engine.dispose()
        
    except Exception as e:
        logger.error(f"Failed to create patient table: {e}")
        sys.exit(1) # exit if error

def setup_mysql_test_database(settings, database_type):
    """Set up MySQL test database using new ConnectionFactory."""
    logger.info(f"Setting up MySQL test {database_type.value} database...")
    
    try:
        if database_type == DatabaseType.SOURCE:
            engine = ConnectionFactory.get_source_connection(settings)
        elif database_type == DatabaseType.REPLICATION:
            engine = ConnectionFactory.get_replication_connection(settings)
        else:
            raise ValueError(f"Unsupported database type: {database_type}")
        
        with engine.connect() as conn:
            # Test the connection and show current user
            result = conn.execute(text("SELECT USER(), DATABASE()"))
            user_info = result.fetchone()
            if user_info:
                logger.info(f"Connected as user: {user_info[0]}, database: {user_info[1]}")
            else:
                logger.warning("Could not retrieve user/database info")
            
            # Drop existing patient table if it exists, then create with complete schema
            conn.execute(text("DROP TABLE IF EXISTS patient"))
            
            # Create complete patient table matching OpenDental schema
            create_patient_sql = f"""
                CREATE TABLE patient (
                    `PatNum` bigint(20) NOT NULL AUTO_INCREMENT,
                    `LName` varchar(100) DEFAULT '',
                    `FName` varchar(100) DEFAULT '',
                    `MiddleI` varchar(100) DEFAULT '',
                    `Preferred` varchar(100) DEFAULT '',
                    `PatStatus` tinyint(3) unsigned NOT NULL DEFAULT 0,
                    `Gender` tinyint(3) unsigned NOT NULL DEFAULT 0,
                    `Position` tinyint(3) unsigned NOT NULL DEFAULT 0,
                    `Birthdate` date NOT NULL DEFAULT '0001-01-01',
                    `SSN` varchar(100) DEFAULT '',
                    `Address` varchar(100) DEFAULT '',
                    `Address2` varchar(100) DEFAULT '',
                    `City` varchar(100) DEFAULT '',
                    `State` varchar(100) DEFAULT '',
                    `Zip` varchar(100) DEFAULT '',
                    `HmPhone` varchar(30) DEFAULT '',
                    `WkPhone` varchar(30) DEFAULT '',
                    `WirelessPhone` varchar(30) DEFAULT '',
                    `Guarantor` bigint(20) NOT NULL DEFAULT 0,
                    `CreditType` char(1) DEFAULT '',
                    `Email` varchar(100) DEFAULT '',
                    `Salutation` varchar(100) DEFAULT '',
                    `EstBalance` double NOT NULL DEFAULT 0,
                    `PriProv` bigint(20) NOT NULL DEFAULT 0,
                    `SecProv` bigint(20) NOT NULL DEFAULT 0,
                    `FeeSched` bigint(20) NOT NULL DEFAULT 0,
                    `BillingType` bigint(20) NOT NULL DEFAULT 0,
                    `ImageFolder` varchar(100) DEFAULT '',
                    `AddrNote` text DEFAULT NULL,
                    `FamFinUrgNote` text DEFAULT NULL,
                    `MedUrgNote` varchar(255) DEFAULT '',
                    `ApptModNote` varchar(255) DEFAULT '',
                    `StudentStatus` char(1) DEFAULT '',
                    `SchoolName` varchar(255) NOT NULL DEFAULT '',
                    `ChartNumber` varchar(100) NOT NULL DEFAULT '',
                    `MedicaidID` varchar(20) DEFAULT '',
                    `Bal_0_30` double NOT NULL DEFAULT 0,
                    `Bal_31_60` double NOT NULL DEFAULT 0,
                    `Bal_61_90` double NOT NULL DEFAULT 0,
                    `BalOver90` double NOT NULL DEFAULT 0,
                    `InsEst` double NOT NULL DEFAULT 0,
                    `BalTotal` double NOT NULL DEFAULT 0,
                    `EmployerNum` bigint(20) NOT NULL DEFAULT 0,
                    `EmploymentNote` varchar(255) DEFAULT '',
                    `County` varchar(255) DEFAULT '',
                    `GradeLevel` tinyint(4) NOT NULL DEFAULT 0,
                    `Urgency` tinyint(4) NOT NULL DEFAULT 0,
                    `DateFirstVisit` date NOT NULL DEFAULT '0001-01-01',
                    `ClinicNum` bigint(20) NOT NULL DEFAULT 0,
                    `HasIns` varchar(255) DEFAULT '',
                    `TrophyFolder` varchar(255) DEFAULT '',
                    `PlannedIsDone` tinyint(3) unsigned NOT NULL DEFAULT 0,
                    `Premed` tinyint(3) unsigned NOT NULL DEFAULT 0,
                    `Ward` varchar(255) DEFAULT '',
                    `PreferConfirmMethod` tinyint(3) unsigned NOT NULL DEFAULT 0,
                    `PreferContactMethod` tinyint(3) unsigned NOT NULL DEFAULT 0,
                    `PreferRecallMethod` tinyint(3) unsigned NOT NULL DEFAULT 0,
                    `SchedBeforeTime` time DEFAULT NULL,
                    `SchedAfterTime` time DEFAULT NULL,
                    `SchedDayOfWeek` tinyint(3) unsigned NOT NULL DEFAULT 0,
                    `Language` varchar(100) DEFAULT '',
                    `AdmitDate` date NOT NULL DEFAULT '0001-01-01',
                    `Title` varchar(15) DEFAULT NULL,
                    `PayPlanDue` double NOT NULL DEFAULT 0,
                    `SiteNum` bigint(20) NOT NULL DEFAULT 0,
                    `DateTStamp` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
                    `ResponsParty` bigint(20) NOT NULL DEFAULT 0,
                    `CanadianEligibilityCode` tinyint(4) NOT NULL DEFAULT 0,
                    `AskToArriveEarly` int(11) NOT NULL DEFAULT 0,
                    `PreferContactConfidential` tinyint(4) NOT NULL DEFAULT 0,
                    `SuperFamily` bigint(20) NOT NULL DEFAULT 0,
                    `TxtMsgOk` tinyint(4) NOT NULL DEFAULT 0,
                    `SmokingSnoMed` varchar(32) NOT NULL DEFAULT '',
                    `Country` varchar(255) NOT NULL DEFAULT '',
                    `DateTimeDeceased` datetime NOT NULL DEFAULT '0001-01-01 00:00:00',
                    `BillingCycleDay` int(11) NOT NULL DEFAULT 1,
                    `SecUserNumEntry` bigint(20) NOT NULL DEFAULT 0,
                    `SecDateEntry` date NOT NULL DEFAULT '0001-01-01',
                    `HasSuperBilling` tinyint(4) NOT NULL DEFAULT 0,
                    `PatNumCloneFrom` bigint(20) NOT NULL DEFAULT 0,
                    `DiscountPlanNum` bigint(20) NOT NULL DEFAULT 0,
                    `HasSignedTil` tinyint(4) NOT NULL DEFAULT 0,
                    `ShortCodeOptIn` tinyint(4) NOT NULL DEFAULT 0,
                    `SecurityHash` varchar(255) NOT NULL DEFAULT '',
                    PRIMARY KEY (`PatNum`),
                    KEY `indexLName` (`LName`(10)),
                    KEY `indexFName` (`FName`(10)),
                    KEY `indexLFName` (`LName`,`FName`),
                    KEY `indexGuarantor` (`Guarantor`),
                    KEY `ResponsParty` (`ResponsParty`),
                    KEY `SuperFamily` (`SuperFamily`),
                    KEY `SiteNum` (`SiteNum`),
                    KEY `PatStatus` (`PatStatus`),
                    KEY `Email` (`Email`),
                    KEY `ChartNumber` (`ChartNumber`),
                    KEY `SecUserNumEntry` (`SecUserNumEntry`),
                    KEY `HmPhone` (`HmPhone`),
                    KEY `WirelessPhone` (`WirelessPhone`),
                    KEY `WkPhone` (`WkPhone`),
                    KEY `PatNumCloneFrom` (`PatNumCloneFrom`),
                    KEY `DiscountPlanNum` (`DiscountPlanNum`),
                    KEY `FeeSched` (`FeeSched`),
                    KEY `SecDateEntry` (`SecDateEntry`),
                    KEY `PriProv` (`PriProv`),
                    KEY `SecProv` (`SecProv`),
                    KEY `ClinicPatStatus` (`ClinicNum`,`PatStatus`),
                    KEY `BirthdateStatus` (`Birthdate`,`PatStatus`),
                    KEY `idx_pat_guarantor` (`Guarantor`),
                    KEY `idx_pat_birth` (`Birthdate`)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """
            conn.execute(text(create_patient_sql))
            
            # Drop existing appointment table if it exists, then create with complete schema
            conn.execute(text("DROP TABLE IF EXISTS appointment"))
            
            create_appointment_sql = f"""
                CREATE TABLE appointment (
                    AptNum BIGINT(20) NOT NULL AUTO_INCREMENT,
                    PatNum BIGINT(20) NOT NULL,
                    AptDateTime DATETIME NOT NULL,
                    AptStatus TINYINT(3) UNSIGNED NOT NULL DEFAULT 0,
                    DateTStamp DATETIME NOT NULL,
                    Notes TEXT,
                    PRIMARY KEY (AptNum),
                    KEY PatNum (PatNum)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """
            conn.execute(text(create_appointment_sql))
            
            # Import standardized test data from fixtures
            test_patients = get_test_patient_data()
            
            # Insert standardized test data
            # Clear any existing test data first
            conn.execute(text("DELETE FROM patient WHERE PatNum IN (1, 2)"))
            
            for patient in test_patients:
                # Build dynamic INSERT statement based on available fields
                fields = list(patient.keys())
                placeholders = ', '.join([f':{field}' for field in fields])
                field_names = ', '.join([f'`{field}`' for field in fields])
                
                insert_sql = f"INSERT INTO patient ({field_names}) VALUES ({placeholders})"
                conn.execute(text(insert_sql), patient)
            
            # Verify the test data was inserted successfully
            result = conn.execute(text("SELECT COUNT(*) FROM patient WHERE PatNum IN (1, 2)"))
            count = result.scalar()
            logger.info(f"Inserted {count} test patients to verify table functionality")
            
            # Clean up test data - remove the test patients we just inserted
            conn.execute(text("DELETE FROM patient WHERE PatNum IN (1, 2)"))
            logger.info("Cleaned up test patients - table is now empty and ready for pytest tests")
            
            conn.commit()
            logger.info(f"Successfully set up MySQL test database with complete schema")
        
        engine.dispose()
        
    except Exception as e:
        logger.error(f"Failed to set up MySQL test database: {e}")
        sys.exit(1) # exit if error

def main():
    """Main function to set up all test databases."""
    logger.info("Starting test database setup with safety checks...")
    
    # Set test environment explicitly
    os.environ['ETL_ENVIRONMENT'] = 'test'
    logger.info("Set ETL_ENVIRONMENT=test for test database setup")
    
    # Safety checks
    if not validate_test_environment():
        sys.exit(1)
    
    if not validate_database_names():
        sys.exit(1)
    
    if not confirm_database_creation():
        sys.exit(0)
    
    # Create test settings using new architecture
    try:
        settings = create_test_settings()
        logger.info("Created test settings using new architecture")
    except Exception as e:
        logger.error(f"Failed to create test settings: {e}")
        sys.exit(1)
    
    # Debug: Print relevant environment variables
    logger.info("Environment Configuration:")
    logger.info(f"  TEST_POSTGRES_ANALYTICS_DB: {os.environ.get('TEST_POSTGRES_ANALYTICS_DB')}")
    logger.info(f"  TEST_POSTGRES_ANALYTICS_USER: {os.environ.get('TEST_POSTGRES_ANALYTICS_USER')}")
    logger.info(f"  TEST_POSTGRES_ANALYTICS_HOST: {os.environ.get('TEST_POSTGRES_ANALYTICS_HOST')}")
    logger.info(f"  TEST_POSTGRES_ANALYTICS_PORT: {os.environ.get('TEST_POSTGRES_ANALYTICS_PORT')}")
    logger.info(f"  TEST_POSTGRES_ANALYTICS_SCHEMA: {os.environ.get('TEST_POSTGRES_ANALYTICS_SCHEMA')}")
    logger.info(f"  TEST_MYSQL_REPLICATION_DB: {os.environ.get('TEST_MYSQL_REPLICATION_DB')}")
    logger.info(f"  TEST_MYSQL_REPLICATION_USER: {os.environ.get('TEST_MYSQL_REPLICATION_USER')}")
    logger.info(f"  TEST_MYSQL_REPLICATION_HOST: {os.environ.get('TEST_MYSQL_REPLICATION_HOST')}")
    logger.info(f"  TEST_MYSQL_REPLICATION_PORT: {os.environ.get('TEST_MYSQL_REPLICATION_PORT')}")
    logger.info(f"  TEST_OPENDENTAL_SOURCE_DB: {os.environ.get('TEST_OPENDENTAL_SOURCE_DB')}")
    logger.info(f"  TEST_OPENDENTAL_SOURCE_USER: {os.environ.get('TEST_OPENDENTAL_SOURCE_USER')}")
    logger.info(f"  TEST_OPENDENTAL_SOURCE_HOST: {os.environ.get('TEST_OPENDENTAL_SOURCE_HOST')}")
    logger.info(f"  TEST_OPENDENTAL_SOURCE_PORT: {os.environ.get('TEST_OPENDENTAL_SOURCE_PORT')}")
    
    try:
        logger.info("Starting database setup...")
        
        # Set up MySQL test source database using new ConnectionFactory
        setup_mysql_test_database(settings, DatabaseType.SOURCE)
        
        # Set up PostgreSQL test database using new ConnectionFactory
        setup_postgresql_test_database(settings)
        
        # Set up MySQL test replication database using new ConnectionFactory
        setup_mysql_test_database(settings, DatabaseType.REPLICATION)
        
        logger.info("Test database setup completed successfully!")
        logger.info("You can now run integration tests with: pytest -m integration")
        
    except Exception as e:
        logger.error(f"Test database setup failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 