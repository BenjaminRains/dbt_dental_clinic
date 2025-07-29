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
- Uses new ConnectionFactory with unified API
- Integrates with new Settings architecture
- Uses standardized test data from test fixtures
- Includes ping checks before MySQL connections

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
import subprocess
import platform
from sqlalchemy import create_engine, text
from datetime import datetime, date
from dotenv import load_dotenv
from pathlib import Path

# Add the etl_pipeline directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

# Load test environment the same way as the test fixtures
def load_test_environment():
    """Load environment variables from .env_test file for testing."""
    etl_env_path = Path(__file__).parent.parent / '.env_test'
    if etl_env_path.exists():
        load_dotenv(etl_env_path, override=True)
        print(f"Loaded environment from: {etl_env_path}")
    else:
        print(f"No .env_test file found at: {etl_env_path}")
        print("Please create etl_pipeline/.env_test from etl_pipeline/docs/env_test.template")
        sys.exit(1)

# Load environment at module import time
load_test_environment()

# Import new architectural components
try:
    from etl_pipeline.config import create_test_settings, DatabaseType, PostgresSchema, reset_settings, get_settings
    from etl_pipeline.core.connections import ConnectionFactory
    # Import standardized test data directly to avoid pytest dependencies
    import sys
    import os
    fixtures_path = os.path.join(os.path.dirname(__file__), '..', 'tests', 'fixtures')
    sys.path.insert(0, fixtures_path)
    from test_data_definitions import get_test_patient_data, get_test_appointment_data, get_test_procedure_data  # type: ignore
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
    
    Only checks ETL_ENVIRONMENT variable and fails if not set to 'test'.
    
    Returns:
        bool: True if running in test environment, False otherwise
    """
    environment = os.environ.get('ETL_ENVIRONMENT')
    
    if not environment:
        logger.error("SAFETY CHECK FAILED: ETL_ENVIRONMENT environment variable is not set!")
        logger.error("To run this script, set:")
        logger.error("  export ETL_ENVIRONMENT=test")
        return False
    
    if environment != 'test':
        logger.error("SAFETY CHECK FAILED: ETL_ENVIRONMENT must be set to 'test'!")
        logger.error(f"Current ETL_ENVIRONMENT: '{environment}'")
        logger.error("To run this script, set:")
        logger.error("  export ETL_ENVIRONMENT=test")
        return False
    
    logger.info(f"Environment validation passed - running in test mode (ETL_ENVIRONMENT={environment})")
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

def ping_mysql_server(host, port):
    """
    Pings the MySQL server to ensure it's running and accessible.
    
    Args:
        host (str): MySQL server hostname or IP address
        port (str): MySQL server port
        
    Returns:
        bool: True if ping successful, False otherwise
    """
    try:
        # Use a simple ping command (ping only the host, not the port)
        if platform.system() == "Windows":
            # Windows uses 'ping' command
            result = subprocess.run(["ping", "-n", "1", host], 
                                  capture_output=True, text=True, timeout=10)
        else:
            # Linux/macOS uses 'ping' command
            result = subprocess.run(["ping", "-c", "1", host], 
                                  capture_output=True, text=True, timeout=10)
        
        if result.returncode == 0:
            logger.info(f"Successfully pinged MySQL server host {host}")
            return True
        else:
            logger.error(f"Failed to ping MySQL server host {host}. Output: {result.stdout}")
            return False
            
    except subprocess.TimeoutExpired:
        logger.error(f"Ping to MySQL server host {host} timed out after 10 seconds")
        return False
    except Exception as e:
        logger.error(f"Error pinging MySQL server host {host}: {e}")
        return False

def setup_postgresql_test_database():
    """Set up PostgreSQL test analytics database using new ConnectionFactory."""
    logger.info("Setting up PostgreSQL test analytics database...")
    
    try:
        # Use new ConnectionFactory with unified API
        settings = get_settings()
        analytics_engine = ConnectionFactory.get_analytics_raw_connection(settings)
        
        with analytics_engine.connect() as conn:
            # Create only the raw schema if it doesn't exist
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw"))
            logger.info("Ensured schema 'raw' exists")
            
            # Drop existing tables if they exist (only raw schema)
            conn.execute(text("DROP TABLE IF EXISTS raw.appointment CASCADE"))
            conn.execute(text("DROP TABLE IF EXISTS raw.patient CASCADE"))
            conn.execute(text("DROP TABLE IF EXISTS raw.procedurelog CASCADE"))
            
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
            
            conn.execute(text("""
                CREATE SEQUENCE IF NOT EXISTS raw."procedurelog_ProcNum_seq"
                    INCREMENT 1
                    START 1
                    MINVALUE 1
                    MAXVALUE 9223372036854775807
                    CACHE 1;
            """))
            
            # Create complete patient table in raw schema with correct column types to match ETL expectations
            conn.execute(text(f"""
                CREATE TABLE raw.patient (
                    "PatNum" bigint NOT NULL DEFAULT nextval('raw."patient_PatNum_seq"'::regclass),
                    "LName" character varying(100) COLLATE pg_catalog."default",
                    "FName" character varying(100) COLLATE pg_catalog."default",
                    "MiddleI" character varying(100) COLLATE pg_catalog."default",
                    "Preferred" character varying(100) COLLATE pg_catalog."default",
                    "PatStatus" boolean DEFAULT false,
                    "Gender" boolean DEFAULT false,
                    "Position" boolean DEFAULT false,
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
                    "CreditType" character(1) COLLATE pg_catalog."default",
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
                    "StudentStatus" character(1) COLLATE pg_catalog."default",
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
                    "GradeLevel" boolean DEFAULT false,
                    "Urgency" boolean DEFAULT false,
                    "DateFirstVisit" date,
                    "ClinicNum" bigint,
                    "HasIns" character varying(255) COLLATE pg_catalog."default",
                    "TrophyFolder" character varying(255) COLLATE pg_catalog."default",
                    "PlannedIsDone" boolean DEFAULT false,
                    "Premed" boolean DEFAULT false,
                    "Ward" character varying(255) COLLATE pg_catalog."default",
                    "PreferConfirmMethod" boolean DEFAULT false,
                    "PreferContactMethod" boolean DEFAULT false,
                    "PreferRecallMethod" boolean DEFAULT false,
                    "SchedBeforeTime" time without time zone,
                    "SchedAfterTime" time without time zone,
                    "SchedDayOfWeek" boolean DEFAULT false,
                    "Language" character varying(100) COLLATE pg_catalog."default",
                    "AdmitDate" date,
                    "Title" character varying(15) COLLATE pg_catalog."default",
                    "PayPlanDue" double precision,
                    "SiteNum" bigint,
                    "DateTStamp" timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
                    "ResponsParty" bigint,
                    "CanadianEligibilityCode" boolean DEFAULT false,
                    "AskToArriveEarly" integer DEFAULT 0,
                    "PreferContactConfidential" boolean DEFAULT false,
                    "SuperFamily" bigint,
                    "TxtMsgOk" boolean DEFAULT false,
                    "SmokingSnoMed" character varying(32) COLLATE pg_catalog."default",
                    "Country" character varying(255) COLLATE pg_catalog."default",
                    "DateTimeDeceased" timestamp without time zone,
                    "BillingCycleDay" integer DEFAULT 1,
                    "SecUserNumEntry" bigint,
                    "SecDateEntry" date,
                    "HasSuperBilling" boolean DEFAULT false,
                    "PatNumCloneFrom" bigint,
                    "DiscountPlanNum" bigint,
                    "HasSignedTil" boolean DEFAULT false,
                    "ShortCodeOptIn" boolean DEFAULT false,
                    "SecurityHash" character varying(255) COLLATE pg_catalog."default",
                    CONSTRAINT patient_pkey PRIMARY KEY ("PatNum")
                )
            """))
            
            # Create appointment table with correct column types to match ETL expectations
            conn.execute(text(f"""
                CREATE TABLE raw.appointment (
                    "AptNum" bigint NOT NULL DEFAULT nextval('raw."appointment_AptNum_seq"'::regclass),
                    "PatNum" bigint NOT NULL,
                    "AptDateTime" timestamp without time zone NOT NULL,
                    "AptStatus" boolean DEFAULT false,
                    "DateTStamp" timestamp without time zone NOT NULL,
                    "Notes" text COLLATE pg_catalog."default",
                    CONSTRAINT appointment_pkey PRIMARY KEY ("AptNum")
                )
            """))
            
            # Create procedurelog table with correct column types to match ETL expectations
            conn.execute(text(f"""
                CREATE TABLE raw.procedurelog (
                    "ProcNum" bigint NOT NULL DEFAULT nextval('raw."procedurelog_ProcNum_seq"'::regclass),
                    "PatNum" bigint NOT NULL,
                    "AptNum" bigint NOT NULL,
                    "ProcStatus" boolean DEFAULT false,
                    "ProcFee" decimal(10,2) NOT NULL DEFAULT 0.00,
                    "ProcFeeCur" decimal(10,2) NOT NULL DEFAULT 0.00,
                    "ProcDate" date NOT NULL,
                    "CodeNum" bigint NOT NULL DEFAULT 0,
                    "ProcNote" text COLLATE pg_catalog."default",
                    "DateTStamp" timestamp without time zone NOT NULL,
                    "SecDateEntry" timestamp without time zone NOT NULL DEFAULT '0001-01-01 00:00:00',
                    CONSTRAINT procedurelog_pkey PRIMARY KEY ("ProcNum")
                )
            """))
            
            # Clear ALL existing data from analytics database (leave empty for ETL testing)
            conn.execute(text("DELETE FROM raw.patient"))
            conn.execute(text("DELETE FROM raw.appointment"))
            conn.execute(text("DELETE FROM raw.procedurelog"))
            
            logger.info("Analytics database tables created and cleared - ready for ETL pipeline testing")
            
            conn.commit()
            logger.info(f"Successfully set up PostgreSQL analytics database with empty tables")
        
        analytics_engine.dispose()
        
    except Exception as e:
        logger.error(f"Failed to set up PostgreSQL analytics database: {e}")
        sys.exit(1) # exit if error

def setup_mysql_test_database(database_type):
    """Set up MySQL test database using new ConnectionFactory."""
    logger.info(f"Setting up MySQL test {database_type.value} database...")
    
    try:
        settings = get_settings()
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
            logger.info(f"Successfully created patient table in {database_type.value} database")
            
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
            logger.info(f"Successfully created appointment table in {database_type.value} database")
            
            # Drop existing procedurelog table if it exists, then create with complete schema
            conn.execute(text("DROP TABLE IF EXISTS procedurelog"))
            
            create_procedurelog_sql = f"""
                CREATE TABLE procedurelog (
                    ProcNum BIGINT(20) NOT NULL AUTO_INCREMENT,
                    PatNum BIGINT(20) NOT NULL,
                    AptNum BIGINT(20) NOT NULL,
                    ProcStatus TINYINT(3) UNSIGNED NOT NULL DEFAULT 0,
                    ProcFee DECIMAL(10,2) NOT NULL DEFAULT 0.00,
                    ProcFeeCur DECIMAL(10,2) NOT NULL DEFAULT 0.00,
                    ProcDate DATE NOT NULL,
                    CodeNum BIGINT(20) NOT NULL DEFAULT 0,
                    ProcNote TEXT,
                    DateTStamp DATETIME NOT NULL,
                    SecDateEntry DATE NOT NULL DEFAULT '0001-01-01',
                    PRIMARY KEY (ProcNum),
                    KEY PatNum (PatNum),
                    KEY AptNum (AptNum),
                    KEY CodeNum (CodeNum)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """
            conn.execute(text(create_procedurelog_sql))
            logger.info(f"Successfully created procedurelog table in {database_type.value} database")
            
            # Handle data insertion based on database type
            if database_type == DatabaseType.SOURCE:
                # Import standardized test data from fixtures
                test_patients = get_test_patient_data()
                test_appointments = get_test_appointment_data()
                test_procedures = get_test_procedure_data()
                
                # Clear any existing test data first
                conn.execute(text("DELETE FROM patient WHERE PatNum IN (1, 2, 3)"))
                conn.execute(text("DELETE FROM appointment WHERE AptNum IN (1, 2, 3)"))
                conn.execute(text("DELETE FROM procedurelog WHERE ProcNum IN (1, 2, 3)"))
                
                # Insert patient test data
                for patient in test_patients:
                    # Build dynamic INSERT statement based on available fields
                    fields = list(patient.keys())
                    placeholders = ', '.join([f':{field}' for field in fields])
                    field_names = ', '.join([f'`{field}`' for field in fields])
                    
                    insert_sql = f"INSERT INTO patient ({field_names}) VALUES ({placeholders})"
                    conn.execute(text(insert_sql), patient)
                
                # Insert appointment test data
                for appointment in test_appointments:
                    # Build dynamic INSERT statement based on available fields
                    fields = list(appointment.keys())
                    placeholders = ', '.join([f':{field}' for field in fields])
                    field_names = ', '.join([f'`{field}`' for field in fields])
                    
                    insert_sql = f"INSERT INTO appointment ({field_names}) VALUES ({placeholders})"
                    conn.execute(text(insert_sql), appointment)
                
                # Insert procedure test data
                for procedure in test_procedures:
                    # Build dynamic INSERT statement based on available fields
                    fields = list(procedure.keys())
                    placeholders = ', '.join([f':{field}' for field in fields])
                    field_names = ', '.join([f'`{field}`' for field in fields])
                    
                    insert_sql = f"INSERT INTO procedurelog ({field_names}) VALUES ({placeholders})"
                    conn.execute(text(insert_sql), procedure)
                
                # Verify the test data was inserted successfully
                patient_count = conn.execute(text("SELECT COUNT(*) FROM patient WHERE PatNum IN (1, 2, 3)")).scalar()
                appointment_count = conn.execute(text("SELECT COUNT(*) FROM appointment WHERE AptNum IN (1, 2, 3)")).scalar()
                procedure_count = conn.execute(text("SELECT COUNT(*) FROM procedurelog WHERE ProcNum IN (1, 2, 3)")).scalar()
                
                logger.info(f"SOURCE DATABASE: Inserted {patient_count} test patients, {appointment_count} test appointments, {procedure_count} test procedures")
                logger.info("Source database is now ready for ETL pipeline testing")
                
            elif database_type == DatabaseType.REPLICATION:
                # Clear ALL existing data from replication database (leave empty for ETL testing)
                conn.execute(text("DELETE FROM patient"))
                conn.execute(text("DELETE FROM appointment"))
                conn.execute(text("DELETE FROM procedurelog"))
                
                logger.info("REPLICATION DATABASE: Cleared all data - ready for ETL pipeline testing")
            
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
        
        # Ping MySQL servers before attempting connections
        source_host = os.environ.get('TEST_OPENDENTAL_SOURCE_HOST', 'localhost')
        source_port = os.environ.get('TEST_OPENDENTAL_SOURCE_PORT', '3306')
        replication_host = os.environ.get('TEST_MYSQL_REPLICATION_HOST', 'localhost')
        replication_port = os.environ.get('TEST_MYSQL_REPLICATION_PORT', '3305')
        
        logger.info(f"Pinging MySQL source server at {source_host}:{source_port}")
        if not ping_mysql_server(source_host, source_port):
            logger.error(f"MySQL source server ping failed at {source_host}:{source_port}. Cannot proceed with database setup.")
            sys.exit(1)
            
        logger.info(f"Pinging MySQL replication server at {replication_host}:{replication_port}")
        if not ping_mysql_server(replication_host, replication_port):
            logger.error(f"MySQL replication server ping failed at {replication_host}:{replication_port}. Cannot proceed with database setup.")
            sys.exit(1)

        # Set up MySQL test source database with test data
        logger.info("Setting up SOURCE database (test_opendental) with test data...")
        setup_mysql_test_database(DatabaseType.SOURCE)
        
        # Set up PostgreSQL analytics database (empty for ETL testing)
        logger.info("Setting up ANALYTICS database (test_opendental_analytics) with empty tables...")
        setup_postgresql_test_database()
        
        # Set up MySQL test replication database (empty for ETL testing)
        logger.info("Setting up REPLICATION database (test_opendental_replication) with empty tables...")
        setup_mysql_test_database(DatabaseType.REPLICATION)
        
        logger.info("="*60)
        logger.info("TEST DATABASE SETUP COMPLETED SUCCESSFULLY!")
        logger.info("="*60)
        logger.info("Database State:")
        logger.info("  • SOURCE (test_opendental): Contains test data (patients, appointments, procedures)")
        logger.info("  • REPLICATION (test_opendental_replication): Empty tables ready for ETL")
        logger.info("  • ANALYTICS (test_opendental_analytics): Empty tables ready for ETL")
        logger.info("")
        logger.info("You can now run E2E tests with: pytest -m e2e")
        logger.info("The ETL pipeline will copy data from SOURCE → REPLICATION → ANALYTICS")
        
    except Exception as e:
        logger.error(f"Test database setup failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 