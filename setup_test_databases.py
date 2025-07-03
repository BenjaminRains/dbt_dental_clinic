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

Usage:
    python setup_test_databases.py

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
from datetime import datetime
from dotenv import load_dotenv

# Add the project root to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

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
        logger.error("❌ SAFETY CHECK FAILED: This script can only run in test environments!")
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
    
    logger.info(f"✅ Environment validation passed - running in test mode (detected: {environment})")
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
        logger.error("❌ SAFETY CHECK FAILED: Database names must contain 'test'!")
        logger.error(f"Unsafe database names: {unsafe_dbs}")
        logger.error("This prevents accidental modification of production databases.")
        return False
    
    logger.info("✅ Database name validation passed - all names contain 'test'")
    return True

def confirm_database_creation():
    """
    Ask for explicit confirmation before creating/modifying databases.
    
    Returns:
        bool: True if user confirms, False otherwise
    """
    print("\n" + "="*60)
    print("⚠️  DATABASE SETUP CONFIRMATION REQUIRED ⚠️")
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
            logger.info("✅ User confirmed database setup")
            return True
        elif response in ['no', 'n']:
            logger.info("❌ User cancelled database setup")
            return False
        else:
            print("Please enter 'yes' or 'no'")

def setup_postgresql_test_database():
    """Set up PostgreSQL test analytics database using environment variables."""
    logger.info("Setting up PostgreSQL test analytics database...")
    
    # Database configuration from environment variables
    config = {
        'host': os.environ.get('TEST_POSTGRES_ANALYTICS_HOST', 'localhost'),
        'port': int(os.environ.get('TEST_POSTGRES_ANALYTICS_PORT', 5432)),
        'database': os.environ.get('TEST_POSTGRES_ANALYTICS_DB', 'test_opendental_analytics'),
        'user': os.environ.get('TEST_POSTGRES_ANALYTICS_USER', 'analytics_test_user'),
        'password': os.environ.get('TEST_POSTGRES_ANALYTICS_PASSWORD', 'test_password'),
        'schema': os.environ.get('TEST_POSTGRES_ANALYTICS_SCHEMA', 'raw')
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
            
            # Drop existing tables if they exist
            conn.execute(text("DROP TABLE IF EXISTS raw.appointment CASCADE"))
            conn.execute(text("DROP TABLE IF EXISTS raw.patient CASCADE"))
            conn.execute(text("DROP TABLE IF EXISTS public.appointment CASCADE"))
            conn.execute(text("DROP TABLE IF EXISTS public.patient CASCADE"))
            
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
                    "SchedDayOfWeek" smallint,
                    "Language" character varying(100) COLLATE pg_catalog."default",
                    "AdmitDate" date,
                    "Title" character varying(15) COLLATE pg_catalog."default",
                    "PayPlanDue" double precision,
                    "SiteNum" bigint,
                    "DateTStamp" timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
                    "ResponsParty" bigint,
                    "CanadianEligibilityCode" smallint,
                    "AskToArriveEarly" integer,
                    "PreferContactConfidential" smallint,
                    "SuperFamily" bigint,
                    "TxtMsgOk" smallint,
                    "SmokingSnoMed" character varying(32) COLLATE pg_catalog."default",
                    "Country" character varying(255) COLLATE pg_catalog."default",
                    "DateTimeDeceased" timestamp without time zone,
                    "BillingCycleDay" integer DEFAULT 1,
                    "SecUserNumEntry" bigint,
                    "SecDateEntry" date,
                    "HasSuperBilling" smallint,
                    "PatNumCloneFrom" bigint,
                    "DiscountPlanNum" bigint,
                    "HasSignedTil" smallint,
                    "ShortCodeOptIn" smallint,
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
            
            # Create patient table in public schema with lowercase column names (as expected by raw_to_public transformer)
            conn.execute(text(f"""
                CREATE TABLE public.patient (
                    patnum integer,
                    lname character varying(255),
                    fname character varying(255),
                    middlei character varying(255),
                    preferred character varying(255),
                    patstatus smallint,
                    gender character varying(10),
                    position smallint,
                    birthdate date,
                    ssn character varying(20),
                    address character varying(500),
                    address2 character varying(500),
                    city character varying(100),
                    state character varying(50),
                    zip character varying(20),
                    hmphone character varying(50),
                    wkphone character varying(50),
                    wirelessphone character varying(50),
                    guarantor bigint,
                    credittype character varying(10),
                    email character varying(255),
                    salutation character varying(255),
                    estbalance decimal(10,2),
                    priprov bigint,
                    secprov bigint,
                    feesched bigint,
                    billingtype bigint,
                    imagefolder character varying(255),
                    addrnote text,
                    famfinurgnote text,
                    medurgnote character varying(255),
                    apptmodnote character varying(255),
                    studentstatus character varying(10),
                    schoolname character varying(255),
                    chartnumber character varying(255),
                    medicaidid character varying(20),
                    bal_0_30 decimal(10,2),
                    bal_31_60 decimal(10,2),
                    bal_61_90 decimal(10,2),
                    balover90 decimal(10,2),
                    insest decimal(10,2),
                    baltotal decimal(10,2),
                    employernum bigint,
                    employmentnote character varying(255),
                    county character varying(255),
                    gradelevel smallint,
                    urgency smallint,
                    datefirstvisit date,
                    clinicnum bigint,
                    hasins character varying(255),
                    trophyfolder character varying(255),
                    plannedisdone smallint,
                    premed smallint,
                    ward character varying(255),
                    preferconfirmmethod smallint,
                    prefercontactmethod smallint,
                    preferrecallmethod smallint,
                    schedbeforetime time without time zone,
                    schedaftertime time without time zone,
                    scheddayofweek smallint,
                    language character varying(100),
                    admitdate date,
                    title character varying(15),
                    payplandue decimal(10,2),
                    sitenum bigint,
                    datestamp timestamp without time zone,
                    responsparty bigint,
                    canadianeligibilitycode smallint,
                    asktoarriveearly integer,
                    prefercontactconfidential smallint,
                    superfamily bigint,
                    txtmsgok smallint,
                    smokingsnomed character varying(32),
                    country character varying(255),
                    datetimedeceased timestamp without time zone,
                    billingcycleday integer,
                    secusernumentry bigint,
                    secdateentry date,
                    hassuperbilling smallint,
                    patnumclonefrom bigint,
                    discountplannum bigint,
                    hassignedtil smallint,
                    shortcodeoptin smallint,
                    securityhash character varying(255)
                )
            """))
            
            # Create appointment table in public schema
            conn.execute(text(f"""
                CREATE TABLE public.appointment (
                    aptnum integer,
                    patnum integer,
                    aptdatetime timestamp without time zone,
                    aptstatus character varying(50),
                    datestamp timestamp without time zone,
                    notes text
                )
            """))
            
            # Insert sample data with minimal required fields
            # Clear any existing test data first
            conn.execute(text("DELETE FROM raw.patient WHERE \"PatNum\" IN (1, 2, 3)"))
            
            test_patients = [
                (1, 'Doe', 'John', 'M', 'Johnny', 0, 0, 0, '1980-01-01', '123-45-6789'),
                (2, 'Smith', 'Jane', 'A', 'Janey', 0, 1, 0, '1985-05-15', '234-56-7890'),
                (3, 'Johnson', 'Bob', 'R', 'Bobby', 0, 0, 0, '1975-12-10', '345-67-8901')
            ]
            
            for patient in test_patients:
                conn.execute(text(f"""
                    INSERT INTO raw.patient ("PatNum", "LName", "FName", "MiddleI", "Preferred", "PatStatus", "Gender", "Position", "Birthdate", "SSN") 
                    VALUES (:patnum, :lname, :fname, :middlei, :preferred, :patstatus, :gender, :position, :birthdate, :ssn)
                """), {
                    'patnum': patient[0], 'lname': patient[1], 'fname': patient[2], 'middlei': patient[3], 
                    'preferred': patient[4], 'patstatus': patient[5], 'gender': patient[6], 'position': patient[7],
                    'birthdate': patient[8], 'ssn': patient[9]
                })
            
            # Verify the test data was inserted successfully
            result = conn.execute(text("SELECT COUNT(*) FROM raw.patient WHERE \"PatNum\" IN (1, 2, 3)"))
            count = result.scalar()
            logger.info(f"Inserted {count} test patients to verify table functionality")
            
            # Clean up test data - remove the test patients we just inserted
            conn.execute(text("DELETE FROM raw.patient WHERE \"PatNum\" IN (1, 2, 3)"))
            logger.info("Cleaned up test patients - table is now empty and ready for pytest tests")
            
            conn.commit()
            logger.info(f"Successfully created complete patient table in raw schema of {config['database']}")
            logger.info(f"Successfully created public schema tables in {config['database']}")
        
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
        'host': os.environ.get('TEST_MYSQL_REPLICATION_HOST', 'localhost'),
        'port': int(os.environ.get('TEST_MYSQL_REPLICATION_PORT', 3305)),
        'database': os.environ.get('TEST_MYSQL_REPLICATION_DB', 'test_opendental_replication'),
        'user': os.environ.get('TEST_MYSQL_REPLICATION_USER', 'replication_test_user'),
        'password': os.environ.get('TEST_MYSQL_REPLICATION_PASSWORD', 'test_password')
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
        
        # Detect MySQL version and get compatible collation
        mysql_version = get_mysql_version(test_engine)
        collation = get_compatible_collation(mysql_version)
        logger.info(f"Using collation: {collation} for MySQL {mysql_version}")
        
        with test_engine.connect() as conn:
            # Drop existing patient table if it exists, then create with complete schema
            conn.execute(text("DROP TABLE IF EXISTS patient"))
            
            # Create complete patient table matching test_opendental_replication schema
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
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE={collation}
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
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE={collation}
            """
            conn.execute(text(create_appointment_sql))
            
            # Insert sample data with minimal required fields
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
            
            # Verify the test data was inserted successfully
            result = conn.execute(text("SELECT COUNT(*) FROM patient WHERE PatNum IN (1, 2, 3)"))
            count = result.scalar()
            logger.info(f"Inserted {count} test patients to verify table functionality")
            
            # Clean up test data - remove the test patients we just inserted
            conn.execute(text("DELETE FROM patient WHERE PatNum IN (1, 2, 3)"))
            logger.info("Cleaned up test patients - table is now empty and ready for pytest tests")
            
            conn.commit()
            logger.info(f"Successfully set up MySQL test replication database with complete schema: {config['database']}")
        
        admin_engine.dispose()
        test_engine.dispose()
        
    except Exception as e:
        logger.error(f"Failed to set up MySQL test replication database: {e}")
        sys.exit(1) # exit if error


def get_mysql_version(engine) -> str:
    """Get MySQL version from the server."""
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT VERSION()"))
            version = result.fetchone()[0]
            logger.info(f"MySQL version detected: {version}")
            return version
    except Exception as e:
        logger.warning(f"Could not detect MySQL version: {e}")
        return "unknown"

def get_compatible_collation(mysql_version: str) -> str:
    """Get collation compatible with the MySQL version."""
    if mysql_version.startswith(('8.', '9.')):
        # MySQL 8.0+ supports utf8mb4_0900_ai_ci
        return "utf8mb4_0900_ai_ci"
    elif mysql_version.startswith(('5.7', '5.6', '5.5')):
        # MySQL 5.5-5.7 supports utf8mb4_general_ci
        return "utf8mb4_general_ci"
    else:
        # Fallback to general_ci for older versions
        return "utf8mb4_general_ci"

def setup_mysql_source_test_database():
    """Set up MySQL test source database using environment variables."""
    logger.info("Setting up MySQL test source database...")
    
    # Database configuration from environment variables
    config = {
        'host': os.environ.get('TEST_OPENDENTAL_SOURCE_HOST', '192.168.2.10'),
        'port': int(os.environ.get('TEST_OPENDENTAL_SOURCE_PORT', 3306)),
        'database': os.environ.get('TEST_OPENDENTAL_SOURCE_DB', 'test_opendental'),
        'user': os.environ.get('TEST_OPENDENTAL_SOURCE_USER', 'test_user'),
        'password': os.environ.get('TEST_OPENDENTAL_SOURCE_PASSWORD', 'test_password')
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
                logger.info(f"Created test source database: {config['database']}")
            else:
                logger.info(f"Test source database {config['database']} already exists")
        
        # Connect to the test database
        test_connection_string = (
            f"mysql+pymysql://{config['user']}:{config['password']}"
            f"@{config['host']}:{config['port']}/{config['database']}"
        )
        
        test_engine = create_engine(test_connection_string)
        
        # Detect MySQL version and get compatible collation
        mysql_version = get_mysql_version(test_engine)
        collation = get_compatible_collation(mysql_version)
        logger.info(f"Using collation: {collation} for MySQL {mysql_version}")
        
        with test_engine.connect() as conn:
            # Drop existing patient table if it exists, then create with complete schema
            conn.execute(text("DROP TABLE IF EXISTS patient"))
            
            # Create complete patient table matching test_opendental schema
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
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE={collation}
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
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE={collation}
            """
            conn.execute(text(create_appointment_sql))
            
            # Insert sample data with minimal required fields
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
            
            # Verify the test data was inserted successfully
            result = conn.execute(text("SELECT COUNT(*) FROM patient WHERE PatNum IN (1, 2, 3)"))
            count = result.scalar()
            logger.info(f"Inserted {count} test patients to verify table functionality")
            
            # Clean up test data - remove the test patients we just inserted
            conn.execute(text("DELETE FROM patient WHERE PatNum IN (1, 2, 3)"))
            logger.info("Cleaned up test patients - table is now empty and ready for pytest tests")
            
            conn.commit()
            logger.info(f"Successfully set up MySQL test source database with complete schema: {config['database']}")
        
        admin_engine.dispose()
        test_engine.dispose()
        
    except Exception as e:
        logger.error(f"Failed to set up MySQL test source database: {e}")
        sys.exit(1) # exit if error


def main():
    """Main function to set up all test databases."""
    logger.info("🔒 Starting test database setup with safety checks...")
    
    # Set test environment explicitly
    os.environ['ETL_ENVIRONMENT'] = 'test'
    logger.info("✅ Set ETL_ENVIRONMENT=test for test database setup")
    
    # Load environment variables
    base_dir = os.path.dirname(os.path.abspath(__file__))
    env_path = os.path.join(base_dir, '.env')
    load_dotenv(dotenv_path=env_path)
    
    if os.path.exists(env_path):
        logger.info(f"Loaded .env file from: {env_path}")
    else:
        logger.warning(f".env file not found at: {env_path}")
    
    # Safety checks
    if not validate_test_environment():
        sys.exit(1)
    
    if not validate_database_names():
        sys.exit(1)
    
    if not confirm_database_creation():
        sys.exit(0)
    
    # Debug: Print relevant environment variables
    logger.info("📋 Environment Configuration:")
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
        logger.info("🚀 Starting database setup...")
        
        # Set up MySQL test source database
        setup_mysql_source_test_database()
        
        # Set up PostgreSQL test database
        setup_postgresql_test_database()
        
        # Set up MySQL test database
        setup_mysql_test_database()
        
        logger.info("✅ Test database setup completed successfully!")
        logger.info("🎯 You can now run integration tests with: pytest -m integration")
        
    except Exception as e:
        logger.error(f"❌ Test database setup failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 