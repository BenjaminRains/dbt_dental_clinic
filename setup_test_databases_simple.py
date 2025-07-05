#!/usr/bin/env python3
"""
Simplified Test Database Setup Script

This script sets up the test databases for the ETL pipeline integration tests.
This version doesn't rely on pytest or test fixtures to avoid import issues.

Usage:
    python setup_test_databases_simple.py
"""

import os
import sys
import logging
from sqlalchemy import create_engine, text
from datetime import datetime, date
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def setup_mysql_test_database(host, port, database, user, password):
    """Set up MySQL test database with basic tables."""
    logger.info(f"Setting up MySQL test database: {database}")
    
    # Create connection string
    connection_string = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
    engine = create_engine(connection_string)
    
    try:
        with engine.connect() as conn:
            # Test the connection
            result = conn.execute(text("SELECT USER(), DATABASE()"))
            user_info = result.fetchone()
            logger.info(f"Connected as user: {user_info[0]}, database: {user_info[1]}")
            
            # Drop existing tables if they exist
            conn.execute(text("DROP TABLE IF EXISTS appointment"))
            conn.execute(text("DROP TABLE IF EXISTS patient"))
            
            # Create patient table with basic schema
            create_patient_sql = """
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
                    PRIMARY KEY (`PatNum`)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """
            conn.execute(text(create_patient_sql))
            
            # Create appointment table
            create_appointment_sql = """
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
            
            # Insert basic test data
            test_patients = [
                {
                    'PatNum': 1,
                    'LName': 'Doe',
                    'FName': 'John',
                    'PatStatus': 0,
                    'Gender': 0,
                    'Birthdate': date(1980, 1, 1),
                    'SSN': '123-45-6789',
                    'DateTStamp': datetime(2023, 1, 1, 10, 0, 0)
                },
                {
                    'PatNum': 2,
                    'LName': 'Smith',
                    'FName': 'Jane',
                    'PatStatus': 0,
                    'Gender': 1,
                    'Birthdate': date(1985, 5, 15),
                    'SSN': '234-56-7890',
                    'DateTStamp': datetime(2023, 1, 2, 11, 0, 0)
                }
            ]
            
            for patient in test_patients:
                fields = list(patient.keys())
                placeholders = ', '.join([f':{field}' for field in fields])
                field_names = ', '.join([f'`{field}`' for field in fields])
                
                insert_sql = f"INSERT INTO patient ({field_names}) VALUES ({placeholders})"
                conn.execute(text(insert_sql), patient)
            
            # Insert basic appointment data
            test_appointments = [
                {
                    'AptNum': 1,
                    'PatNum': 1,
                    'AptDateTime': datetime(2023, 1, 15, 10, 0, 0),
                    'AptStatus': 0,
                    'DateTStamp': datetime(2023, 1, 15, 10, 0, 0),
                    'Notes': 'Test appointment'
                },
                {
                    'AptNum': 2,
                    'PatNum': 2,
                    'AptDateTime': datetime(2023, 1, 16, 14, 0, 0),
                    'AptStatus': 0,
                    'DateTStamp': datetime(2023, 1, 16, 14, 0, 0),
                    'Notes': 'Test appointment 2'
                }
            ]
            
            for appointment in test_appointments:
                fields = list(appointment.keys())
                placeholders = ', '.join([f':{field}' for field in fields])
                field_names = ', '.join([f'`{field}`' for field in fields])
                
                insert_sql = f"INSERT INTO appointment ({field_names}) VALUES ({placeholders})"
                conn.execute(text(insert_sql), appointment)
            
            conn.commit()
            logger.info(f"Successfully set up MySQL test database: {database}")
        
        engine.dispose()
        
    except Exception as e:
        logger.error(f"Failed to set up MySQL test database {database}: {e}")
        raise

def setup_postgresql_test_database(host, port, database, user, password, schema):
    """Set up PostgreSQL test database with basic tables."""
    logger.info(f"Setting up PostgreSQL test database: {database}")
    
    # Create connection string
    connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    engine = create_engine(connection_string)
    
    try:
        with engine.connect() as conn:
            # Create schema if it doesn't exist
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
            logger.info(f"Ensured schema '{schema}' exists")
            
            # Drop existing tables if they exist
            conn.execute(text(f"DROP TABLE IF EXISTS {schema}.appointment CASCADE"))
            conn.execute(text(f"DROP TABLE IF EXISTS {schema}.patient CASCADE"))
            
            # Create patient table
            create_patient_sql = f"""
                CREATE TABLE {schema}.patient (
                    "PatNum" integer NOT NULL,
                    "LName" character varying(100),
                    "FName" character varying(100),
                    "MiddleI" character varying(100),
                    "Preferred" character varying(100),
                    "PatStatus" smallint DEFAULT 0,
                    "Gender" smallint DEFAULT 0,
                    "Position" smallint DEFAULT 0,
                    "Birthdate" date,
                    "SSN" character varying(100),
                    "Address" character varying(100),
                    "Address2" character varying(100),
                    "City" character varying(100),
                    "State" character varying(100),
                    "Zip" character varying(100),
                    "HmPhone" character varying(30),
                    "WkPhone" character varying(30),
                    "WirelessPhone" character varying(30),
                    "Guarantor" bigint DEFAULT 0,
                    "CreditType" character varying(1),
                    "Email" character varying(100),
                    "Salutation" character varying(100),
                    "EstBalance" double precision DEFAULT 0,
                    "PriProv" bigint DEFAULT 0,
                    "SecProv" bigint DEFAULT 0,
                    "FeeSched" bigint DEFAULT 0,
                    "BillingType" bigint DEFAULT 0,
                    "ImageFolder" character varying(100),
                    "AddrNote" text,
                    "FamFinUrgNote" text,
                    "MedUrgNote" character varying(255),
                    "ApptModNote" character varying(255),
                    "StudentStatus" character varying(1),
                    "SchoolName" character varying(255) DEFAULT '',
                    "ChartNumber" character varying(100) DEFAULT '',
                    "MedicaidID" character varying(20),
                    "Bal_0_30" double precision DEFAULT 0,
                    "Bal_31_60" double precision DEFAULT 0,
                    "Bal_61_90" double precision DEFAULT 0,
                    "BalOver90" double precision DEFAULT 0,
                    "InsEst" double precision DEFAULT 0,
                    "BalTotal" double precision DEFAULT 0,
                    "EmployerNum" bigint DEFAULT 0,
                    "EmploymentNote" character varying(255),
                    "County" character varying(255),
                    "GradeLevel" smallint DEFAULT 0,
                    "Urgency" smallint DEFAULT 0,
                    "DateFirstVisit" date DEFAULT '0001-01-01',
                    "ClinicNum" bigint DEFAULT 0,
                    "HasIns" character varying(255),
                    "TrophyFolder" character varying(255),
                    "PlannedIsDone" smallint DEFAULT 0,
                    "Premed" smallint DEFAULT 0,
                    "Ward" character varying(255),
                    "PreferConfirmMethod" smallint DEFAULT 0,
                    "PreferContactMethod" smallint DEFAULT 0,
                    "PreferRecallMethod" smallint DEFAULT 0,
                    "SchedBeforeTime" time without time zone,
                    "SchedAfterTime" time without time zone,
                    "SchedDayOfWeek" smallint DEFAULT 0,
                    "Language" character varying(100),
                    "AdmitDate" date DEFAULT '0001-01-01',
                    "Title" character varying(15),
                    "PayPlanDue" double precision DEFAULT 0,
                    "SiteNum" bigint DEFAULT 0,
                    "DateTStamp" timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
                    "ResponsParty" bigint DEFAULT 0,
                    "CanadianEligibilityCode" smallint DEFAULT 0,
                    "AskToArriveEarly" integer DEFAULT 0,
                    "PreferContactConfidential" smallint DEFAULT 0,
                    "SuperFamily" bigint DEFAULT 0,
                    "TxtMsgOk" smallint DEFAULT 0,
                    "SmokingSnoMed" character varying(32) DEFAULT '',
                    "Country" character varying(255) DEFAULT '',
                    "DateTimeDeceased" timestamp without time zone DEFAULT '0001-01-01 00:00:00',
                    "BillingCycleDay" integer DEFAULT 1,
                    "SecUserNumEntry" bigint DEFAULT 0,
                    "SecDateEntry" date DEFAULT '0001-01-01',
                    "HasSuperBilling" smallint DEFAULT 0,
                    "PatNumCloneFrom" bigint DEFAULT 0,
                    "DiscountPlanNum" bigint DEFAULT 0,
                    "HasSignedTil" smallint DEFAULT 0,
                    "ShortCodeOptIn" smallint DEFAULT 0,
                    "SecurityHash" character varying(255) DEFAULT '',
                    CONSTRAINT patient_pkey PRIMARY KEY ("PatNum")
                )
            """
            conn.execute(text(create_patient_sql))
            
            # Create appointment table
            create_appointment_sql = f"""
                CREATE TABLE {schema}.appointment (
                    "AptNum" integer NOT NULL,
                    "PatNum" bigint NOT NULL,
                    "AptDateTime" timestamp without time zone NOT NULL,
                    "AptStatus" smallint DEFAULT 0,
                    "DateTStamp" timestamp without time zone NOT NULL,
                    "Notes" text,
                    CONSTRAINT appointment_pkey PRIMARY KEY ("AptNum")
                )
            """
            conn.execute(text(create_appointment_sql))
            
            conn.commit()
            logger.info(f"Successfully set up PostgreSQL test database: {database}")
        
        engine.dispose()
        
    except Exception as e:
        logger.error(f"Failed to set up PostgreSQL test database {database}: {e}")
        raise

def main():
    """Main function to set up all test databases."""
    logger.info("Starting simplified test database setup...")
    
    # Load environment variables
    load_dotenv()
    
    # Get test database configuration
    test_source_host = os.getenv('TEST_OPENDENTAL_SOURCE_HOST', 'localhost')
    test_source_port = int(os.getenv('TEST_OPENDENTAL_SOURCE_PORT', '3306'))
    test_source_db = os.getenv('TEST_OPENDENTAL_SOURCE_DB', 'test_opendental')
    test_source_user = os.getenv('TEST_OPENDENTAL_SOURCE_USER', 'test_user')
    test_source_password = os.getenv('TEST_OPENDENTAL_SOURCE_PASSWORD', 'test_pass')
    
    test_replication_host = os.getenv('TEST_MYSQL_REPLICATION_HOST', 'localhost')
    test_replication_port = int(os.getenv('TEST_MYSQL_REPLICATION_PORT', '3305'))
    test_replication_db = os.getenv('TEST_MYSQL_REPLICATION_DB', 'test_opendental_replication')
    test_replication_user = os.getenv('TEST_MYSQL_REPLICATION_USER', 'replication_test_user')
    test_replication_password = os.getenv('TEST_MYSQL_REPLICATION_PASSWORD', 'test_pass')
    
    test_analytics_host = os.getenv('TEST_POSTGRES_ANALYTICS_HOST', 'localhost')
    test_analytics_port = int(os.getenv('TEST_POSTGRES_ANALYTICS_PORT', '5432'))
    test_analytics_db = os.getenv('TEST_POSTGRES_ANALYTICS_DB', 'test_opendental_analytics')
    test_analytics_user = os.getenv('TEST_POSTGRES_ANALYTICS_USER', 'analytics_test_user')
    test_analytics_password = os.getenv('TEST_POSTGRES_ANALYTICS_PASSWORD', 'test_pass')
    test_analytics_schema = os.getenv('TEST_POSTGRES_ANALYTICS_SCHEMA', 'raw')
    
    try:
        logger.info("Setting up test databases...")
        
        # Set up MySQL test source database
        setup_mysql_test_database(
            test_source_host, test_source_port, test_source_db,
            test_source_user, test_source_password
        )
        
        # Set up MySQL test replication database
        setup_mysql_test_database(
            test_replication_host, test_replication_port, test_replication_db,
            test_replication_user, test_replication_password
        )
        
        # Set up PostgreSQL test analytics database
        setup_postgresql_test_database(
            test_analytics_host, test_analytics_port, test_analytics_db,
            test_analytics_user, test_analytics_password, test_analytics_schema
        )
        
        logger.info("Test database setup completed successfully!")
        logger.info("You can now run integration tests with: pytest -m integration")
        
    except Exception as e:
        logger.error(f"Test database setup failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 