"""
Standardized Test Data Definitions

This module contains standardized test data that matches the real OpenDental schema.
All integration tests should use this data to ensure consistency and avoid schema mismatches.

The test data is designed to work with both MySQL and PostgreSQL databases,
with proper handling of schema differences between the two systems.

Connection Architecture Compliance:
- ✅ Environment-specific test data following naming conventions
- ✅ Test data that supports Settings injection patterns
- ✅ Test data for provider pattern testing
- ✅ Test data for unified interface testing
- ✅ Test data for enum-based database type testing
"""

from typing import List, Dict, Any
from datetime import datetime, date

# Import connection architecture components
from etl_pipeline.config import DatabaseType, PostgresSchema, Settings
from etl_pipeline.config.providers import DictConfigProvider

# Standard test patients that match the real OpenDental schema
STANDARD_TEST_PATIENTS = [
    {
        'PatNum': 1,
        'LName': 'Doe',
        'FName': 'John',
        # 'MiddleI': 'M',  # Removed - column doesn't exist in actual schema
        'Preferred': 'Johnny',
        'PatStatus': False,
        'Gender': False,
        'Position': False,
        'Birthdate': date(1980, 1, 1),
        'SSN': '123-45-6789',
        'Address': '123 Main St',
        'Address2': 'Apt 1',
        'City': 'Anytown',
        'State': 'CA',
        'Zip': '90210',
        'HmPhone': '555-123-4567',
        'WkPhone': '555-987-6543',
        'WirelessPhone': '555-555-5555',
        'Guarantor': 0,
        'CreditType': '',
        'Email': 'john.doe@example.com',
        'Salutation': 'Mr.',
        'EstBalance': 0.0,
        'PriProv': 0,
        'SecProv': 0,
        'FeeSched': 0,
        'BillingType': 0,
        'ImageFolder': '',
        'AddrNote': '',
        'FamFinUrgNote': '',
        'MedUrgNote': '',
        'ApptModNote': '',
        'StudentStatus': '',
        'SchoolName': '',
        'ChartNumber': '',
        'MedicaidID': '',
        'Bal_0_30': 0.0,
        'Bal_31_60': 0.0,
        'Bal_61_90': 0.0,
        'BalOver90': 0.0,
        'InsEst': 0.0,
        'BalTotal': 0.0,
        'EmployerNum': 0,
        'EmploymentNote': '',
        'County': '',
        'GradeLevel': False,
        'Urgency': False,
        'DateFirstVisit': date(2023, 1, 1),
        'ClinicNum': 0,
        'HasIns': '',
        'TrophyFolder': '',
        'PlannedIsDone': False,
        'Premed': False,
        'Ward': '',
        'PreferConfirmMethod': False,
        'PreferContactMethod': False,
        'PreferRecallMethod': False,
        'SchedBeforeTime': None,
        'SchedAfterTime': None,
        'SchedDayOfWeek': False,
        'Language': '',
        'AdmitDate': date(2023, 1, 1),
        'Title': '',
        'PayPlanDue': 0.0,
        'SiteNum': 0,
        'DateTStamp': datetime(2023, 1, 1, 10, 0, 0),
        'ResponsParty': 0,
        'CanadianEligibilityCode': False,
        'AskToArriveEarly': 0,
        'PreferContactConfidential': False,
        'SuperFamily': 0,
        'TxtMsgOk': False,
        'SmokingSnoMed': '',
        'Country': '',
        'DateTimeDeceased': datetime(1900, 1, 1, 0, 0, 0),
        'BillingCycleDay': 1,
        'SecUserNumEntry': 0,
        'SecDateEntry': date(2023, 1, 1),
        'HasSuperBilling': False,
        'PatNumCloneFrom': 0,
        'DiscountPlanNum': 0,
        'HasSignedTil': False,
        'ShortCodeOptIn': False,
        'SecurityHash': ''
    },
    {
        'PatNum': 2,
        'LName': 'Smith',
        'FName': 'Jane',
        # 'MiddleI': 'A',  # Removed - column doesn't exist in actual schema
        'Preferred': 'Janey',
        'PatStatus': False,
        'Gender': True,
        'Position': False,
        'Birthdate': date(1985, 5, 15),
        'SSN': '234-56-7890',
        'Address': '456 Oak Ave',
        'Address2': '',
        'City': 'Somewhere',
        'State': 'NY',
        'Zip': '10001',
        'HmPhone': '555-234-5678',
        'WkPhone': '555-876-5432',
        'WirelessPhone': '555-444-4444',
        'Guarantor': 0,
        'CreditType': '',
        'Email': 'jane.smith@example.com',
        'Salutation': 'Ms.',
        'EstBalance': 0.0,
        'PriProv': 0,
        'SecProv': 0,
        'FeeSched': 0,
        'BillingType': 0,
        'ImageFolder': '',
        'AddrNote': '',
        'FamFinUrgNote': '',
        'MedUrgNote': '',
        'ApptModNote': '',
        'StudentStatus': '',
        'SchoolName': '',
        'ChartNumber': '',
        'MedicaidID': '',
        'Bal_0_30': 0.0,
        'Bal_31_60': 0.0,
        'Bal_61_90': 0.0,
        'BalOver90': 0.0,
        'InsEst': 0.0,
        'BalTotal': 0.0,
        'EmployerNum': 0,
        'EmploymentNote': '',
        'County': '',
        'GradeLevel': False,
        'Urgency': False,
        'DateFirstVisit': date(2023, 1, 2),
        'ClinicNum': 0,
        'HasIns': '',
        'TrophyFolder': '',
        'PlannedIsDone': False,
        'Premed': False,
        'Ward': '',
        'PreferConfirmMethod': False,
        'PreferContactMethod': False,
        'PreferRecallMethod': False,
        'SchedBeforeTime': None,
        'SchedAfterTime': None,
        'SchedDayOfWeek': False,
        'Language': '',
        'AdmitDate': date(2023, 1, 2),
        'Title': '',
        'PayPlanDue': 0.0,
        'SiteNum': 0,
        'DateTStamp': datetime(2023, 1, 2, 11, 0, 0),
        'ResponsParty': 0,
        'CanadianEligibilityCode': False,
        'AskToArriveEarly': 0,
        'PreferContactConfidential': False,
        'SuperFamily': 0,
        'TxtMsgOk': False,
        'SmokingSnoMed': '',
        'Country': '',
        'DateTimeDeceased': datetime(1900, 1, 1, 0, 0, 0),
        'BillingCycleDay': 1,
        'SecUserNumEntry': 0,
        'SecDateEntry': date(2023, 1, 2),
        'HasSuperBilling': False,
        'PatNumCloneFrom': 0,
        'DiscountPlanNum': 0,
        'HasSignedTil': False,
        'ShortCodeOptIn': False,
        'SecurityHash': ''
    },
    {
        'PatNum': 3,
        'LName': 'Johnson',
        'FName': 'Bob',
        # 'MiddleI': 'R',  # Removed - column doesn't exist in actual schema
        'Preferred': 'Bobby',
        'PatStatus': False,
        'Gender': False,
        'Position': False,
        'Birthdate': date(1975, 12, 10),
        'SSN': '345-67-8901',
        'Address': '789 Pine St',
        'Address2': 'Suite 100',
        'City': 'Elsewhere',
        'State': 'TX',
        'Zip': '75001',
        'HmPhone': '555-345-6789',
        'WkPhone': '555-765-4321',
        'WirelessPhone': '555-333-3333',
        'Guarantor': 0,
        'CreditType': '',
        'Email': 'bob.johnson@example.com',
        'Salutation': 'Mr.',
        'EstBalance': 0.0,
        'PriProv': 0,
        'SecProv': 0,
        'FeeSched': 0,
        'BillingType': 0,
        'ImageFolder': '',
        'AddrNote': '',
        'FamFinUrgNote': '',
        'MedUrgNote': '',
        'ApptModNote': '',
        'StudentStatus': '',
        'SchoolName': '',
        'ChartNumber': '',
        'MedicaidID': '',
        'Bal_0_30': 0.0,
        'Bal_31_60': 0.0,
        'Bal_61_90': 0.0,
        'BalOver90': 0.0,
        'InsEst': 0.0,
        'BalTotal': 0.0,
        'EmployerNum': 0,
        'EmploymentNote': '',
        'County': '',
        'GradeLevel': False,
        'Urgency': False,
        'DateFirstVisit': date(2023, 1, 3),
        'ClinicNum': 0,
        'HasIns': '',
        'TrophyFolder': '',
        'PlannedIsDone': False,
        'Premed': False,
        'Ward': '',
        'PreferConfirmMethod': False,
        'PreferContactMethod': False,
        'PreferRecallMethod': False,
        'SchedBeforeTime': None,
        'SchedAfterTime': None,
        'SchedDayOfWeek': False,
        'Language': '',
        'AdmitDate': date(2023, 1, 3),
        'Title': '',
        'PayPlanDue': 0.0,
        'SiteNum': 0,
        'DateTStamp': datetime(2023, 1, 3, 12, 0, 0),
        'ResponsParty': 0,
        'CanadianEligibilityCode': False,
        'AskToArriveEarly': 0,
        'PreferContactConfidential': False,
        'SuperFamily': 0,
        'TxtMsgOk': False,
        'SmokingSnoMed': '',
        'Country': '',
        'DateTimeDeceased': datetime(1900, 1, 1, 0, 0, 0),
        'BillingCycleDay': 1,
        'SecUserNumEntry': 0,
        'SecDateEntry': date(2023, 1, 3),
        'HasSuperBilling': False,
        'PatNumCloneFrom': 0,
        'DiscountPlanNum': 0,
        'HasSignedTil': False,
        'ShortCodeOptIn': False,
        'SecurityHash': ''
    }
]

# Standard test appointments that reference the test patients
STANDARD_TEST_APPOINTMENTS = [
    {
        'AptNum': 1,
        'PatNum': 1,
        'AptDateTime': datetime(2023, 1, 15, 9, 0, 0),
        'AptStatus': False,
        'DateTStamp': datetime(2023, 1, 15, 9, 0, 0),
        'Notes': 'Regular checkup'
    },
    {
        'AptNum': 2,
        'PatNum': 2,
        'AptDateTime': datetime(2023, 1, 16, 10, 30, 0),
        'AptStatus': False,
        'DateTStamp': datetime(2023, 1, 16, 10, 30, 0),
        'Notes': 'Cleaning appointment'
    },
    {
        'AptNum': 3,
        'PatNum': 3,
        'AptDateTime': datetime(2023, 1, 17, 14, 0, 0),
        'AptStatus': False,
        'DateTStamp': datetime(2023, 1, 17, 14, 0, 0),
        'Notes': 'Follow-up visit'
    }
]

# Standard test procedures that match the real OpenDental schema
STANDARD_TEST_PROCEDURES = [
    {
        'ProcNum': 1,
        'PatNum': 1,
        'AptNum': 1,
        'ProcStatus': False,
        'ProcFee': 150.00,
        'ProcFeeCur': 150.00,
        'ProcDate': date(2023, 1, 15),
        'CodeNum': 1,
        'ProcNote': 'Regular cleaning',
        'DateTStamp': datetime(2023, 1, 15, 9, 0, 0)
    },
    {
        'ProcNum': 2,
        'PatNum': 2,
        'AptNum': 2,
        'ProcStatus': False,
        'ProcFee': 200.00,
        'ProcFeeCur': 200.00,
        'ProcDate': date(2023, 1, 16),
        'CodeNum': 2,
        'ProcNote': 'Deep cleaning',
        'DateTStamp': datetime(2023, 1, 16, 10, 30, 0)
    },
    {
        'ProcNum': 3,
        'PatNum': 3,
        'AptNum': 3,
        'ProcStatus': False,
        'ProcFee': 300.00,
        'ProcFeeCur': 300.00,
        'ProcDate': date(2023, 1, 17),
        'CodeNum': 3,
        'ProcNote': 'Cavity filling',
        'DateTStamp': datetime(2023, 1, 17, 14, 0, 0)
    }
]

# Connection architecture specific test data
CONNECTION_ARCHITECTURE_TEST_DATA = {
    'database_types': [DatabaseType.SOURCE, DatabaseType.REPLICATION, DatabaseType.ANALYTICS],
    'postgres_schemas': [PostgresSchema.RAW, PostgresSchema.STAGING, PostgresSchema.INTERMEDIATE, PostgresSchema.MARTS],
    'test_environment': 'test',
    'production_environment': 'production'
}

# Environment-specific test data following connection architecture patterns
TEST_ENVIRONMENT_DATA = {
    'environment': 'test',
    'database_prefix': 'TEST_',
    'source_database': 'test_opendental',
    'replication_database': 'test_opendental_replication',
    'analytics_database': 'test_opendental_analytics',
    'source_host': 'test-source-host',
    'replication_host': 'test-repl-host',
    'analytics_host': 'test-analytics-host'
}

PRODUCTION_ENVIRONMENT_DATA = {
    'environment': 'production',
    'database_prefix': '',
    'source_database': 'opendental',
    'replication_database': 'opendental_replication',
    'analytics_database': 'opendental_analytics',
    'source_host': 'prod-source-host',
    'replication_host': 'prod-repl-host',
    'analytics_host': 'prod-analytics-host'
}

# Settings injection test data
SETTINGS_INJECTION_TEST_DATA = {
    'test_settings': {
        'environment': 'test',
        'provider_type': 'DictConfigProvider',
        'database_types': [DatabaseType.SOURCE, DatabaseType.REPLICATION, DatabaseType.ANALYTICS],
        'schemas': [PostgresSchema.RAW, PostgresSchema.STAGING, PostgresSchema.INTERMEDIATE, PostgresSchema.MARTS]
    },
    'production_settings': {
        'environment': 'production',
        'provider_type': 'FileConfigProvider',
        'database_types': [DatabaseType.SOURCE, DatabaseType.REPLICATION, DatabaseType.ANALYTICS],
        'schemas': [PostgresSchema.RAW, PostgresSchema.STAGING, PostgresSchema.INTERMEDIATE, PostgresSchema.MARTS]
    }
}

# Unified interface test data
UNIFIED_INTERFACE_TEST_DATA = {
    'connection_methods': [
        'get_source_connection(settings)',
        'get_replication_connection(settings)',
        'get_analytics_connection(settings, schema)',
        'get_analytics_raw_connection(settings)',
        'get_analytics_staging_connection(settings)',
        'get_analytics_intermediate_connection(settings)',
        'get_analytics_marts_connection(settings)'
    ],
    'settings_methods': [
        'get_database_config(DatabaseType.SOURCE)',
        'get_database_config(DatabaseType.ANALYTICS, PostgresSchema.RAW)',
        'get_source_connection_config()',
        'get_replication_connection_config()',
        'get_analytics_connection_config(schema)'
    ]
}

# Provider pattern test data
PROVIDER_PATTERN_TEST_DATA = {
    'test_provider': {
        'provider_type': 'DictConfigProvider',
        'environment': 'test',
        'config_sources': ['pipeline', 'tables', 'env'],
        'injected_config': True
    },
    'production_provider': {
        'provider_type': 'FileConfigProvider',
        'environment': 'production',
        'config_sources': ['pipeline.yml', 'tables.yml', '.env_production'],
        'injected_config': False
    }
}


def get_test_patient_data(include_all_fields: bool = True) -> List[Dict[str, Any]]:
    """
    Get standardized test patient data following connection architecture patterns.
    
    Args:
        include_all_fields: Ignored - always returns complete patient records.
    
    Returns:
        List of patient data dictionaries
    """
    return STANDARD_TEST_PATIENTS.copy()


def get_test_appointment_data() -> List[Dict[str, Any]]:
    """
    Get standardized test appointment data following connection architecture patterns.
    
    Returns:
        List of appointment data dictionaries
    """
    return STANDARD_TEST_APPOINTMENTS.copy()


def get_test_procedure_data() -> List[Dict[str, Any]]:
    """
    Get standardized test procedure data following connection architecture patterns.
    
    Returns:
        List of procedure data dictionaries
    """
    return STANDARD_TEST_PROCEDURES.copy()


def get_test_data_for_table(table_name: str, include_all_fields: bool = True) -> List[Dict[str, Any]]:
    """
    Get standardized test data for a specific table following connection architecture patterns.
    
    Args:
        table_name: Name of the table ('patient', 'appointment', 'procedurelog', etc.)
        include_all_fields: For patient table, whether to include all fields
    
    Returns:
        List of data dictionaries for the specified table
    
    Raises:
        ValueError: If table_name is not supported
    """
    if table_name.lower() == 'patient':
        return get_test_patient_data(include_all_fields)
    elif table_name.lower() == 'appointment':
        return get_test_appointment_data()
    elif table_name.lower() == 'procedurelog':
        return get_test_procedure_data()
    else:
        raise ValueError(f"Unsupported table: {table_name}. Supported tables: patient, appointment, procedurelog")


def get_connection_architecture_test_data() -> Dict[str, Any]:
    """
    Get connection architecture test data following connection architecture patterns.
    
    Returns:
        Dictionary containing connection architecture test data
    """
    return CONNECTION_ARCHITECTURE_TEST_DATA.copy()


def get_environment_test_data(environment: str = 'test') -> Dict[str, Any]:
    """
    Get environment-specific test data following connection architecture patterns.
    
    Args:
        environment: Environment name ('test' or 'production')
    
    Returns:
        Dictionary containing environment-specific test data
    
    Raises:
        ValueError: If environment is not supported
    """
    if environment.lower() == 'test':
        return TEST_ENVIRONMENT_DATA.copy()
    elif environment.lower() == 'production':
        return PRODUCTION_ENVIRONMENT_DATA.copy()
    else:
        raise ValueError(f"Unsupported environment: {environment}. Supported environments: test, production")


def get_settings_injection_test_data() -> Dict[str, Any]:
    """
    Get Settings injection test data following connection architecture patterns.
    
    Returns:
        Dictionary containing Settings injection test data
    """
    return SETTINGS_INJECTION_TEST_DATA.copy()


def get_unified_interface_test_data() -> Dict[str, Any]:
    """
    Get unified interface test data following connection architecture patterns.
    
    Returns:
        Dictionary containing unified interface test data
    """
    return UNIFIED_INTERFACE_TEST_DATA.copy()


def get_provider_pattern_test_data() -> Dict[str, Any]:
    """
    Get provider pattern test data following connection architecture patterns.
    
    Returns:
        Dictionary containing provider pattern test data
    """
    return PROVIDER_PATTERN_TEST_DATA.copy()


def get_database_type_enum() -> type[DatabaseType]:
    """
    Get DatabaseType enum for testing following connection architecture patterns.
    
    Returns:
        DatabaseType enum class
    """
    return DatabaseType


def get_postgres_schema_enum() -> type[PostgresSchema]:
    """
    Get PostgresSchema enum for testing following connection architecture patterns.
    
    Returns:
        PostgresSchema enum class
    """
    return PostgresSchema 