"""
Standardized Test Data Definitions

This module contains standardized test data that matches the real OpenDental schema.
All integration tests should use this data to ensure consistency and avoid schema mismatches.

The test data is designed to work with both MySQL and PostgreSQL databases,
with proper handling of schema differences between the two systems.
"""

from typing import List, Dict, Any
from datetime import datetime, date

# Standard test patients that match the real OpenDental schema
STANDARD_TEST_PATIENTS = [
    {
        'PatNum': 1,
        'LName': 'Doe',
        'FName': 'John',
        'MiddleI': 'M',
        'Preferred': 'Johnny',
        'PatStatus': 0,
        'Gender': 0,
        'Position': 0,
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
        'GradeLevel': 0,
        'Urgency': 0,
        'DateFirstVisit': date(2023, 1, 1),
        'ClinicNum': 0,
        'HasIns': '',
        'TrophyFolder': '',
        'PlannedIsDone': 0,
        'Premed': 0,
        'Ward': '',
        'PreferConfirmMethod': 0,
        'PreferContactMethod': 0,
        'PreferRecallMethod': 0,
        'SchedBeforeTime': None,
        'SchedAfterTime': None,
        'SchedDayOfWeek': 0,
        'Language': '',
        'AdmitDate': date(2023, 1, 1),
        'Title': '',
        'PayPlanDue': 0.0,
        'SiteNum': 0,
        'DateTStamp': datetime(2023, 1, 1, 10, 0, 0),
        'ResponsParty': 0,
        'CanadianEligibilityCode': 0,
        'AskToArriveEarly': 0,
        'PreferContactConfidential': 0,
        'SuperFamily': 0,
        'TxtMsgOk': 0,
        'SmokingSnoMed': '',
        'Country': '',
        'DateTimeDeceased': datetime(1900, 1, 1, 0, 0, 0),
        'BillingCycleDay': 1,
        'SecUserNumEntry': 0,
        'SecDateEntry': date(2023, 1, 1),
        'HasSuperBilling': 0,
        'PatNumCloneFrom': 0,
        'DiscountPlanNum': 0,
        'HasSignedTil': 0,
        'ShortCodeOptIn': 0,
        'SecurityHash': ''
    },
    {
        'PatNum': 2,
        'LName': 'Smith',
        'FName': 'Jane',
        'MiddleI': 'A',
        'Preferred': 'Janey',
        'PatStatus': 0,
        'Gender': 1,
        'Position': 0,
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
        'GradeLevel': 0,
        'Urgency': 0,
        'DateFirstVisit': date(2023, 1, 2),
        'ClinicNum': 0,
        'HasIns': '',
        'TrophyFolder': '',
        'PlannedIsDone': 0,
        'Premed': 0,
        'Ward': '',
        'PreferConfirmMethod': 0,
        'PreferContactMethod': 0,
        'PreferRecallMethod': 0,
        'SchedBeforeTime': None,
        'SchedAfterTime': None,
        'SchedDayOfWeek': 0,
        'Language': '',
        'AdmitDate': date(2023, 1, 2),
        'Title': '',
        'PayPlanDue': 0.0,
        'SiteNum': 0,
        'DateTStamp': datetime(2023, 1, 2, 11, 0, 0),
        'ResponsParty': 0,
        'CanadianEligibilityCode': 0,
        'AskToArriveEarly': 0,
        'PreferContactConfidential': 0,
        'SuperFamily': 0,
        'TxtMsgOk': 0,
        'SmokingSnoMed': '',
        'Country': '',
        'DateTimeDeceased': datetime(1900, 1, 1, 0, 0, 0),
        'BillingCycleDay': 1,
        'SecUserNumEntry': 0,
        'SecDateEntry': date(2023, 1, 2),
        'HasSuperBilling': 0,
        'PatNumCloneFrom': 0,
        'DiscountPlanNum': 0,
        'HasSignedTil': 0,
        'ShortCodeOptIn': 0,
        'SecurityHash': ''
    },
    {
        'PatNum': 3,
        'LName': 'Johnson',
        'FName': 'Bob',
        'MiddleI': 'R',
        'Preferred': 'Bobby',
        'PatStatus': 0,
        'Gender': 0,
        'Position': 0,
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
        'GradeLevel': 0,
        'Urgency': 0,
        'DateFirstVisit': date(2023, 1, 3),
        'ClinicNum': 0,
        'HasIns': '',
        'TrophyFolder': '',
        'PlannedIsDone': 0,
        'Premed': 0,
        'Ward': '',
        'PreferConfirmMethod': 0,
        'PreferContactMethod': 0,
        'PreferRecallMethod': 0,
        'SchedBeforeTime': None,
        'SchedAfterTime': None,
        'SchedDayOfWeek': 0,
        'Language': '',
        'AdmitDate': date(2023, 1, 3),
        'Title': '',
        'PayPlanDue': 0.0,
        'SiteNum': 0,
        'DateTStamp': datetime(2023, 1, 3, 12, 0, 0),
        'ResponsParty': 0,
        'CanadianEligibilityCode': 0,
        'AskToArriveEarly': 0,
        'PreferContactConfidential': 0,
        'SuperFamily': 0,
        'TxtMsgOk': 0,
        'SmokingSnoMed': '',
        'Country': '',
        'DateTimeDeceased': datetime(1900, 1, 1, 0, 0, 0),
        'BillingCycleDay': 1,
        'SecUserNumEntry': 0,
        'SecDateEntry': date(2023, 1, 3),
        'HasSuperBilling': 0,
        'PatNumCloneFrom': 0,
        'DiscountPlanNum': 0,
        'HasSignedTil': 0,
        'ShortCodeOptIn': 0,
        'SecurityHash': ''
    }
]

# Standard test appointments that reference the test patients
STANDARD_TEST_APPOINTMENTS = [
    {
        'AptNum': 1,
        'PatNum': 1,
        'AptDateTime': datetime(2023, 1, 15, 9, 0, 0),
        'AptStatus': 0,
        'DateTStamp': datetime(2023, 1, 15, 9, 0, 0),
        'Notes': 'Regular checkup'
    },
    {
        'AptNum': 2,
        'PatNum': 2,
        'AptDateTime': datetime(2023, 1, 16, 10, 0, 0),
        'AptStatus': 0,
        'DateTStamp': datetime(2023, 1, 16, 10, 0, 0),
        'Notes': 'Cleaning appointment'
    },
    {
        'AptNum': 3,
        'PatNum': 3,
        'AptDateTime': datetime(2023, 1, 17, 11, 0, 0),
        'AptStatus': 0,
        'DateTStamp': datetime(2023, 1, 17, 11, 0, 0),
        'Notes': 'Consultation'
    }
]

# Minimal test data for quick setup (only required fields)
MINIMAL_TEST_PATIENTS = [
    {
        'PatNum': 1,
        'LName': 'Doe',
        'FName': 'John',
        'MiddleI': 'M',
        'Preferred': 'Johnny',
        'PatStatus': 0,
        'Gender': 0,
        'Position': 0,
        'Birthdate': date(1980, 1, 1),
        'SSN': '123-45-6789'
    },
    {
        'PatNum': 2,
        'LName': 'Smith',
        'FName': 'Jane',
        'MiddleI': 'A',
        'Preferred': 'Janey',
        'PatStatus': 0,
        'Gender': 1,
        'Position': 0,
        'Birthdate': date(1985, 5, 15),
        'SSN': '234-56-7890'
    },
    {
        'PatNum': 3,
        'LName': 'Johnson',
        'FName': 'Bob',
        'MiddleI': 'R',
        'Preferred': 'Bobby',
        'PatStatus': 0,
        'Gender': 0,
        'Position': 0,
        'Birthdate': date(1975, 12, 10),
        'SSN': '345-67-8901'
    }
]

# Test data for incremental loading tests (with different timestamps)
INCREMENTAL_TEST_PATIENTS = [
    {
        'PatNum': 4,
        'LName': 'Wilson',
        'FName': 'Alice',
        'MiddleI': 'B',
        'Preferred': 'Al',
        'PatStatus': 0,
        'Gender': 1,
        'Position': 0,
        'Birthdate': date(1990, 3, 20),
        'SSN': '456-78-9012',
        'DateTStamp': datetime(2023, 1, 4, 13, 0, 0)  # Newer timestamp
    },
    {
        'PatNum': 5,
        'LName': 'Brown',
        'FName': 'Charlie',
        'MiddleI': 'C',
        'Preferred': 'Chuck',
        'PatStatus': 0,
        'Gender': 0,
        'Position': 0,
        'Birthdate': date(1988, 7, 4),
        'SSN': '567-89-0123',
        'DateTStamp': datetime(2023, 1, 5, 14, 0, 0)  # Even newer timestamp
    }
]

def get_test_patient_data(include_all_fields: bool = True) -> List[Dict[str, Any]]:
    """
    Get standardized test patient data.
    
    Args:
        include_all_fields: If True, return complete patient records with all fields.
                           If False, return minimal records with only required fields.
    
    Returns:
        List of patient data dictionaries
    """
    if include_all_fields:
        return STANDARD_TEST_PATIENTS.copy()
    else:
        return MINIMAL_TEST_PATIENTS.copy()

def get_test_appointment_data() -> List[Dict[str, Any]]:
    """
    Get standardized test appointment data.
    
    Returns:
        List of appointment data dictionaries
    """
    return STANDARD_TEST_APPOINTMENTS.copy()

def get_incremental_test_patient_data() -> List[Dict[str, Any]]:
    """
    Get test patient data for incremental loading tests.
    These records have newer timestamps than the standard test data.
    
    Returns:
        List of patient data dictionaries with newer timestamps
    """
    return INCREMENTAL_TEST_PATIENTS.copy()

def get_test_data_for_table(table_name: str, include_all_fields: bool = True) -> List[Dict[str, Any]]:
    """
    Get standardized test data for a specific table.
    
    Args:
        table_name: Name of the table ('patient', 'appointment', etc.)
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
    else:
        raise ValueError(f"Unsupported table: {table_name}. Supported tables: patient, appointment") 