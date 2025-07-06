"""
Test Data Fixtures

This module contains standardized test data fixtures for integration tests.
These fixtures provide consistent, reusable test data across all test modules.
"""

import pytest


@pytest.fixture
def standard_patient_test_data():
    """Standardized patient test data for all integration tests."""
    return [
        {'PatNum': 1, 'LName': 'John Doe', 'FName': 'John', 'DateTStamp': '2023-01-01 10:00:00', 'PatStatus': 0},
        {'PatNum': 2, 'LName': 'Jane Smith', 'FName': 'Jane', 'DateTStamp': '2023-01-02 11:00:00', 'PatStatus': 0},
        {'PatNum': 3, 'LName': 'Bob Johnson', 'FName': 'Bob', 'DateTStamp': '2023-01-03 12:00:00', 'PatStatus': 0}
    ]


@pytest.fixture
def incremental_patient_test_data():
    """Test data for incremental loading tests."""
    return [
        {'PatNum': 4, 'LName': 'New User', 'FName': 'Test', 'DateTStamp': '2023-01-04 13:00:00', 'PatStatus': 0}
    ]


@pytest.fixture
def partial_patient_test_data():
    """Test data for partial loading tests (2 patients instead of 3)."""
    return [
        {'PatNum': 1, 'LName': 'John Doe', 'FName': 'John', 'DateTStamp': '2023-01-01 10:00:00', 'PatStatus': 0},
        {'PatNum': 2, 'LName': 'Jane Smith', 'FName': 'Jane', 'DateTStamp': '2023-01-02 11:00:00', 'PatStatus': 0}
    ]


@pytest.fixture
def etl_tracking_test_data():
    """Standardized ETL tracking test data."""
    return [
        {
            'table_name': 'patient',
            'last_loaded': '2023-01-01 10:00:00',
            'load_status': 'success',
            'rows_loaded': 3
        }
    ]


@pytest.fixture
def invalid_schema_test_data():
    """Test data for invalid schema tests."""
    return [
        {'id': 1, 'invalid_column': 'test data', 'created_at': '2023-01-01 10:00:00'}
    ]


@pytest.fixture
def composite_pk_test_data():
    """Test data for composite primary key tests."""
    return [
        {'id1': 0, 'id2': 0, 'value': 'val0'},
        {'id1': 0, 'id2': 1, 'value': 'val1'},
        {'id1': 1, 'id2': 0, 'value': 'val2'},
        {'id1': 1, 'id2': 1, 'value': 'val3'}
    ]


@pytest.fixture
def large_table_test_data():
    """Test data for large table tests (no primary key)."""
    return [
        {'id': i, 'value': f'val{i}'} for i in range(6)
    ]


@pytest.fixture
def simple_test_table_data():
    """Simple test data for basic table operations."""
    return [
        {'id': 1, 'name': 'test'},
        {'id': 2, 'name': 'test2'}
    ]


@pytest.fixture
def patient_with_all_fields_test_data():
    """Patient test data with all fields populated for comprehensive testing."""
    return [
        {
            'PatNum': 9999,
            'LName': 'Test',
            'FName': 'Type',
            # 'MiddleI': 'T',  # Removed - column doesn't exist in actual schema
            'Preferred': 'Test',
            'PatStatus': 0,
            'Gender': 0,
            'Position': 0,
            'Birthdate': '1990-01-01',
            'SSN': '123-45-6789',
            'Address': '123 Test St',
            'Address2': 'Apt 1',
            'City': 'Test City',
            'State': 'TS',
            'Zip': '12345',
            'HmPhone': '555-123-4567',
            'WkPhone': '555-987-6543',
            'WirelessPhone': '555-555-5555',
            'Guarantor': 0,
            'CreditType': 'C',
            'Email': 'test@example.com',
            'Salutation': 'Mr.',
            'EstBalance': 0.0,
            'PriProv': 0,
            'SecProv': 0,
            'FeeSched': 0,
            'BillingType': 0,
            'ImageFolder': '',
            'AddrNote': None,
            'FamFinUrgNote': None,
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
            'DateFirstVisit': '2023-01-01',
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
            'AdmitDate': '2023-01-01',
            'Title': None,
            'PayPlanDue': 0.0,
            'SiteNum': 0,
            'DateTStamp': '2023-01-01 10:00:00',
            'ResponsParty': 0,
            'CanadianEligibilityCode': 0,
            'AskToArriveEarly': 0,
            'PreferContactConfidential': 0,
            'SuperFamily': 0,
            'TxtMsgOk': 0,
            'SmokingSnoMed': '',
            'Country': '',
            'DateTimeDeceased': '0001-01-01 00:00:00',
            'BillingCycleDay': 1,
            'SecUserNumEntry': 0,
            'SecDateEntry': '2023-01-01',
            'HasSuperBilling': 0,
            'PatNumCloneFrom': 0,
            'DiscountPlanNum': 0,
            'HasSignedTil': 0,
            'ShortCodeOptIn': 0,
            'SecurityHash': ''
        }
    ] 