"""
Test data fixtures for ETL pipeline tests.

This module contains fixtures related to:
- Static test data
- Sample datasets
- Test data generators
- Data utilities
"""

import pytest
import pandas as pd
import numpy as np
from typing import Dict, List, Any
from datetime import datetime, timedelta


@pytest.fixture
def sample_patient_data():
    """Sample patient data for testing."""
    return pd.DataFrame({
        'PatNum': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        'LName': ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 'Rodriguez', 'Martinez'],
        'FName': ['John', 'Jane', 'Bob', 'Alice', 'Charlie', 'Maria', 'David', 'Sarah', 'Carlos', 'Ana'],
        'DateTStamp': [
            datetime(2024, 1, 1, 10, 0, 0),
            datetime(2024, 1, 2, 14, 30, 0),
            datetime(2024, 1, 3, 9, 15, 0),
            datetime(2024, 1, 4, 16, 45, 0),
            datetime(2024, 1, 5, 11, 20, 0),
            datetime(2024, 1, 6, 13, 10, 0),
            datetime(2024, 1, 7, 8, 30, 0),
            datetime(2024, 1, 8, 15, 20, 0),
            datetime(2024, 1, 9, 12, 45, 0),
            datetime(2024, 1, 10, 17, 15, 0)
        ],
        'Status': ['Active', 'Active', 'Inactive', 'Active', 'Active', 'Active', 'Inactive', 'Active', 'Active', 'Active'],
        'BirthDate': [
            datetime(1980, 5, 15),
            datetime(1985, 8, 22),
            datetime(1975, 3, 10),
            datetime(1990, 12, 5),
            datetime(1982, 7, 18),
            datetime(1988, 4, 12),
            datetime(1978, 9, 25),
            datetime(1987, 11, 8),
            datetime(1983, 6, 30),
            datetime(1986, 2, 14)
        ]
    })


@pytest.fixture
def sample_appointment_data():
    """Sample appointment data for testing."""
    return pd.DataFrame({
        'AptNum': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        'PatNum': [1, 2, 1, 3, 4, 5, 2, 6, 7, 8],
        'AptDateTime': [
            datetime(2024, 2, 1, 10, 0, 0),
            datetime(2024, 2, 2, 14, 30, 0),
            datetime(2024, 2, 3, 9, 15, 0),
            datetime(2024, 2, 4, 16, 45, 0),
            datetime(2024, 2, 5, 11, 20, 0),
            datetime(2024, 2, 6, 13, 10, 0),
            datetime(2024, 2, 7, 8, 30, 0),
            datetime(2024, 2, 8, 15, 20, 0),
            datetime(2024, 2, 9, 12, 45, 0),
            datetime(2024, 2, 10, 17, 15, 0)
        ],
        'AptStatus': ['Scheduled', 'Confirmed', 'Completed', 'Cancelled', 'Scheduled', 'Confirmed', 'Completed', 'Scheduled', 'Cancelled', 'Confirmed'],
        'AptType': ['Cleaning', 'Checkup', 'Filling', 'Root Canal', 'Cleaning', 'Checkup', 'Filling', 'Cleaning', 'Root Canal', 'Checkup']
    })


@pytest.fixture
def sample_procedure_data():
    """Sample procedure data for testing."""
    return pd.DataFrame({
        'ProcNum': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        'PatNum': [1, 2, 1, 3, 4, 5, 2, 6, 7, 8],
        'ProcDate': [
            datetime(2024, 1, 15),
            datetime(2024, 1, 16),
            datetime(2024, 1, 17),
            datetime(2024, 1, 18),
            datetime(2024, 1, 19),
            datetime(2024, 1, 20),
            datetime(2024, 1, 21),
            datetime(2024, 1, 22),
            datetime(2024, 1, 23),
            datetime(2024, 1, 24)
        ],
        'ProcFee': [150.00, 200.00, 75.50, 800.00, 125.00, 300.00, 175.25, 450.00, 225.75, 350.00],
        'ProcStatus': ['Complete', 'Complete', 'Complete', 'Complete', 'Complete', 'Complete', 'Complete', 'Complete', 'Complete', 'Complete'],
        'ProcCode': ['D1110', 'D0120', 'D1351', 'D3310', 'D1110', 'D0150', 'D1351', 'D2330', 'D1110', 'D0120']
    })


@pytest.fixture
def sample_claim_data():
    """Sample claim data for testing."""
    return pd.DataFrame({
        'ClaimNum': [1, 2, 3, 4, 5],
        'PatNum': [1, 2, 3, 4, 5],
        'DateSent': [
            datetime(2024, 1, 20),
            datetime(2024, 1, 21),
            datetime(2024, 1, 22),
            datetime(2024, 1, 23),
            datetime(2024, 1, 24)
        ],
        'ClaimStatus': ['Sent', 'Processed', 'Paid', 'Denied', 'Sent'],
        'ClaimAmount': [150.00, 200.00, 75.50, 800.00, 125.00],
        'InsuranceCarrier': ['Blue Cross', 'Aetna', 'Cigna', 'United Health', 'Blue Cross']
    })


@pytest.fixture
def sample_payment_data():
    """Sample payment data for testing."""
    return pd.DataFrame({
        'PayNum': [1, 2, 3, 4, 5],
        'PatNum': [1, 2, 3, 4, 5],
        'DatePay': [
            datetime(2024, 1, 25),
            datetime(2024, 1, 26),
            datetime(2024, 1, 27),
            datetime(2024, 1, 28),
            datetime(2024, 1, 29)
        ],
        'PayAmt': [150.00, 200.00, 75.50, 0.00, 125.00],
        'PayType': ['Insurance', 'Cash', 'Credit Card', 'Insurance', 'Cash'],
        'PayStatus': ['Paid', 'Paid', 'Paid', 'Pending', 'Paid']
    })


@pytest.fixture
def sample_provider_data():
    """Sample provider data for testing."""
    return pd.DataFrame({
        'ProvNum': [1, 2, 3, 4, 5],
        'LName': ['Dr. Smith', 'Dr. Johnson', 'Dr. Williams', 'Dr. Brown', 'Dr. Jones'],
        'FName': ['John', 'Jane', 'Bob', 'Alice', 'Charlie'],
        'Specialty': ['General', 'Orthodontist', 'Endodontist', 'Periodontist', 'General'],
        'Status': ['Active', 'Active', 'Active', 'Active', 'Inactive']
    })


@pytest.fixture
def sample_insurance_data():
    """Sample insurance data for testing."""
    return pd.DataFrame({
        'InsSubNum': [1, 2, 3, 4, 5],
        'PatNum': [1, 2, 3, 4, 5],
        'CarrierName': ['Blue Cross', 'Aetna', 'Cigna', 'United Health', 'Blue Cross'],
        'SubscriberName': ['John Smith', 'Jane Johnson', 'Bob Williams', 'Alice Brown', 'Charlie Jones'],
        'GroupNum': ['GRP001', 'GRP002', 'GRP003', 'GRP004', 'GRP005'],
        'SubscriberID': ['SUB001', 'SUB002', 'SUB003', 'SUB004', 'SUB005']
    })


@pytest.fixture
def sample_dental_codes():
    """Sample dental procedure codes for testing."""
    return pd.DataFrame({
        'CodeNum': [1, 2, 3, 4, 5],
        'ProcCode': ['D1110', 'D0120', 'D1351', 'D3310', 'D0150'],
        'ProcName': ['Adult Prophylaxis', 'Periodic Oral Evaluation', 'Sealant', 'Endodontic Therapy', 'Comprehensive Oral Evaluation'],
        'ProcFee': [150.00, 75.00, 125.00, 800.00, 200.00],
        'ProcCategory': ['Preventive', 'Diagnostic', 'Preventive', 'Endodontic', 'Diagnostic']
    })


@pytest.fixture
def sample_medication_data():
    """Sample medication data for testing."""
    return pd.DataFrame({
        'MedicationNum': [1, 2, 3, 4, 5],
        'PatNum': [1, 2, 3, 4, 5],
        'MedName': ['Amoxicillin', 'Ibuprofen', 'Acetaminophen', 'Penicillin', 'Aspirin'],
        'Dosage': ['500mg', '400mg', '500mg', '250mg', '325mg'],
        'Frequency': ['3x daily', '4x daily', '4x daily', '4x daily', '2x daily'],
        'StartDate': [
            datetime(2024, 1, 15),
            datetime(2024, 1, 16),
            datetime(2024, 1, 17),
            datetime(2024, 1, 18),
            datetime(2024, 1, 19)
        ]
    })


@pytest.fixture
def sample_allergy_data():
    """Sample allergy data for testing."""
    return pd.DataFrame({
        'AllergyNum': [1, 2, 3, 4, 5],
        'PatNum': [1, 2, 3, 4, 5],
        'AllergyDef': ['Penicillin', 'Latex', 'Aspirin', 'Sulfa', 'Codeine'],
        'Reaction': ['Rash', 'Hives', 'Stomach upset', 'Rash', 'Nausea'],
        'Severity': ['Moderate', 'Mild', 'Mild', 'Moderate', 'Severe'],
        'DateTStamp': [
            datetime(2024, 1, 10),
            datetime(2024, 1, 11),
            datetime(2024, 1, 12),
            datetime(2024, 1, 13),
            datetime(2024, 1, 14)
        ]
    })


@pytest.fixture
def sample_document_data():
    """Sample document data for testing."""
    return pd.DataFrame({
        'DocNum': [1, 2, 3, 4, 5],
        'PatNum': [1, 2, 3, 4, 5],
        'DocType': ['X-Ray', 'Consent Form', 'Medical History', 'Treatment Plan', 'Insurance Card'],
        'DocDate': [
            datetime(2024, 1, 5),
            datetime(2024, 1, 6),
            datetime(2024, 1, 7),
            datetime(2024, 1, 8),
            datetime(2024, 1, 9)
        ],
        'DocStatus': ['Active', 'Active', 'Active', 'Active', 'Active'],
        'FileName': ['xray_001.jpg', 'consent_001.pdf', 'history_001.pdf', 'plan_001.pdf', 'insurance_001.jpg']
    })


@pytest.fixture
def sample_task_data():
    """Sample task data for testing."""
    return pd.DataFrame({
        'TaskNum': [1, 2, 3, 4, 5],
        'PatNum': [1, 2, 3, 4, 5],
        'TaskType': ['Follow-up Call', 'Appointment Reminder', 'Insurance Follow-up', 'Treatment Plan Review', 'Recall'],
        'TaskDate': [
            datetime(2024, 2, 1),
            datetime(2024, 2, 2),
            datetime(2024, 2, 3),
            datetime(2024, 2, 4),
            datetime(2024, 2, 5)
        ],
        'TaskStatus': ['Pending', 'Completed', 'Pending', 'Completed', 'Pending'],
        'TaskPriority': ['High', 'Medium', 'High', 'Medium', 'Low']
    })


@pytest.fixture
def sample_recall_data():
    """Sample recall data for testing."""
    return pd.DataFrame({
        'RecallNum': [1, 2, 3, 4, 5],
        'PatNum': [1, 2, 3, 4, 5],
        'RecallType': ['Cleaning', 'Checkup', 'Cleaning', 'Checkup', 'Cleaning'],
        'DateDue': [
            datetime(2024, 7, 1),
            datetime(2024, 7, 2),
            datetime(2024, 7, 3),
            datetime(2024, 7, 4),
            datetime(2024, 7, 5)
        ],
        'RecallStatus': ['Active', 'Active', 'Active', 'Active', 'Active'],
        'Interval': [6, 12, 6, 12, 6]
    })


@pytest.fixture
def sample_fee_data():
    """Sample fee data for testing."""
    return pd.DataFrame({
        'FeeNum': [1, 2, 3, 4, 5],
        'FeeSched': ['Standard', 'Premium', 'Standard', 'Premium', 'Standard'],
        'ProcCode': ['D1110', 'D0120', 'D1351', 'D3310', 'D0150'],
        'Fee': [150.00, 200.00, 75.50, 800.00, 125.00],
        'EffectiveDate': [
            datetime(2024, 1, 1),
            datetime(2024, 1, 1),
            datetime(2024, 1, 1),
            datetime(2024, 1, 1),
            datetime(2024, 1, 1)
        ]
    })


@pytest.fixture
def sample_employee_data():
    """Sample employee data for testing."""
    return pd.DataFrame({
        'EmployeeNum': [1, 2, 3, 4, 5],
        'LName': ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones'],
        'FName': ['John', 'Jane', 'Bob', 'Alice', 'Charlie'],
        'Position': ['Dentist', 'Hygienist', 'Assistant', 'Receptionist', 'Manager'],
        'Status': ['Active', 'Active', 'Active', 'Active', 'Active'],
        'HireDate': [
            datetime(2020, 1, 15),
            datetime(2021, 3, 20),
            datetime(2022, 6, 10),
            datetime(2023, 9, 5),
            datetime(2024, 1, 1)
        ]
    }) 