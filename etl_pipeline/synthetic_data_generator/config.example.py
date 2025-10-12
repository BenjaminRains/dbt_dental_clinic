"""
Configuration Example for Synthetic Data Generator
================================================

Copy this file to config.py and customize for your environment.
DO NOT commit config.py to version control (it's in .gitignore).

This file shows all available configuration options with recommended values
for different deployment scenarios.
"""

from datetime import datetime


# ============================================
# DEPLOYMENT PROFILES
# ============================================

class LocalDevelopment:
    """Local development configuration"""
    
    # Database connection
    db_host = "localhost"
    db_port = 5432
    db_name = "opendental_demo"
    db_user = "postgres"
    db_password = "postgres"
    db_schema = "raw"
    
    # Data volumes (small for testing)
    num_clinics = 3
    num_providers = 8
    num_operatories = 12
    num_patients = 1000
    num_appointments = 3000
    num_procedures = 4000
    
    # Date range
    start_date = datetime(2023, 1, 1)
    end_date = datetime.now()
    
    # Probabilities
    insurance_coverage_rate = 0.60
    appointment_completion_rate = 0.75
    claim_submission_rate = 0.90
    standard_fee_match_rate = 0.80
    
    # Performance
    batch_size = 1000  # Records per batch insert
    log_level = "INFO"


class ProductionDemo:
    """Production demo configuration for dbt Cloud"""
    
    # Database connection (Render.com example)
    db_host = "dpg-xxxxx.render.com"  # Replace with your host
    db_port = 5432
    db_name = "opendental_demo"
    db_user = "opendental_demo_user"
    db_password = "YOUR_SECURE_PASSWORD_HERE"
    db_schema = "raw"
    
    # Data volumes (full scale for portfolio)
    num_clinics = 5
    num_providers = 12
    num_operatories = 20
    num_patients = 5000
    num_appointments = 15000
    num_procedures = 20000
    
    # Date range (CRITICAL: dbt filters to >= 2023-01-01)
    start_date = datetime(2023, 1, 1)
    end_date = datetime.now()
    
    # Probabilities (realistic business scenarios)
    insurance_coverage_rate = 0.60  # 60% of patients have insurance
    appointment_completion_rate = 0.75  # 75% of appointments are completed
    claim_submission_rate = 0.90  # 90% of insured procedures get claims
    standard_fee_match_rate = 0.80  # 80% of procedures match standard fees
    
    # Business rules
    hygiene_recall_months = 6  # Recall every 6 months
    
    # Performance
    batch_size = 5000  # Larger batches for cloud database
    log_level = "INFO"


class MinimalViableProduct:
    """Minimal configuration for quick testing"""
    
    # Database connection
    db_host = "localhost"
    db_port = 5432
    db_name = "opendental_demo_test"
    db_user = "postgres"
    db_password = "postgres"
    db_schema = "raw"
    
    # Data volumes (minimal for 2-minute generation)
    num_clinics = 2
    num_providers = 4
    num_operatories = 6
    num_patients = 100
    num_appointments = 300
    num_procedures = 400
    
    # Date range
    start_date = datetime(2023, 6, 1)  # Only 6 months of data
    end_date = datetime.now()
    
    # Probabilities
    insurance_coverage_rate = 0.50
    appointment_completion_rate = 0.80
    claim_submission_rate = 0.85
    standard_fee_match_rate = 0.90
    
    # Performance
    batch_size = 500
    log_level = "DEBUG"


# ============================================
# ACTIVE CONFIGURATION
# ============================================

# Select which configuration to use
# Options: LocalDevelopment, ProductionDemo, MinimalViableProduct
ACTIVE_CONFIG = LocalDevelopment


# ============================================
# ADVANCED SETTINGS
# ============================================

class AdvancedSettings:
    """Advanced configuration options"""
    
    # Random seed for reproducibility
    random_seed = 42
    
    # Age distribution (must sum to 1.0)
    age_distribution = {
        'pediatric': 0.05,   # 0-17 years (5%)
        'adult': 0.70,       # 18-64 years (70%)
        'senior': 0.25       # 65+ years (25%)
    }
    
    # Procedure distribution by category
    procedure_distribution = {
        'diagnostic': 0.25,      # Exams, X-rays (25%)
        'preventive': 0.35,      # Cleanings, fluoride (35%)
        'restorative': 0.25,     # Fillings, crowns (25%)
        'endodontics': 0.05,     # Root canals (5%)
        'periodontics': 0.05,    # Gum treatments (5%)
        'oral_surgery': 0.03,    # Extractions (3%)
        'prosthodontics': 0.02   # Dentures, bridges (2%)
    }
    
    # Appointment status distribution
    appointment_status_distribution = {
        'completed': 0.75,   # 75% completed
        'scheduled': 0.15,   # 15% future scheduled
        'broken': 0.10       # 10% no-show/broken
    }
    
    # Claim status progression (cumulative)
    claim_status_progression = {
        'sent': 1.0,         # 100% of claims are sent
        'received': 0.85,    # 85% are received
        'paid': 0.70         # 70% are paid
    }
    
    # Payment timing (days after procedure)
    payment_timing = {
        'insurance_payment_days': (30, 60),  # 30-60 days after claim
        'patient_payment_days': (0, 90)      # 0-90 days after procedure
    }
    
    # Adjustment frequency
    adjustment_rate = 0.20  # 20% of procedures have adjustments
    
    # Adjustment amount ranges (percentage of procedure fee)
    adjustment_ranges = {
        'senior_discount': (-0.10, -0.05),      # 5-10% discount
        'insurance_writeoff': (-0.30, -0.10),   # 10-30% writeoff
        'provider_discount': (-0.20, -0.05),    # 5-20% discount
        'cash_discount': (-0.05, -0.03)         # 3-5% discount
    }
    
    # Business hours (for appointment scheduling)
    business_hours = {
        'start': 8,   # 8 AM
        'end': 17,    # 5 PM
        'days': [0, 1, 2, 3, 4]  # Monday-Friday (0=Monday)
    }
    
    # Average appointment durations (minutes)
    appointment_durations = {
        'exam': 30,
        'cleaning': 60,
        'filling': 45,
        'crown_prep': 90,
        'root_canal': 120
    }


# ============================================
# VALIDATION SETTINGS
# ============================================

class ValidationSettings:
    """Data quality validation settings"""
    
    # Enable validation checks
    enable_validation = True
    
    # Validation tolerance for financial balancing
    ar_balance_tolerance = 0.01  # Allow $0.01 rounding difference
    
    # Foreign key validation
    validate_foreign_keys = True
    
    # Date consistency validation
    validate_date_sequences = True
    
    # Business rule validation
    validate_business_rules = True
    
    # Referential integrity checks
    referential_integrity_checks = [
        ('paysplit', 'payment_id', 'payment', 'payment_id'),
        ('paysplit', 'patient_id', 'patient', 'patient_id'),
        ('procedurelog', 'patient_id', 'patient', 'patient_id'),
        ('claim', 'patient_id', 'patient', 'patient_id'),
        ('appointment', 'patient_id', 'patient', 'patient_id'),
    ]


# ============================================
# ENVIRONMENT-SPECIFIC OVERRIDES
# ============================================

import os

def get_config_from_env():
    """Override configuration from environment variables"""
    config = ACTIVE_CONFIG()
    
    # Database overrides
    config.db_host = os.getenv('POSTGRES_HOST', config.db_host)
    config.db_port = int(os.getenv('POSTGRES_PORT', config.db_port))
    config.db_name = os.getenv('POSTGRES_DB', config.db_name)
    config.db_user = os.getenv('POSTGRES_USER', config.db_user)
    config.db_password = os.getenv('POSTGRES_PASSWORD', config.db_password)
    
    # Volume overrides
    config.num_patients = int(os.getenv('NUM_PATIENTS', config.num_patients))
    
    # Date overrides
    if os.getenv('START_DATE'):
        config.start_date = datetime.strptime(
            os.getenv('START_DATE'), 
            '%Y-%m-%d'
        )
    
    return config


# ============================================
# USAGE EXAMPLES
# ============================================

"""
# Example 1: Use configuration in your code
from config import ACTIVE_CONFIG
config = ACTIVE_CONFIG()
print(f"Connecting to: {config.db_host}:{config.db_port}")

# Example 2: Use environment variables
from config import get_config_from_env
config = get_config_from_env()

# Example 3: Override specific settings
config = ProductionDemo()
config.num_patients = 10000  # Custom override

# Example 4: Use different profile
from config import MinimalViableProduct
config = MinimalViableProduct()
"""