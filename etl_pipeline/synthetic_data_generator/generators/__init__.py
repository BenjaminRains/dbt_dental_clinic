"""
Generators Module
=================

Data generator modules for synthetic dental practice data.

Available Generators:
- FoundationGenerator: Core configuration (clinics, providers, codes, fees)
- PatientGenerator: Patient demographics and families
- InsuranceGenerator: Insurance carriers, plans, and benefits
- ClinicalGenerator: Appointments and procedures
- FinancialGenerator: Claims, payments, and adjustments (with AR balancing)
- SupportingGenerator: Communications, documents, and referrals

Usage:
    from generators.foundation_data_generator import FoundationGenerator
    from generators.patient_generator import PatientGenerator
    # ... etc
"""

__version__ = '1.0.0'
__author__ = 'Benjamin Rains'

# Import all generators for easy access
from .foundation_data_generator import FoundationGenerator
from .patient_generator import PatientGenerator
from .insurance_generator import InsuranceGenerator
from .clinical_generator import ClinicalGenerator
from .financial_generator import FinancialGenerator
from .supporting_generator import SupportingGenerator

__all__ = [
    'FoundationGenerator',
    'PatientGenerator',
    'InsuranceGenerator',
    'ClinicalGenerator',
    'FinancialGenerator',
    'SupportingGenerator',
]

