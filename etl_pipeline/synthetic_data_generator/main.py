"""
Dental Practice Synthetic Data Generator
========================================

Generates realistic but completely fake dental practice data for dbt model testing
and portfolio deployment. Ensures HIPAA compliance with zero real PHI.

Author: Benjamin Rains
Version: 1.0.0
Date: 2025-01-07
"""

import random
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from decimal import Decimal
import psycopg2
from psycopg2.extras import execute_batch
from faker import Faker

# Initialize Faker for realistic fake data
fake = Faker()
Faker.seed(42)  # Reproducible results
random.seed(42)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ============================================
# CONFIGURATION & CONSTANTS
# ============================================

@dataclass
class GeneratorConfig:
    """Configuration for data generation"""
    # Database connection
    db_host: str = "localhost"
    db_port: int = 5432
    db_name: str = "opendental_demo"
    db_user: str = "postgres"
    db_password: str = "postgres"
    db_schema: str = "raw"
    
    # Data volumes
    num_clinics: int = 5
    num_providers: int = 12
    num_operatories: int = 20
    num_patients: int = 5000
    num_appointments: int = 15000
    num_procedures: int = 20000
    
    # Date range (CRITICAL: dbt models filter to >= 2023-01-01)
    start_date: datetime = datetime(2023, 1, 1)
    end_date: datetime = datetime.now()
    
    # Probabilities
    insurance_coverage_rate: float = 0.60  # 60% have insurance
    appointment_completion_rate: float = 0.75  # 75% completed
    claim_submission_rate: float = 0.90  # 90% of insured procedures get claims
    
    # Business rules
    hygiene_recall_months: int = 6
    standard_fee_match_rate: float = 0.80  # 80% match standard fees exactly


# OpenDental-specific constants (MUST MATCH SOURCE SYSTEM)
APPOINTMENT_STATUS = {
    'SCHEDULED': 1,
    'COMPLETED': 2,
    'UNKNOWN': 3,
    'BROKEN': 5,
    'UNSCHEDULED': 6
}

PROCEDURE_STATUS = {
    'TREATMENT_PLANNED': 1,
    'COMPLETED': 2,
    'ADMIN': 3,
    'EXISTING_PRIOR': 4,
    'REFERRED': 5,
    'ORDERED': 6,
    'CONDITION': 7,
    'IN_PROGRESS': 8
}

CLAIM_STATUS = {
    'UNSENT': 'U',
    'HOLD': 'H',
    'WAITING': 'W',
    'SENT': 'S',
    'RECEIVED': 'R',
    'PAID': 'P'
}

# Adjustment type IDs (CRITICAL: Must match int_adjustments.sql expectations)
ADJUSTMENT_TYPES = {
    'INSURANCE_WRITEOFF': 188,
    'SENIOR_DISCOUNT': 186,
    'PROVIDER_DISCOUNT': 474,
    'PROVIDER_DISCOUNT_2': 475,
    'EMPLOYEE_DISCOUNT': 472,
    'EMPLOYEE_DISCOUNT_2': 485,
    'CASH_DISCOUNT': 9,
    'CASH_DISCOUNT_2': 185,
    'FAMILY_DISCOUNT': 486,
    'FAMILY_DISCOUNT_2': 482,
    'NEW_PATIENT_DISCOUNT': 537,
    'PATIENT_REFUND': 18,
    'PATIENT_REFUND_2': 337,
    'REALLOCATION': 235,
    'REFERRAL_CREDIT': 483,
    'ADMIN_CORRECTION': 549,
    'ADMIN_ADJUSTMENT': 550
}

# Payment type IDs
PAYMENT_TYPES = {
    'ADMIN': 0,
    'CHECK': 71,
    'CASH': 69,
    'CREDIT_CARD': 70,
    'HIGH_VALUE': 391,
    'REFUND': 72
}

# Definition categories (CRITICAL for int_procedure_complete.sql)
DEFINITION_CATEGORIES = {
    'PAYMENT_TYPES': 1,
    'PROCEDURE_STATUS': 4,
    'TREATMENT_AREA': 5,
    'FEE_SCHEDULE_TYPES': 6
}

# Unearned income types (for paysplit)
UNEARNED_TYPES = {
    'NORMAL': 0,
    'UNEARNED_REVENUE': 288,
    'TREATMENT_PLAN_PREPAY': 439
}


# ============================================
# DATABASE CONNECTION
# ============================================

class DatabaseConnection:
    """Manages PostgreSQL database connection"""
    
    def __init__(self, config: GeneratorConfig):
        self.config = config
        self.conn = None
        self.cursor = None
    
    def __enter__(self):
        """Context manager entry"""
        self.conn = psycopg2.connect(
            host=self.config.db_host,
            port=self.config.db_port,
            dbname=self.config.db_name,
            user=self.config.db_user,
            password=self.config.db_password
        )
        self.cursor = self.conn.cursor()
        logger.info(f"Connected to database: {self.config.db_name}")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        if exc_type:
            self.conn.rollback()
            logger.error(f"Transaction rolled back due to error: {exc_val}")
        else:
            self.conn.commit()
            logger.info("Transaction committed successfully")
        
        self.cursor.close()
        self.conn.close()
    
    def execute_batch(self, sql: str, data: List[tuple], page_size: int = 1000):
        """Execute batch insert with progress logging"""
        execute_batch(self.cursor, sql, data, page_size=page_size)
        logger.info(f"Inserted {len(data)} records")


# ============================================
# ID GENERATORS
# ============================================

class IDGenerator:
    """Generates sequential IDs for each table"""
    
    def __init__(self):
        self._counters = {}
    
    def next_id(self, table_name: str) -> int:
        """Get next ID for a table"""
        if table_name not in self._counters:
            self._counters[table_name] = 1
        
        current_id = self._counters[table_name]
        self._counters[table_name] += 1
        return current_id
    
    def reset(self, table_name: str):
        """Reset counter for a table"""
        self._counters[table_name] = 1


# ============================================
# UTILITY FUNCTIONS
# ============================================

def random_date_between(start: datetime, end: datetime) -> datetime:
    """Generate random datetime between start and end"""
    delta = end - start
    random_days = random.randint(0, delta.days)
    random_seconds = random.randint(0, 86400)  # seconds in a day
    return start + timedelta(days=random_days, seconds=random_seconds)


def random_business_datetime(date: datetime) -> datetime:
    """Generate random datetime during business hours (8 AM - 5 PM)"""
    business_start = date.replace(hour=8, minute=0, second=0)
    business_end = date.replace(hour=17, minute=0, second=0)
    delta_seconds = (business_end - business_start).seconds
    random_seconds = random.randint(0, delta_seconds)
    return business_start + timedelta(seconds=random_seconds)


def create_metadata(created_date: datetime, user_id: int = 1) -> Dict:
    """Create standard metadata columns required by all tables"""
    return {
        'SecDateEntry': created_date,
        'DateTStamp': created_date,
        'SecDateTEdit': created_date,
        'SecUserNumEntry': user_id
    }


def optional_fk(value: Optional[int], probability: float = 0.7) -> Optional[int]:
    """Return value with probability, else None (for optional foreign keys)"""
    if value is None:
        return None
    return value if random.random() < probability else None


def to_opendental_boolean(value: bool) -> bool:
    """Convert Python boolean to PostgreSQL boolean"""
    return value


# ============================================
# MAIN GENERATOR CLASS
# ============================================

class SyntheticDataGenerator:
    """Main generator class orchestrating all data generation"""
    
    def __init__(self, config: GeneratorConfig):
        self.config = config
        self.id_gen = IDGenerator()
        self.data_store = {}  # Store generated data for reference
        
    def generate_all(self):
        """Generate all synthetic data"""
        logger.info("=" * 60)
        logger.info("DENTAL PRACTICE SYNTHETIC DATA GENERATOR")
        logger.info("=" * 60)
        logger.info(f"Target database: {self.config.db_name}")
        logger.info(f"Date range: {self.config.start_date.date()} to {self.config.end_date.date()}")
        logger.info(f"Patients: {self.config.num_patients:,}")
        logger.info(f"Appointments: {self.config.num_appointments:,}")
        logger.info(f"Procedures: {self.config.num_procedures:,}")
        logger.info("=" * 60)
        
        with DatabaseConnection(self.config) as db:
            # Phase 1: Foundation (Week 1)
            logger.info("\n[Phase 1] Generating Foundation Data...")
            self._generate_foundation(db)
            
            # Phase 2: Patients (Week 1-2)
            logger.info("\n[Phase 2] Generating Patient Data...")
            self._generate_patients(db)
            
            # Phase 3: Insurance (Week 2)
            logger.info("\n[Phase 3] Generating Insurance Data...")
            self._generate_insurance(db)
            
            # Phase 4: Clinical (Week 2-3)
            logger.info("\n[Phase 4] Generating Clinical Data...")
            self._generate_clinical(db)
            
            # Phase 5: Financial (Week 3)
            logger.info("\n[Phase 5] Generating Financial Data...")
            self._generate_financial(db)
            
            # Phase 6: Supporting (Week 4)
            logger.info("\n[Phase 6] Generating Supporting Data...")
            self._generate_supporting(db)
        
        logger.info("\n" + "=" * 60)
        logger.info("✓ Data generation completed successfully!")
        logger.info("=" * 60)
        self._print_summary()
    
    def _generate_foundation(self, db: DatabaseConnection):
        """Generate foundation tables (clinics, providers, codes, fees)"""
        from generators.foundation_data_generator import FoundationGenerator
        gen = FoundationGenerator(self.config, self.id_gen, self.data_store)
        gen.generate(db)
    
    def _generate_patients(self, db: DatabaseConnection):
        """Generate patient data"""
        from generators.patient_generator import PatientGenerator
        gen = PatientGenerator(self.config, self.id_gen, self.data_store)
        gen.generate(db)
    
    def _generate_insurance(self, db: DatabaseConnection):
        """Generate insurance data"""
        from generators.insurance_generator import InsuranceGenerator
        gen = InsuranceGenerator(self.config, self.id_gen, self.data_store)
        gen.generate(db)
    
    def _generate_clinical(self, db: DatabaseConnection):
        """Generate clinical data (appointments, procedures)"""
        from generators.clinical_generator import ClinicalGenerator
        gen = ClinicalGenerator(self.config, self.id_gen, self.data_store)
        gen.generate(db)
    
    def _generate_financial(self, db: DatabaseConnection):
        """Generate financial data (claims, payments, adjustments)"""
        from generators.financial_generator import FinancialGenerator
        gen = FinancialGenerator(self.config, self.id_gen, self.data_store)
        gen.generate(db)
    
    def _generate_supporting(self, db: DatabaseConnection):
        """Generate supporting data (communications, tasks, etc)"""
        from generators.supporting_generator import SupportingGenerator
        gen = SupportingGenerator(self.config, self.id_gen, self.data_store)
        gen.generate(db)
    
    def _print_summary(self):
        """Print generation summary"""
        logger.info("\nGenerated Record Counts:")
        logger.info("-" * 40)
        for table, count in sorted(self.data_store.get('counts', {}).items()):
            logger.info(f"  {table:30s}: {count:>8,}")


# ============================================
# CLI ENTRY POINT
# ============================================

def main():
    """CLI entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Generate synthetic dental practice data'
    )
    parser.add_argument(
        '--patients', type=int, default=5000,
        help='Number of patients to generate (default: 5000)'
    )
    parser.add_argument(
        '--db-host', default='localhost',
        help='Database host (default: localhost)'
    )
    parser.add_argument(
        '--db-name', default='opendental_demo',
        help='Database name (default: opendental_demo)'
    )
    parser.add_argument(
        '--db-user', default='postgres',
        help='Database user (default: postgres)'
    )
    parser.add_argument(
        '--db-password', default='postgres',
        help='Database password (default: postgres)'
    )
    parser.add_argument(
        '--start-date', default='2023-01-01',
        help='Start date for data (default: 2023-01-01)'
    )
    
    args = parser.parse_args()
    
    # Create configuration
    config = GeneratorConfig(
        db_host=args.db_host,
        db_name=args.db_name,
        db_user=args.db_user,
        db_password=args.db_password,
        num_patients=args.patients,
        start_date=datetime.strptime(args.start_date, '%Y-%m-%d')
    )
    
    # Generate data
    generator = SyntheticDataGenerator(config)
    generator.generate_all()


if __name__ == '__main__':
    main()