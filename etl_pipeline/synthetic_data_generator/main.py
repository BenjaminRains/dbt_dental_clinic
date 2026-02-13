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
    # Updated to match production ratios (based on opendental_analytics analysis)
    # Production: 34,445 patients, 2.34 appts/patient, 22.84 procs/patient
    # Demo: 10,000 patients (29% of production) with same ratios
    num_clinics: int = 5  # Multi-clinic demo (production has 1 clinic)
    num_providers: int = 12  # Demo size (production has 57)
    num_operatories: int = 50  # 10 per clinic for multi-clinic demo (production has 16)
    num_patients: int = 10000  # 29% of production (34,445)
    num_appointments: int = 23379  # 2.34 per patient (production ratio)
    num_procedures: int = 228365  # 22.84 per patient (production ratio)
    
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
        try:
            self.conn = psycopg2.connect(
                host=self.config.db_host,
                port=self.config.db_port,
                dbname=self.config.db_name,
                user=self.config.db_user,
                password=self.config.db_password,
                connect_timeout=5
            )
            self.cursor = self.conn.cursor()
            logger.info(f"Connected to database: {self.config.db_name}")
            return self
        except psycopg2.OperationalError as e:
            # Provide helpful error messages for common connection issues
            error_str = str(e)
            if "Connection refused" in error_str or "10061" in error_str:
                logger.error("❌ Connection refused - Database server is not accessible")
                if self.config.db_host == "localhost" and self.config.db_port in [5434, 5433]:
                    logger.error("")
                    logger.error("💡 This appears to be a port-forwarded connection.")
                    logger.error("   You need to start port forwarding first:")
                    logger.error("")
                    logger.error("   1. Run: aws-ssm-init")
                    logger.error("   2. Run: ssm-port-forward-demo-db")
                    logger.error("   3. Keep that terminal open in the background")
                    logger.error("   4. Then retry this command")
                    logger.error("")
                    logger.error(f"   Expected connection: {self.config.db_host}:{self.config.db_port}")
                elif self.config.db_name == "opendental_demo":
                    logger.error("")
                    logger.error("💡 For demo database access, you have two options:")
                    logger.error("")
                    logger.error("   Option 1: Port Forwarding (Local Development)")
                    logger.error("     1. Run: aws-ssm-init")
                    logger.error("     2. Run: ssm-port-forward-demo-db")
                    logger.error("     3. Keep that terminal open")
                    logger.error("")
                    logger.error("   Option 2: Direct Connection (EC2)")
                    logger.error("     - Make sure you're running from the EC2 instance")
                    logger.error("     - Or update DEMO_POSTGRES_HOST to the EC2 private IP")
                    logger.error("")
            raise
    
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
    import os
    
    # Get defaults from environment variables (if set)
    # Priority: CLI args > Environment variables > Hardcoded defaults
    # SAFETY: Only use DEMO_POSTGRES_* variables, NEVER use POSTGRES_ANALYTICS_* (production database)
    default_db_host = os.environ.get('DEMO_POSTGRES_HOST', 'localhost')
    default_db_port = int(os.environ.get('DEMO_POSTGRES_PORT', '5432'))
    default_db_name = os.environ.get('DEMO_POSTGRES_DB', 'opendental_demo')
    default_db_user = os.environ.get('DEMO_POSTGRES_USER', 'postgres')
    default_db_password = os.environ.get('DEMO_POSTGRES_PASSWORD', 'postgres')
    
    # SAFETY CHECK: Warn if POSTGRES_ANALYTICS_* variables are set (production database)
    # This indicates we're in ETL clinic environment, which should NOT be used for synthetic data
    if os.environ.get('POSTGRES_ANALYTICS_DB'):
        logger.warning("⚠️  WARNING: POSTGRES_ANALYTICS_* environment variables detected!")
        logger.warning("   You appear to be in ETL clinic environment.")
        logger.warning("   This script ONLY writes to opendental_demo (synthetic data).")
        logger.warning("   Make sure you're not accidentally targeting production database.")
        logger.warning("   Using DEMO_POSTGRES_* variables or command-line arguments.")
    
    parser = argparse.ArgumentParser(
        description='Generate synthetic dental practice data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f'''
Environment Variables (used as defaults if not specified):
  DEMO_POSTGRES_HOST     - Database host (default: {default_db_host})
  DEMO_POSTGRES_PORT     - Database port (default: {default_db_port})
  DEMO_POSTGRES_DB       - Database name (default: {default_db_name})
  DEMO_POSTGRES_USER     - Database user (default: {default_db_user})
  DEMO_POSTGRES_PASSWORD - Database password (default: [from env or 'postgres'])

Examples:
  # Use environment variables (if dbt-init -Target demo was run)
  python main.py --patients 5000
  
  # Override specific values
  python main.py --patients 5000 --db-password "mypassword"
  
  # Full manual specification
  python main.py --patients 5000 --db-host localhost --db-port 5432 \\
                 --db-name opendental_demo --db-user postgres --db-password "mypassword"
        '''
    )
    parser.add_argument(
        '--patients', type=int, default=10000,
        help='Number of patients to generate (default: 10000)'
    )
    parser.add_argument(
        '--db-host', default=default_db_host,
        help=f'Database host (default: {default_db_host} from DEMO_POSTGRES_HOST env var or "localhost")'
    )
    parser.add_argument(
        '--db-port', type=int, default=default_db_port,
        help=f'Database port (default: {default_db_port} from DEMO_POSTGRES_PORT env var or 5432)'
    )
    parser.add_argument(
        '--db-name', default=default_db_name,
        help=f'Database name (default: {default_db_name} from DEMO_POSTGRES_DB env var or "opendental_demo")'
    )
    parser.add_argument(
        '--db-user', default=default_db_user,
        help=f'Database user (default: {default_db_user} from DEMO_POSTGRES_USER env var or "postgres")'
    )
    parser.add_argument(
        '--db-password', default=default_db_password,
        help='Database password (default: from DEMO_POSTGRES_PASSWORD env var or "postgres")'
    )
    parser.add_argument(
        '--start-date', default='2023-01-01',
        help='Start date for data (default: 2023-01-01)'
    )
    
    args = parser.parse_args()
    
    # Log which values are being used (for debugging)
    logger.info(f"Using database connection:")
    logger.info(f"  Host: {args.db_host}")
    logger.info(f"  Port: {args.db_port}")
    logger.info(f"  Database: {args.db_name}")
    logger.info(f"  User: {args.db_user}")
    logger.info(f"  Password: {'***' if args.db_password else '[not set]'}")
    
    # CRITICAL SAFETY CHECK: Always enforce opendental_demo
    if args.db_name != 'opendental_demo':
        logger.error(f"❌ ERROR: Target database is '{args.db_name}', not 'opendental_demo'!")
        logger.error("   This script is designed for synthetic data generation ONLY.")
        logger.error("   For safety, this script will NOT run against clinic/test databases.")
        logger.error("   Please specify --db-name opendental_demo or set DEMO_POSTGRES_DB=opendental_demo")
        raise ValueError(f"Cannot run against database '{args.db_name}'. Only 'opendental_demo' is allowed.")
    
    # Create configuration
    config = GeneratorConfig(
        db_host=args.db_host,
        db_port=args.db_port,
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