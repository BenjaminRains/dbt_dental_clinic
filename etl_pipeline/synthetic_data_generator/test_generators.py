"""
Test Script for Synthetic Data Generators
==========================================

Tests all generator modules to ensure they work correctly:
- FoundationGenerator: Clinics, providers, codes, fees
- PatientGenerator: Patient demographics
- InsuranceGenerator: Insurance carriers, plans, benefits
- ClinicalGenerator: Appointments and procedures
- FinancialGenerator: Claims, payments, adjustments
- SupportingGenerator: Communications, documents

Usage:
    python test_generators.py
    python test_generators.py --generator foundation
    python test_generators.py --generator all --patients 100
"""

import sys
import os
import logging
import argparse
from datetime import datetime
from typing import Dict, List, Optional

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from main import (
    GeneratorConfig,
    DatabaseConnection,
    IDGenerator,
    SyntheticDataGenerator
)

# Import all generators
from generators.foundation_data_generator import FoundationGenerator
from generators.patient_generator import PatientGenerator
from generators.insurance_generator import InsuranceGenerator
from generators.clinical_generator import ClinicalGenerator
from generators.financial_generator import FinancialGenerator
from generators.supporting_generator import SupportingGenerator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class GeneratorTester:
    """Test harness for synthetic data generators"""
    
    def __init__(self, config: GeneratorConfig, use_test_db: bool = True):
        self.config = config
        self.use_test_db = use_test_db
        self.id_gen = IDGenerator()
        self.data_store = {}
        self.test_results = {}
        
        # Override database name for testing if needed
        if use_test_db:
            # Use a test database name (or same database with test schema)
            self.config.db_name = config.db_name  # Keep same for now
            self.config.db_schema = "raw"  # Ensure we use raw schema
    
    def test_foundation(self) -> bool:
        """Test FoundationGenerator"""
        logger.info("\n" + "=" * 60)
        logger.info("Testing FoundationGenerator")
        logger.info("=" * 60)
        
        try:
            with DatabaseConnection(self.config) as db:
                gen = FoundationGenerator(self.config, self.id_gen, self.data_store)
                gen.generate(db)
            
            # Verify data was generated
            counts = self.data_store.get('counts', {})
            required_tables = ['clinic', 'provider', 'procedurecode', 'feesched', 'fee']
            
            all_present = True
            for table in required_tables:
                if table in counts and counts[table] > 0:
                    logger.info(f"✓ {table}: {counts[table]} records")
                else:
                    logger.error(f"✗ {table}: Missing or zero records")
                    all_present = False
            
            self.test_results['foundation'] = all_present
            return all_present
            
        except Exception as e:
            logger.error(f"✗ FoundationGenerator failed: {e}", exc_info=True)
            self.test_results['foundation'] = False
            return False
    
    def test_patients(self) -> bool:
        """Test PatientGenerator"""
        logger.info("\n" + "=" * 60)
        logger.info("Testing PatientGenerator")
        logger.info("=" * 60)
        
        try:
            with DatabaseConnection(self.config) as db:
                gen = PatientGenerator(self.config, self.id_gen, self.data_store)
                gen.generate(db)
            
            # Verify data was generated
            counts = self.data_store.get('counts', {})
            required_tables = ['patient', 'patientnote']
            
            all_present = True
            for table in required_tables:
                if table in counts and counts[table] > 0:
                    logger.info(f"✓ {table}: {counts[table]} records")
                else:
                    logger.error(f"✗ {table}: Missing or zero records")
                    all_present = False
            
            # Check patient count matches config
            if 'patient' in counts:
                expected = self.config.num_patients
                actual = counts['patient']
                if actual == expected:
                    logger.info(f"✓ Patient count matches: {actual}")
                else:
                    logger.warning(f"⚠ Patient count mismatch: expected {expected}, got {actual}")
            
            self.test_results['patients'] = all_present
            return all_present
            
        except Exception as e:
            logger.error(f"✗ PatientGenerator failed: {e}", exc_info=True)
            self.test_results['patients'] = False
            return False
    
    def test_insurance(self) -> bool:
        """Test InsuranceGenerator"""
        logger.info("\n" + "=" * 60)
        logger.info("Testing InsuranceGenerator")
        logger.info("=" * 60)
        
        try:
            with DatabaseConnection(self.config) as db:
                gen = InsuranceGenerator(self.config, self.id_gen, self.data_store)
                gen.generate(db)
            
            # Verify data was generated
            counts = self.data_store.get('counts', {})
            required_tables = ['carrier', 'insplan', 'patplan']
            
            all_present = True
            for table in required_tables:
                if table in counts and counts[table] > 0:
                    logger.info(f"✓ {table}: {counts[table]} records")
                else:
                    logger.error(f"✗ {table}: Missing or zero records")
                    all_present = False
            
            self.test_results['insurance'] = all_present
            return all_present
            
        except Exception as e:
            logger.error(f"✗ InsuranceGenerator failed: {e}", exc_info=True)
            self.test_results['insurance'] = False
            return False
    
    def test_clinical(self) -> bool:
        """Test ClinicalGenerator"""
        logger.info("\n" + "=" * 60)
        logger.info("Testing ClinicalGenerator")
        logger.info("=" * 60)
        
        try:
            with DatabaseConnection(self.config) as db:
                gen = ClinicalGenerator(self.config, self.id_gen, self.data_store)
                gen.generate(db)
            
            # Verify data was generated
            counts = self.data_store.get('counts', {})
            required_tables = ['appointment', 'procedurelog']
            
            all_present = True
            for table in required_tables:
                if table in counts and counts[table] > 0:
                    logger.info(f"✓ {table}: {counts[table]} records")
                else:
                    logger.error(f"✗ {table}: Missing or zero records")
                    all_present = False
            
            self.test_results['clinical'] = all_present
            return all_present
            
        except Exception as e:
            logger.error(f"✗ ClinicalGenerator failed: {e}", exc_info=True)
            self.test_results['clinical'] = False
            return False
    
    def test_financial(self) -> bool:
        """Test FinancialGenerator"""
        logger.info("\n" + "=" * 60)
        logger.info("Testing FinancialGenerator")
        logger.info("=" * 60)
        
        try:
            with DatabaseConnection(self.config) as db:
                gen = FinancialGenerator(self.config, self.id_gen, self.data_store)
                gen.generate(db)
            
            # Verify data was generated
            counts = self.data_store.get('counts', {})
            required_tables = ['claim', 'payment', 'paysplit', 'adjustment']
            
            all_present = True
            for table in required_tables:
                if table in counts and counts[table] > 0:
                    logger.info(f"✓ {table}: {counts[table]} records")
                else:
                    logger.error(f"✗ {table}: Missing or zero records")
                    all_present = False
            
            self.test_results['financial'] = all_present
            return all_present
            
        except Exception as e:
            logger.error(f"✗ FinancialGenerator failed: {e}", exc_info=True)
            self.test_results['financial'] = False
            return False
    
    def test_supporting(self) -> bool:
        """Test SupportingGenerator"""
        logger.info("\n" + "=" * 60)
        logger.info("Testing SupportingGenerator")
        logger.info("=" * 60)
        
        try:
            with DatabaseConnection(self.config) as db:
                gen = SupportingGenerator(self.config, self.id_gen, self.data_store)
                gen.generate(db)
            
            # Verify data was generated
            counts = self.data_store.get('counts', {})
            required_tables = ['commlog', 'document']
            
            all_present = True
            for table in required_tables:
                if table in counts and counts[table] > 0:
                    logger.info(f"✓ {table}: {counts[table]} records")
                else:
                    logger.error(f"✗ {table}: Missing or zero records")
                    all_present = False
            
            self.test_results['supporting'] = all_present
            return all_present
            
        except Exception as e:
            logger.error(f"✗ SupportingGenerator failed: {e}", exc_info=True)
            self.test_results['supporting'] = False
            return False
    
    def test_all(self) -> bool:
        """Test all generators in sequence"""
        logger.info("\n" + "=" * 60)
        logger.info("TESTING ALL GENERATORS")
        logger.info("=" * 60)
        logger.info(f"Database: {self.config.db_name}")
        logger.info(f"Patients: {self.config.num_patients}")
        logger.info(f"Date Range: {self.config.start_date.date()} to {self.config.end_date.date()}")
        logger.info("=" * 60)
        
        results = []
        
        # Test in order (foundation must come first)
        results.append(("Foundation", self.test_foundation()))
        results.append(("Patients", self.test_patients()))
        results.append(("Insurance", self.test_insurance()))
        results.append(("Clinical", self.test_clinical()))
        results.append(("Financial", self.test_financial()))
        results.append(("Supporting", self.test_supporting()))
        
        # Print summary
        logger.info("\n" + "=" * 60)
        logger.info("TEST SUMMARY")
        logger.info("=" * 60)
        
        all_passed = True
        for name, passed in results:
            status = "✓ PASS" if passed else "✗ FAIL"
            logger.info(f"{name:15s}: {status}")
            if not passed:
                all_passed = False
        
        logger.info("=" * 60)
        
        if all_passed:
            logger.info("✓ All tests passed!")
        else:
            logger.error("✗ Some tests failed!")
        
        # Print data counts
        logger.info("\nGenerated Record Counts:")
        logger.info("-" * 40)
        counts = self.data_store.get('counts', {})
        for table, count in sorted(counts.items()):
            logger.info(f"  {table:30s}: {count:>8,}")
        
        total = sum(counts.values())
        logger.info(f"  {'TOTAL':30s}: {total:>8,}")
        
        return all_passed
    
    def verify_database_connection(self) -> bool:
        """Verify database connection works"""
        logger.info("Verifying database connection...")
        try:
            with DatabaseConnection(self.config) as db:
                # Try a simple query
                db.cursor.execute("SELECT 1")
                result = db.cursor.fetchone()
                if result and result[0] == 1:
                    logger.info(f"✓ Connected to database: {self.config.db_name}")
                    return True
                else:
                    logger.error("✗ Database connection test failed")
                    return False
        except Exception as e:
            logger.error(f"✗ Database connection failed: {e}")
            return False


def load_env_config() -> Dict[str, str]:
    """Load configuration from .env_demo if it exists"""
    config = {}
    env_file = os.path.join(os.path.dirname(__file__), '.env_demo')
    
    if os.path.exists(env_file):
        logger.info(f"Loading configuration from {env_file}")
        with open(env_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    config[key.strip()] = value.strip()
    
    return config


def main():
    """Main test entry point"""
    parser = argparse.ArgumentParser(
        description='Test synthetic data generators'
    )
    parser.add_argument(
        '--generator', 
        choices=['foundation', 'patients', 'insurance', 'clinical', 'financial', 'supporting', 'all'],
        default='all',
        help='Generator to test (default: all)'
    )
    parser.add_argument(
        '--patients', type=int, default=100,
        help='Number of patients for testing (default: 100)'
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
        '--db-password',
        help='Database password (required if not in .env_demo)'
    )
    parser.add_argument(
        '--start-date', default='2023-01-01',
        help='Start date for data (default: 2023-01-01)'
    )
    parser.add_argument(
        '--skip-connection-test', action='store_true',
        help='Skip database connection test'
    )
    
    args = parser.parse_args()
    
    # Load environment config
    env_config = load_env_config()
    
    # Build configuration
    db_password = args.db_password or env_config.get('DEMO_POSTGRES_PASSWORD')
    if not db_password:
        logger.error("Database password required! Provide via --db-password or .env_demo")
        sys.exit(1)
    
    config = GeneratorConfig(
        db_host=args.db_host or env_config.get('DEMO_POSTGRES_HOST', 'localhost'),
        db_port=int(env_config.get('DEMO_POSTGRES_PORT', '5432')),
        db_name=args.db_name or env_config.get('DEMO_POSTGRES_DB', 'opendental_demo'),
        db_user=args.db_user or env_config.get('DEMO_POSTGRES_USER', 'postgres'),
        db_password=db_password,
        num_patients=args.patients,
        start_date=datetime.strptime(args.start_date, '%Y-%m-%d')
    )
    
    # Create tester
    tester = GeneratorTester(config)
    
    # Test database connection
    if not args.skip_connection_test:
        if not tester.verify_database_connection():
            logger.error("Cannot proceed without database connection")
            sys.exit(1)
    
    # Run tests
    success = False
    if args.generator == 'all':
        success = tester.test_all()
    elif args.generator == 'foundation':
        success = tester.test_foundation()
    elif args.generator == 'patients':
        success = tester.test_patients()
    elif args.generator == 'insurance':
        success = tester.test_insurance()
    elif args.generator == 'clinical':
        success = tester.test_clinical()
    elif args.generator == 'financial':
        success = tester.test_financial()
    elif args.generator == 'supporting':
        success = tester.test_supporting()
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()

