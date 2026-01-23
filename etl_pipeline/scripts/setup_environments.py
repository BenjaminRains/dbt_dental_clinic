#!/usr/bin/env python3
"""
Environment Setup Script

This script helps create separate environment files for clinic and test environments
according to the connection architecture principles.

Usage:
    python scripts/setup_environments.py --clinic
    python scripts/setup_environments.py --test
    python scripts/setup_environments.py --both
"""

import argparse
import os
import shutil
from pathlib import Path


def setup_environment_files():
    """Create separate environment files for clinic and test."""
    
    # Get the script directory and project root
    script_dir = Path(__file__).parent
    project_root = script_dir.parent
    etl_pipeline_dir = project_root / "etl_pipeline"
    
    # Template files
    clinic_template = etl_pipeline_dir / ".env_clinic.template"
    test_template = etl_pipeline_dir / ".env_test.template"
    
    # Target files
    clinic_env = etl_pipeline_dir / ".env_clinic"
    test_env = etl_pipeline_dir / ".env_test"
    
    parser = argparse.ArgumentParser(
        description="Setup environment files for ETL pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/setup_environments.py --clinic  # Create .env_clinic
  python scripts/setup_environments.py --test    # Create .env_test
  python scripts/setup_environments.py --both    # Create both files
        """
    )
    
    parser.add_argument(
        "--clinic", 
        action="store_true",
        help="Create .env_clinic file"
    )
    parser.add_argument(
        "--test", 
        action="store_true",
        help="Create .env_test file"
    )
    parser.add_argument(
        "--both", 
        action="store_true",
        help="Create both .env_clinic and .env_test files"
    )
    parser.add_argument(
        "--force", 
        action="store_true",
        help="Overwrite existing files"
    )
    
    args = parser.parse_args()
    
    # If no arguments provided, show help
    if not any([args.clinic, args.test, args.both]):
        parser.print_help()
        return
    
    print("ETL Pipeline Environment Setup")
    print("=" * 40)
    
    # Create clinic environment file
    if args.clinic or args.both:
        if clinic_template.exists():
            if clinic_env.exists() and not args.force:
                print(f"⚠️  {clinic_env} already exists. Use --force to overwrite.")
            else:
                shutil.copy2(clinic_template, clinic_env)
                print(f"✅ Created {clinic_env}")
                print("   Please configure your clinic database settings.")
        else:
            print(f"❌ Template file {clinic_template} not found.")
    
    # Create test environment file
    if args.test or args.both:
        if test_template.exists():
            if test_env.exists() and not args.force:
                print(f"⚠️  {test_env} already exists. Use --force to overwrite.")
            else:
                shutil.copy2(test_template, test_env)
                print(f"✅ Created {test_env}")
                print("   Please configure your test database settings.")
        else:
            print(f"❌ Template file {test_template} not found.")
    
    print("\nNext Steps:")
    print("1. Configure database connection settings in the created files")
    print("2. Set appropriate passwords and connection details")
    print("3. Use .env_clinic for clinic ETL operations (real PHI data)")
    print("4. Use .env_test for testing and development")
    print("\nFor more information, see docs/environment_setup.md")


if __name__ == "__main__":
    setup_environment_files() 