#!/usr/bin/env python3
"""
Environment Setup Script

This script helps create separate environment files for production and test environments
according to the connection architecture principles.

Usage:
    python scripts/setup_environments.py --production
    python scripts/setup_environments.py --test
    python scripts/setup_environments.py --both
"""

import argparse
import os
import shutil
from pathlib import Path


def setup_environment_files():
    """Create separate environment files for production and test."""
    
    # Get the script directory and project root
    script_dir = Path(__file__).parent
    project_root = script_dir.parent
    docs_dir = project_root / "docs"
    
    # Template files
    production_template = docs_dir / "env_production.template"
    test_template = docs_dir / "env_test.template"
    
    # Target files
    production_env = project_root / ".env_production"
    test_env = project_root / ".env_test"
    
    parser = argparse.ArgumentParser(
        description="Setup environment files for ETL pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/setup_environments.py --production  # Create .env_production
  python scripts/setup_environments.py --test        # Create .env_test
  python scripts/setup_environments.py --both        # Create both files
        """
    )
    
    parser.add_argument(
        "--production", 
        action="store_true",
        help="Create .env_production file"
    )
    parser.add_argument(
        "--test", 
        action="store_true",
        help="Create .env_test file"
    )
    parser.add_argument(
        "--both", 
        action="store_true",
        help="Create both .env_production and .env_test files"
    )
    parser.add_argument(
        "--force", 
        action="store_true",
        help="Overwrite existing files"
    )
    
    args = parser.parse_args()
    
    # If no arguments provided, show help
    if not any([args.production, args.test, args.both]):
        parser.print_help()
        return
    
    print("ETL Pipeline Environment Setup")
    print("=" * 40)
    
    # Create production environment file
    if args.production or args.both:
        if production_template.exists():
            if production_env.exists() and not args.force:
                print(f"⚠️  {production_env} already exists. Use --force to overwrite.")
            else:
                shutil.copy2(production_template, production_env)
                print(f"✅ Created {production_env}")
                print("   Please configure your production database settings.")
        else:
            print(f"❌ Template file {production_template} not found.")
    
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
    print("3. Use .env_production for production ETL operations")
    print("4. Use .env_test for testing and development")
    print("\nFor more information, see docs/environment_setup.md")


if __name__ == "__main__":
    setup_environment_files() 