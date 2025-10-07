#!/usr/bin/env python3
"""
ETL Pipeline CLI Entry Point
"""
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from etl_pipeline.cli.main import cli

if __name__ == '__main__':
    cli()
