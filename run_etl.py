#!/usr/bin/env python3
"""
Simple ETL Pipeline Runner
Run this script to execute the ETL pipeline without installing the package.
"""

import sys
import os
from pathlib import Path

# Add the etl_pipeline directory to Python path
project_root = Path(__file__).parent
etl_pipeline_path = project_root / "etl_pipeline"
sys.path.insert(0, str(etl_pipeline_path))

# Import and run the CLI
from etl_pipeline.cli.main import cli

if __name__ == "__main__":
    cli() 