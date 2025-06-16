#!/usr/bin/env python3
"""ETL Pipeline CLI Entry Point."""

import sys
import os
from pathlib import Path

# Add project root to Python path (same pattern as your working commands)
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

def main():
    """Main CLI entry point."""
    try:
        from etl_pipeline.cli.main import main as cli_main
        cli_main()
    except Exception as e:
        print(f"❌ CLI Error: {str(e)}")
        sys.exit(1)

if __name__ == '__main__':
    main()