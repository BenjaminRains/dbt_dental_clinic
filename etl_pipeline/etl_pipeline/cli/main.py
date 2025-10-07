#!/usr/bin/env python3
"""
ETL Pipeline CLI Main Entry Point
"""
import os
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import click
from etl_pipeline.cli.commands import run, status, test_connections, update_schema

@click.group()
def cli():
    """ETL Pipeline CLI for Dental Clinic Data Engineering."""
    pass

# Add core commands to the CLI group
cli.add_command(run)
cli.add_command(status)
cli.add_command(test_connections)
cli.add_command(update_schema)

def main():
    """Main CLI entry point."""
    cli()

if __name__ == '__main__':
    main()