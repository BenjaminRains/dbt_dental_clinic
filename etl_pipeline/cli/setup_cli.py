"""Setup script to install ETL CLI as a command-line tool."""

import subprocess
import sys
from pathlib import Path

def setup_cli():
    """Set up the ETL CLI for command-line usage."""
    print("🔧 Setting up ETL Pipeline CLI...")
    
    # Check if we're in a pipenv environment
    try:
        result = subprocess.run(['pipenv', '--version'], 
                              capture_output=True, text=True)
        if result.returncode == 0:
            print("✅ Pipenv detected")
            
            # Install CLI dependencies
            print("📦 Installing CLI dependencies...")
            deps = ['click>=8.0.0', 'tabulate>=0.9.0', 'tqdm>=4.60.0']
            
            for dep in deps:
                subprocess.run(['pipenv', 'install', dep], check=True)
            
            print("✅ Dependencies installed")
            
        else:
            print("❌ Pipenv not found. Please install pipenv first.")
            return False
            
    except FileNotFoundError:
        print("❌ Pipenv not found. Please install pipenv first.")
        return False
    
    # Create necessary directories
    dirs = [
        'etl_pipeline/logs',
        'etl_pipeline/config',
        'etl_pipeline/cli'
    ]
    
    for directory in dirs:
        Path(directory).mkdir(parents=True, exist_ok=True)
        print(f"📁 Created/verified directory: {directory}")
    
    # Create CLI entry script
    cli_script = """#!/usr/bin/env python3
'''ETL Pipeline CLI Entry Script'''
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from etl_pipeline.cli.main import main

if __name__ == '__main__':
    main()
"""
    
    with open('etl-cli', 'w') as f:
        f.write(cli_script)
    
    # Make executable on Unix systems
    import stat
    st = Path('etl-cli').stat()
    Path('etl-cli').chmod(st.st_mode | stat.S_IEXEC)
    
    print("✅ CLI setup completed!")
    print("\n📋 Usage:")
    print("  Via pipenv: pipenv run python -m etl_pipeline.cli.main --help")
    print("  Via script: ./etl-cli --help")
    print("  Via Python: python -m etl_pipeline --help")
    
    return True

if __name__ == '__main__':
    setup_cli() 