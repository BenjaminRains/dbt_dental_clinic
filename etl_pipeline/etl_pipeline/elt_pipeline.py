"""
Intelligent ETL pipeline implementation.
Handles the extraction and loading of data from OpenDental to analytics
using data-driven configuration and priority-based processing.
"""

# This file has been refactored. All pipeline orchestration, table processing,
# and priority logic have been moved to the orchestration modules:
#   - orchestration/pipeline_orchestrator.py
#   - orchestration/table_processor.py
#   - orchestration/priority_processor.py
#
# Only minimal configuration, utility functions, or CLI entrypoints should remain here.

import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Add project root to Python path
current_file = Path(__file__).resolve()
project_root = current_file.parent.parent
sys.path.insert(0, str(project_root))

# Load environment variables
load_dotenv()

# (Optionally, add CLI entrypoint or utility imports here)

if __name__ == "__main__":
    print("This file is now a stub. Use the orchestration modules to run the ETL pipeline.")