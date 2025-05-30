# Get the directory where this script is located
$SCRIPT_DIR = Split-Path -Parent $MyInvocation.MyCommand.Path
$ETL_ROOT = Split-Path -Parent $SCRIPT_DIR

# Set PYTHONPATH to include ETL pipeline
$env:PYTHONPATH = "$ETL_ROOT;$env:PYTHONPATH"

# Load ETL environment variables
& "$SCRIPT_DIR\set_env.ps1"

# Run the ETL pipeline
Set-Location -Path $ETL_ROOT
python test_connections.py $args 