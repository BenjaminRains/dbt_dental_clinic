# Configuration Directory

This directory contains configuration files for the dbt_dental_clinic project.

## Files

- `.env`: Contains environment variables for database connections and other configuration
- `.env.template`: Template for creating a new `.env` file
- `load_env.py`: Script to load environment variables from the `.env` file
- `powershell_profile.txt`: PowerShell profile configuration
- `mdc_opendental_config.txt`: OpenDental configuration

## Usage

### Environment Variables

The project uses environment variables for database connections and other configuration. 
These are loaded from the `.env` file in this directory.

To use the environment variables in your dbt project:

1. Copy `.env.template` to `.env` and fill in your values
2. Use the `load_env.py` script to load the environment variables

### Running dbt

To run dbt with the environment variables loaded, use the provided scripts:

- `scripts/run_dbt.ps1`: PowerShell script for Windows
- `scripts/run_dbt.sh`: Bash script for Unix-like systems

Example:

```powershell
# Windows
.\scripts\run_dbt.ps1 run

# Unix-like
./scripts/run_dbt.sh run
```

## Security

- Never commit the `.env` file to version control
- Keep your database credentials secure
- Rotate credentials regularly 