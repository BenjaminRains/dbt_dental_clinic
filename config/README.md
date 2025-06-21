# Configuration Directory

This directory contains configuration files for the dbt_dental_clinic project and serves as the
 **configuration layer** for the Data Engineering Environment Manager.

## Architecture Overview

The configuration system works in layers:

```
┌─────────────────────────────────────────────────────────────┐
│                Data Engineering Environment Manager         │
│  (User Interface Layer - scripts/environment_manager.ps1)  │
│  • Virtual environment management                           │
│  • Command aliases (dbt, etl, etc.)                        │
│  • Environment switching                                    │
│  • User prompts and status                                  │
└─────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────┐
│                    Script Execution Layer                   │
│  (scripts/run_dbt.ps1, scripts/run_dbt.sh)                 │
│  • Bridges user interface with configuration                │
│  • Calls config/load_env.py to load environment variables  │
│  • Executes commands in proper environment                  │
└─────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────┐
│                   Configuration Layer                       │
│  (This directory - config/)                                 │
│  • Environment variable loading                             │
│  • Database connection configuration                        │
│  • Security guidelines                                      │
│  • Project configuration scripts                            │
└─────────────────────────────────────────────────────────────┘
```

## Files

- `load_env.py`: Script to load environment variables from the `.env` file in project root
- `CONNECTION_SAFETY.md`: Database connection security guidelines

## Environment Files

### Project Root (Main Configuration)
- `.env`: Contains environment variables for database connections and dbt configuration
- `.env.template`: Template for creating a new `.env` file
- `dbt_dental_clinic.code-workspace`: VS Code workspace configuration

### ETL Pipeline (ETL-Specific Configuration)
- `etl_pipeline/.env.template`: Template for ETL-specific environment variables

## How It Works

### Command Execution Flow

When you run a dbt command through the Environment Manager:

```powershell
[dbt:dbt_dental_clinic] C:\Users\rains\dbt_dental_clinic> dbt clean
```

The following happens:

1. **Environment Manager** checks if dbt environment is active
2. **Environment Manager** calls the `dbt` alias
3. **`dbt` alias** executes `run_dbt.ps1` with arguments
4. **`run_dbt.ps1`** calls `config/load_env.py` to load environment variables
5. **`load_env.py`** loads variables from `.env` file in project root
6. **`run_dbt.ps1`** executes `pipenv run dbt clean`

### Configuration Loading

The `load_env.py` script:
- Locates the project root directory
- Loads environment variables from `.env` file
- Makes database connection settings available to dbt
- Enables secure credential management

## Usage

### Environment Variables

The project uses environment variables for database connections and other configuration. 
These are loaded from the `.env` file in the project root.

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
- Use different credentials for development/staging/production

## Integration with Environment Manager

The config directory is **automatically used** by the Environment Manager:

- **No manual configuration needed** - the Environment Manager handles everything
- **Automatic environment variable loading** when running dbt commands
- **Secure credential management** through `.env` files
- **Project-specific configuration** for different environments

The Environment Manager provides the **user interface**, while this config directory provides the **configuration infrastructure** that makes it all work seamlessly. 