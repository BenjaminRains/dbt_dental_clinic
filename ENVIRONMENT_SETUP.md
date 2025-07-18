# Environment Setup for dbt_dental_clinic

## Overview

This project uses a **local environment manager** instead of a global PowerShell profile. This ensures project isolation and prevents conflicts between different projects.

## Quick Start

### 1. Load the Environment Manager

```powershell
# In the dbt_dental_clinic project directory
. .\load_env.ps1
```

### 2. Initialize Your Environment

**For dbt (Data Build Tool):**
```powershell
dbt-init
```

**For ETL (Extract, Transform, Load):**
```powershell
etl-init
# You'll be prompted to choose:
# - Type 'production' for Production (.env_production)
# - Type 'test' for Test (.env_test)
# - Type 'cancel' to abort
```

### 3. Check Environment Status

```powershell
# Check overall environment status
env-status

# Check detailed ETL environment status
etl-env-status
```

## Available Commands

### Environment Management
- `dbt-init` - Initialize dbt environment
- `dbt-deactivate` - Deactivate dbt environment
- `etl-init` - Initialize ETL environment (interactive)
- `etl-deactivate` - Deactivate ETL environment

### dbt Commands (when dbt environment is active)
- `dbt` - Run dbt commands
- `notebook` - Start Jupyter notebook
- `format` - Format code with black/isort
- `lint` - Lint code
- `test` - Run tests

### ETL Commands (when ETL environment is active)
- `etl` - Run ETL pipeline commands
- `etl-status` - Show ETL pipeline status
- `etl-validate` - Validate ETL data
- `etl-run` - Run ETL pipeline
- `etl-test` - Test ETL connections
- `etl-env-status` - Show detailed ETL environment status

### Utility Commands
- `env-status` - Show overall environment status

## Environment Files

The project uses separate environment files for clean isolation:

### Production Environment (`.env_production`)
```bash
ETL_ENVIRONMENT=production
OPENDENTAL_SOURCE_HOST=your-production-server
OPENDENTAL_SOURCE_DB=opendental
# ... other production variables
```

### Test Environment (`.env_test`)
```bash
ETL_ENVIRONMENT=test
TEST_OPENDENTAL_SOURCE_HOST=your-test-server
TEST_OPENDENTAL_SOURCE_DB=test_opendental
# ... other test variables with TEST_ prefix
```

## Benefits of Local Environment Manager

1. **Project Isolation**: No interference between different projects
2. **Explicit Control**: You know exactly which environment manager is active
3. **Interactive Selection**: Choose environment explicitly (production/test)
4. **Clean Setup**: No global state affecting local projects
5. **Version Control**: Environment manager is part of the project

## Troubleshooting

### Issue: Commands not found
**Solution**: Load the environment manager first
```powershell
. .\load_env.ps1
```

### Issue: Wrong environment loaded
**Solution**: Use `etl-env-status` to check current environment, then restart with correct choice

### Issue: Environment files missing
**Solution**: Create environment files from templates
```powershell
# Copy templates
Copy-Item "etl_pipeline\docs\env_production.template" "etl_pipeline\.env_production"
Copy-Item "etl_pipeline\docs\env_test.template" "etl_pipeline\.env_test"
```

## Workflow Example

```powershell
# 1. Load environment manager
. .\load_env.ps1

# 2. Initialize ETL environment
etl-init
# Choose: production

# 3. Check environment
etl-env-status

# 4. Run ETL operations
etl run --full

# 5. Switch to dbt
etl-deactivate
dbt-init

# 6. Run dbt operations
dbt run
``` 