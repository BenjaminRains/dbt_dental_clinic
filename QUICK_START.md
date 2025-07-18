# Quick Start Guide - dbt_dental_clinic Environment Manager

## Overview

This guide shows you how to use the local environment manager for the `dbt_dental_clinic` project. The environment manager provides interactive environment selection and project-specific commands.

## Starting from Project Root

### Step 1: Open Terminal in Project Root

Open PowerShell in the project root directory:
```
C:\Users\rains\dbt_dental_clinic>
```

### Step 2: Load the Environment Manager

Choose one of these methods:

**Method A: Load Environment Manager (Recommended)**
```powershell
. .\load_env.ps1
```

**Method B: Load Project Profile**
```powershell
. .\load_project.ps1
```

**Method C: Load Directly**
```powershell
. .\scripts\environment_manager.ps1
```

You should see output like:
```
🔄 Loading dbt_dental_clinic environment manager...
✅ Environment manager loaded!
Available commands:
  dbt-init       - Initialize dbt environment
  etl-init       - Initialize ETL environment (interactive)
  etl-env-status - Show ETL environment details
  env-status     - Check environment status
```

### Step 3: Initialize Your Environment

#### For ETL Operations (Interactive)
```powershell
etl-init
```

You'll be prompted to choose your environment:
```
🔧 ETL Environment Selection
Which environment would you like to use?
  Type 'production' for Production (.env_production)
  Type 'test' for Test (.env_test)
  Type 'cancel' to abort

Enter environment (production/test/cancel):
```

**Choose:**
- `production` - For production database operations
- `test` - For testing and development
- `cancel` - To abort the setup

#### For dbt Operations
```powershell
dbt-init
```

### Step 4: Verify Environment

Check which environment is active:
```powershell
etl-env-status
```

You should see output like:
```
📊 ETL Environment Status:
  Environment: production
  Active: ✅
  Project: dbt_dental_clinic

🔧 Key Environment Variables:
  OPENDENTAL_SOURCE_DB: opendental
  OPENDENTAL_SOURCE_HOST: your-server
```

### Step 5: Run Your Operations

#### ETL Operations
```powershell
# Run schema analysis
python scripts/analyze_opendental_schema.py

# Run ETL pipeline
etl run --full

# Check ETL status
etl status

# Test connections
etl test-connections
```

#### dbt Operations
```powershell
# Run dbt models
dbt run

# Test dbt models
dbt test

# Generate documentation
dbt docs generate
```

## Available Commands

### Environment Management
| Command | Description |
|---------|-------------|
| `dbt-init` | Initialize dbt environment |
| `dbt-deactivate` | Deactivate dbt environment |
| `etl-init` | Initialize ETL environment (interactive) |
| `etl-deactivate` | Deactivate ETL environment |

### ETL Commands (when ETL environment is active)
| Command | Description |
|---------|-------------|
| `etl` | Run ETL pipeline commands |
| `etl-status` | Show ETL pipeline status |
| `etl-validate` | Validate ETL data |
| `etl-run` | Run ETL pipeline |
| `etl-test` | Test ETL connections |
| `etl-env-status` | Show detailed ETL environment status |

### dbt Commands (when dbt environment is active)
| Command | Description |
|---------|-------------|
| `dbt` | Run dbt commands |
| `notebook` | Start Jupyter notebook |
| `format` | Format code with black/isort |
| `lint` | Lint code |
| `test` | Run tests |

### Utility Commands
| Command | Description |
|---------|-------------|
| `env-status` | Show overall environment status |

## Environment Files

The project uses separate environment files for clean isolation:

### Production Environment (`.env_production`)
```bash
ETL_ENVIRONMENT=production
OPENDENTAL_SOURCE_HOST=your-production-server
OPENDENTAL_SOURCE_DB=opendental
OPENDENTAL_SOURCE_USER=readonly_user
OPENDENTAL_SOURCE_PASSWORD=your_password
# ... other production variables
```

### Test Environment (`.env_test`)
```bash
ETL_ENVIRONMENT=test
TEST_OPENDENTAL_SOURCE_HOST=your-test-server
TEST_OPENDENTAL_SOURCE_DB=test_opendental
TEST_OPENDENTAL_SOURCE_USER=test_user
TEST_OPENDENTAL_SOURCE_PASSWORD=your_test_password
# ... other test variables with TEST_ prefix
```

## Complete Workflow Example

```powershell
# 1. Start in project root
cd C:\Users\rains\dbt_dental_clinic

# 2. Load environment manager
. .\load_env.ps1

# 3. Initialize ETL environment
etl-init
# Choose: production

# 4. Verify environment
etl-env-status

# 5. Run schema analysis
python scripts/analyze_opendental_schema.py

# 6. Run ETL operations
etl run --full

# 7. Switch to dbt
etl-deactivate
dbt-init

# 8. Run dbt operations
dbt run
dbt test
```

## Troubleshooting

### Issue: Commands not found
**Solution**: Load the environment manager first
```powershell
. .\load_env.ps1
```

### Issue: Wrong environment loaded
**Solution**: Check current environment and restart
```powershell
etl-env-status
etl-deactivate
etl-init
# Choose correct environment
```

### Issue: Environment files missing
**Solution**: Create environment files from templates
```powershell
# Copy templates
Copy-Item "etl_pipeline\docs\env_production.template" "etl_pipeline\.env_production"
Copy-Item "etl_pipeline\docs\env_test.template" "etl_pipeline\.env_test"

# Edit the files with your database settings
notepad etl_pipeline\.env_production
notepad etl_pipeline\.env_test
```

### Issue: Global profile conflicts
**Solution**: The global profile has been deactivated. Use only the local environment manager.

## Benefits of This Approach

1. **Project Isolation**: No interference between different projects
2. **Interactive Selection**: Choose environment explicitly (production/test)
3. **Explicit Control**: You know exactly which environment manager is active
4. **Clean Setup**: No global state affecting local projects
5. **Version Controlled**: Environment manager is part of the project

## File Structure

```
dbt_dental_clinic/
├── scripts/
│   ├── environment_manager.ps1    # Main environment manager
│   └── project_profile.ps1        # Project profile loader
├── load_env.ps1                   # Simple environment loader
├── load_project.ps1               # Project profile loader
├── .ps1_profile                   # Auto-loading profile
├── ENVIRONMENT_SETUP.md           # Detailed setup guide
└── QUICK_START.md                # This file
```

## Next Steps

1. **Set up environment files**: Create `.env_production` and `.env_test` with your database settings
2. **Test the workflow**: Follow the complete workflow example above
3. **Customize as needed**: Modify the environment manager for your specific needs

For detailed setup instructions, see `ENVIRONMENT_SETUP.md`. 