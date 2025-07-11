# Environment Setup - Quick Start

## Overview

The ETL pipeline now uses **separate environment files** for clean environment separation, following the connection architecture principles.

## Quick Setup

### 1. Create Environment Files

```bash
# Create both production and test environment files
python scripts/setup_environments.py --both

# Or create individually
python scripts/setup_environments.py --production
python scripts/setup_environments.py --test
```

### 2. Configure Your Environments

**Production Environment** (`.env.production`):
```bash
ETL_ENVIRONMENT=production
OPENDENTAL_SOURCE_HOST=your-production-server
OPENDENTAL_SOURCE_DB=opendental
OPENDENTAL_SOURCE_USER=readonly_user
OPENDENTAL_SOURCE_PASSWORD=your_production_password
# ... configure other production variables
```

**Test Environment** (`.env.test`):
```bash
ETL_ENVIRONMENT=test
TEST_OPENDENTAL_SOURCE_HOST=your-test-server
TEST_OPENDENTAL_SOURCE_DB=test_opendental
TEST_OPENDENTAL_SOURCE_USER=test_user
TEST_OPENDENTAL_SOURCE_PASSWORD=your_test_password
# ... configure other test variables
```

### 3. Use Appropriate Environment

**For Production ETL Operations**:
```bash
# Load production environment
source .env.production
# or
export $(cat .env.production | xargs)

# Run ETL operations
python -m etl_pipeline.main
```

**For Testing/Development**:
```bash
# Load test environment
source .env.test
# or
export $(cat .env.test | xargs)

# Run tests
python -m pytest tests/
```

## Benefits of Separate Files

1. **Clean Separation**: No mixing of production and test variables
2. **Security**: Production credentials isolated from test environment
3. **Clarity**: Clear indication of which environment is active
4. **Architecture Compliance**: Follows connection architecture principles
5. **Provider Pattern Support**: Works seamlessly with FileConfigProvider and DictConfigProvider

## Connection Architecture Integration

The separate environment files work perfectly with the connection architecture:

### Production Usage
```python
from etl_pipeline.config import get_settings
from etl_pipeline.core import ConnectionFactory

# Uses FileConfigProvider with .env.production
settings = get_settings()  # ETL_ENVIRONMENT=production
source_engine = ConnectionFactory.get_source_connection(settings)
```

### Test Usage
```python
from etl_pipeline.config import create_test_settings
from etl_pipeline.core import ConnectionFactory

# Uses DictConfigProvider with injected test configuration
test_settings = create_test_settings(
    env_vars={'TEST_OPENDENTAL_SOURCE_HOST': 'test-host'}
)
source_engine = ConnectionFactory.get_source_connection(test_settings)
```

## File Structure

```
etl_pipeline/
├── .env.production          # Production environment (create from template)
├── .env.test               # Test environment (create from template)
├── docs/
│   ├── env.production.template  # Production template
│   ├── env.test.template        # Test template
│   └── environment_setup.md     # Detailed documentation
└── scripts/
    └── setup_environments.py    # Setup script
```

## Migration from Single File

If you're currently using the single `.env` file approach:

1. **Backup your current configuration**:
   ```bash
   cp .env .env.backup
   ```

2. **Create separate environment files**:
   ```bash
   python scripts/setup_environments.py --both
   ```

3. **Migrate your configuration**:
   - Copy production variables to `.env.production`
   - Copy test variables to `.env.test`
   - Remove the old `.env` file

4. **Update your scripts** to use the appropriate environment file

## Security Notes

1. **Never commit real passwords** to version control
2. **Use environment-specific secrets management**
3. **Keep production and test credentials separate**
4. **Use read-only connections** for source databases
5. **Implement proper access controls** for each environment

## Troubleshooting

### Issue: Environment not detected correctly
**Solution**: Ensure `ETL_ENVIRONMENT` is set correctly in your environment file

### Issue: Wrong database connections
**Solution**: Verify you're using the correct environment file for your operation

### Issue: Missing variables
**Solution**: Use `settings.validate_configs()` to check configuration completeness

## Next Steps

1. **Configure your database connections** in the environment files
2. **Test the setup** with the connection architecture
3. **Update your deployment scripts** to use appropriate environment files
4. **Read the full documentation** in `docs/environment_setup.md`

For detailed information about the connection architecture, see `docs/connection_architecture.md`. 