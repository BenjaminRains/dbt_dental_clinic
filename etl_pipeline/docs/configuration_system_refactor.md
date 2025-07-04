# Configuration System Refactor

## Overview

The current configuration system has several architectural issues that prevent proper testing isolation and make the system difficult to maintain. This document outlines the required changes to create a more robust, testable, and flexible configuration system.




## Current Issues

### 1. Global Instance Anti-Pattern
- Global settings instance created at module import time
- No test isolation - all tests share the same instance
- Environment detection happens before test environment variables can be set

### 2. Configuration File Hardcoding
- Configuration files are hardcoded to specific paths
- No ability to inject test configurations
- Tight coupling to file system structure

### 3. Environment Detection Timing
- Environment detected once at module import
- No runtime flexibility
- Tests cannot override environment after import

### 4. Environment Variable Validation
- Too permissive validation logic
- Unclear validation rules
- No environment-specific validation requirements

## Proposed Solution

### Phase 1: Core Architecture Changes

#### 1.1 Remove Global Instance Pattern

**Current Problem:**
```python
# settings.py - module level
settings = get_global_settings()  # Created at import time
```

**Solution:**
```python
# settings.py - replace with lazy initialization
_settings_instance = None

def get_settings(environment=None):
    """Get settings instance with lazy initialization."""
    global _settings_instance
    if _settings_instance is None:
        _settings_instance = create_settings(environment)
    return _settings_instance

def create_settings(environment=None):
    """Create a new settings instance."""
    if environment is None:
        environment = detect_environment()
    return Settings(environment=environment)

def reset_settings():
    """Reset global settings instance (for testing)."""
    global _settings_instance
    _settings_instance = None
```

#### 1.2 Add Configuration Provider Pattern

**New File: `etl_pipeline/config/providers.py`**
```python
from abc import ABC, abstractmethod
from typing import Dict, Optional
from pathlib import Path

class ConfigurationProvider(ABC):
    """Abstract interface for configuration sources."""
    
    @abstractmethod
    def get_pipeline_config(self) -> Dict:
        """Get pipeline configuration."""
        pass
    
    @abstractmethod
    def get_tables_config(self) -> Dict:
        """Get tables configuration."""
        pass
    
    @abstractmethod
    def get_environment_variables(self) -> Dict:
        """Get environment variables."""
        pass

class FileConfigurationProvider(ConfigurationProvider):
    """File-based configuration provider."""
    
    def __init__(self, config_dir: Path):
        self.config_dir = config_dir
    
    def get_pipeline_config(self) -> Dict:
        # Load from pipeline.yml/yaml
        pass
    
    def get_tables_config(self) -> Dict:
        # Load from tables.yml/yaml
        pass
    
    def get_environment_variables(self) -> Dict:
        # Return current environment variables
        return dict(os.environ)

class TestConfigurationProvider(ConfigurationProvider):
    """Test configuration provider with injected data."""
    
    def __init__(self, pipeline_config: Dict, tables_config: Dict, 
                 env_vars: Dict):
        self.pipeline_config = pipeline_config
        self.tables_config = tables_config
        self.env_vars = env_vars
    
    def get_pipeline_config(self) -> Dict:
        return self.pipeline_config
    
    def get_tables_config(self) -> Dict:
        return self.tables_config
    
    def get_environment_variables(self) -> Dict:
        return self.env_vars
```

#### 1.3 Update Settings Class

**Modified File: `etl_pipeline/config/settings.py`**

```python
class Settings:
    """Configuration settings manager with dependency injection."""
    
    def __init__(self, environment=None, provider=None):
        """
        Initialize settings with optional provider injection.
        
        Args:
            environment: Environment name ('production', 'test')
            provider: Configuration provider instance
        """
        # Detect environment if not provided
        if environment is None:
            environment = self._detect_environment()
        
        self.environment = environment
        self.env_prefix = "TEST_" if environment == 'test' else ""
        
        # Use provided provider or create default
        self.provider = provider or FileConfigurationProvider(
            Path(__file__).parent
        )
        
        # Load configurations
        self.pipeline_config = self.provider.get_pipeline_config()
        self.tables_config = self.provider.get_tables_config()
        self._connection_cache = {}
    
    @staticmethod
    def _detect_environment() -> str:
        """Detect environment from various sources."""
        environment = (
            os.getenv('ETL_ENVIRONMENT') or
            os.getenv('ENVIRONMENT') or
            os.getenv('APP_ENV') or
            'production'
        )
        
        valid_environments = ['production', 'test']
        if environment not in valid_environments:
            logger.warning(f"Invalid environment '{environment}', using 'production'")
            environment = 'production'
        
        return environment
    
    def validate_configs(self) -> bool:
        """Validate configuration with environment-specific rules."""
        missing_vars = []
        
        for db_type, env_mapping in self.ENV_MAPPINGS.items():
            for key, env_var in env_mapping.items():
                if key == 'schema':  # Schema is optional
                    continue
                
                # For test environment, require TEST_ prefixed variables
                if self.environment == 'test':
                    prefixed_env_var = f"TEST_{env_var}"
                    value = os.getenv(prefixed_env_var)
                    if not value:
                        missing_vars.append(f"{db_type}: {prefixed_env_var} (required for test environment)")
                else:
                    # For production, use base variables
                    value = os.getenv(env_var)
                    if not value:
                        missing_vars.append(f"{db_type}: {env_var}")
        
        if missing_vars:
            logger.error(f"Missing required variables for {self.environment} environment:")
            for var in missing_vars:
                logger.error(f"  - {var}")
            return False
        
        return True
```

#### 1.4 Create Configuration Factory

**New File: `etl_pipeline/config/factory.py`**
```python
from typing import Dict, Optional
from pathlib import Path
from .settings import Settings
from .providers import FileConfigurationProvider, TestConfigurationProvider

class ConfigurationFactory:
    """Factory for creating configuration instances."""
    
    @staticmethod
    def create_production_settings() -> Settings:
        """Create production settings instance."""
        return Settings(environment='production')
    
    @staticmethod
    def create_test_settings(
        config_dir: Optional[Path] = None,
        pipeline_config: Optional[Dict] = None,
        tables_config: Optional[Dict] = None,
        env_vars: Optional[Dict] = None
    ) -> Settings:
        """Create test settings instance with optional configuration injection."""
        provider = TestConfigurationProvider(
            pipeline_config=pipeline_config or {},
            tables_config=tables_config or {'tables': {}},
            env_vars=env_vars or {}
        )
        return Settings(environment='test', provider=provider)
    
    @staticmethod
    def create_settings_from_env() -> Settings:
        """Create settings instance based on environment variables."""
        return Settings()
    
    @staticmethod
    def create_settings_with_custom_provider(provider) -> Settings:
        """Create settings instance with custom provider."""
        return Settings(provider=provider)
```

### Phase 2: Test Infrastructure Updates

#### 2.1 Create Test Settings Base Class

**New File: `etl_pipeline/tests/conftest.py`**
```python
import pytest
import tempfile
import yaml
from pathlib import Path
from etl_pipeline.config.factory import ConfigurationFactory
from etl_pipeline.config.settings import reset_settings

@pytest.fixture
def test_settings_factory():
    """Factory for creating test settings instances."""
    def _create_test_settings(pipeline_config=None, tables_config=None, env_vars=None):
        return ConfigurationFactory.create_test_settings(
            pipeline_config=pipeline_config,
            tables_config=tables_config,
            env_vars=env_vars
        )
    return _create_test_settings

@pytest.fixture(autouse=True)
def reset_global_settings():
    """Reset global settings before each test."""
    reset_settings()
    yield
    reset_settings()

@pytest.fixture
def temp_config_files():
    """Create temporary configuration files for testing."""
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        
        # Create temporary pipeline config
        pipeline_config = {
            'general': {
                'pipeline_name': 'test_pipeline',
                'environment': 'test',
                'batch_size': 1000
            }
        }
        pipeline_file = temp_path / 'pipeline.yml'
        with open(pipeline_file, 'w') as f:
            yaml.dump(pipeline_config, f)
        
        # Create temporary tables config
        tables_config = {
            'tables': {
                'test_table': {
                    'primary_key': 'id',
                    'incremental': True
                }
            }
        }
        tables_file = temp_path / 'tables.yml'
        with open(tables_file, 'w') as f:
            yaml.dump(tables_config, f)
        
        yield temp_path, pipeline_file, tables_file
```

#### 2.2 Update Integration Tests

**Modified File: `etl_pipeline/tests/integration/config/test_config_integration.py`**

```python
import pytest
from etl_pipeline.config.factory import ConfigurationFactory

class TestRealConfigurationLoading:
    """Test actual configuration file loading and processing."""

    @pytest.mark.integration
    def test_real_pipeline_config_loading(self, test_environment_variables, real_pipeline_config):
        """Test loading real pipeline configuration from file."""
        with pytest.MonkeyPatch().context() as m:
            # Set environment variables
            for key, value in test_environment_variables.items():
                m.setenv(key, value)
            
            # Create test settings with injected configuration
            settings = ConfigurationFactory.create_test_settings(
                pipeline_config=real_pipeline_config
            )
            
            # Verify config was loaded
            assert settings.pipeline_config == real_pipeline_config
            assert settings.get_pipeline_setting('general.pipeline_name') == 'dental_clinic_etl_test'
            assert settings.get_pipeline_setting('general.batch_size') == 1000
            assert settings.get_pipeline_setting('general.parallel_jobs') == 2
            
            print("✅ Real pipeline config loaded successfully")

    @pytest.mark.integration
    def test_real_tables_config_loading(self, test_environment_variables, real_tables_config):
        """Test loading real tables configuration from file."""
        with pytest.MonkeyPatch().context() as m:
            # Set environment variables
            for key, value in test_environment_variables.items():
                m.setenv(key, value)
            
            # Create test settings with injected configuration
            settings = ConfigurationFactory.create_test_settings(
                tables_config=real_tables_config
            )
            
            # Verify config was loaded
            assert settings.tables_config == real_tables_config
            assert settings.get_table_config('patient')['primary_key'] == 'PatNum'
            assert settings.get_table_config('patient')['incremental'] is True
            
            print("✅ Real tables config loaded successfully")

class TestRealEnvironmentIntegration:
    """Test real environment variable integration."""

    @pytest.mark.integration
    def test_environment_detection(self, test_environment_variables):
        """Test automatic environment detection from environment variables."""
        with pytest.MonkeyPatch().context() as m:
            # Set environment variables
            for key, value in test_environment_variables.items():
                m.setenv(key, value)
            
            # Create settings after environment variables are set
            settings = ConfigurationFactory.create_settings_from_env()
            
            # Test environment detection
            assert settings.environment == 'test'
            assert settings.env_prefix == 'TEST_'
            
            print("✅ Environment detection working correctly")
```

### Phase 3: Backward Compatibility

#### 3.1 Update Import Interface

**Modified File: `etl_pipeline/config/__init__.py`**
```python
from .settings import get_settings, reset_settings
from .factory import ConfigurationFactory

# Backward compatibility - provide the same interface
def get_global_settings():
    """Backward compatibility function."""
    return get_settings()

# Create a default settings instance for backward compatibility
# This will be lazy-initialized when first accessed
_settings = None

def get_default_settings():
    """Get default settings instance (lazy initialization)."""
    global _settings
    if _settings is None:
        _settings = get_settings()
    return _settings

# Export the same interface as before
settings = get_default_settings()
```

## Files Affected

### New Files
1. `etl_pipeline/config/providers.py` - Configuration provider interfaces
2. `etl_pipeline/config/factory.py` - Configuration factory
3. `etl_pipeline/tests/conftest.py` - Test infrastructure

### Modified Files
1. `etl_pipeline/config/settings.py` - Core settings class updates
2. `etl_pipeline/config/__init__.py` - Import interface updates
3. `etl_pipeline/tests/integration/config/test_config_integration.py` - Test updates

### Integration Tests Requiring Refactoring

#### **High Priority - Directly Affected**
1. **`etl_pipeline/tests/integration/config/test_config_integration.py`** - ❌ **Currently Failing**
   - Global settings instance issues
   - Environment detection timing problems
   - Configuration file loading issues
   - Validation logic problems

2. **`etl_pipeline/tests/integration/config/test_logging_integration.py`** - ⚠️ **Likely Affected**
   - May use global settings instance
   - Environment detection problems

#### **Medium Priority - Indirectly Affected**
3. **`etl_pipeline/tests/integration/core/test_mysql_replicator_real_integration.py`** - ⚠️ **Potentially Affected**
   - Uses `ConnectionFactory.get_opendental_source_test_connection()`
   - Relies on test environment variables being properly set

4. **`etl_pipeline/tests/integration/core/test_schema_discovery_real_integration.py`** - ⚠️ **Potentially Affected**
   - Uses `ConnectionFactory.get_mysql_replication_test_connection()`
   - Depends on proper test environment setup

5. **`etl_pipeline/tests/integration/core/test_postgres_schema_real_integration.py`** - ⚠️ **Potentially Affected**
   - Uses both MySQL and PostgreSQL test connections
   - Environment variable dependency

6. **`etl_pipeline/tests/integration/orchestration/test_table_processor_real_integration.py`** - ⚠️ **Potentially Affected**
   - Uses `ConnectionFactory.get_opendental_source_test_connection()`
   - Sets `os.environ['ETL_ENVIRONMENT'] = 'test'` but this may be too late

7. **`etl_pipeline/tests/integration/orchestration/test_pipeline_orchestrator_real_integration.py`** - ⚠️ **Potentially Affected**
   - Uses `ConnectionFactory.get_postgres_analytics_test_connection()`
   - Environment variable dependency

#### **Low Priority - Likely Safe**
8. **`etl_pipeline/tests/integration/loaders/test_postgres_loader_integration.py`** - ✅ **Likely Safe**
   - Uses SQLite for testing (not real database connections)
   - Minimal dependency on configuration system

9. **`etl_pipeline/tests/integration/monitoring/test_unified_metrics_integration.py`** - ✅ **Likely Safe**
   - May not depend on database connections
   - Likely uses mocked or local data

### Files That May Need Updates
1. Any files importing `from etl_pipeline.config import settings` - May need to update to use factory pattern
2. Any files using `Settings()` directly - May need to update constructor calls
3. **`etl_pipeline/core/connections.py`** - May need updates to use new configuration patterns
4. **`etl_pipeline/orchestration/table_processor.py`** - May need environment detection updates
5. **`etl_pipeline/orchestration/pipeline_orchestrator.py`** - May need configuration loading updates

## Implementation Strategy

### Phase 1: Critical Fixes (Immediate - 1-2 days)

#### Step 1.1: Fix Global Instance Pattern
1. **Remove global instance creation** from `settings.py`
2. **Add lazy initialization** with `get_settings()` and `reset_settings()`
3. **Fix environment detection timing** to happen at instance creation
4. **Test**: Verify configuration integration tests pass

#### Step 1.2: Fix Environment Detection
1. **Update Settings constructor** to detect environment at creation time
2. **Improve environment variable validation** to be environment-specific
3. **Add test configuration injection** capability
4. **Test**: Verify environment detection works correctly

#### Step 1.3: Update Critical Tests
1. **Fix configuration integration tests** to use new patterns
2. **Add test isolation fixtures** in `conftest.py`
3. **Update test configuration injection** methods
4. **Test**: All configuration integration tests should pass

### Phase 2: Database Connection Updates (Short Term - 1 week)

#### Step 2.1: Update ConnectionFactory
1. **Make ConnectionFactory environment-aware** - use settings instead of direct `os.getenv()`
2. **Add configuration injection** for test connections             
3. **Update connection validation** to use new settings patterns
4. **Test**: Verify database connection tests work

#### Step 2.2: Update Core Integration Tests
1. **Fix MySQL replicator tests** - ensure proper environment detection
2. **Fix schema discovery tests** - update environment variable handling
3. **Fix PostgreSQL schema tests** - update connection parameter loading
4. **Test**: All core integration tests should pass

### Phase 3: Orchestration Updates (Medium Term - 1 week)

#### Step 3.1: Update Orchestration Components
1. **Update TableProcessor** - use new configuration patterns
2. **Update PipelineOrchestrator** - fix environment detection
3. **Add configuration injection** for orchestration components
4. **Test**: Verify orchestration integration tests work

#### Step 3.2: Update Orchestration Tests
1. **Fix table processor tests** - update to use new configuration patterns
2. **Fix pipeline orchestrator tests** - ensure proper test isolation
3. **Add test data management** improvements
4. **Test**: All orchestration integration tests should pass

### Phase 4: Architectural Improvements (Long Term - 2 weeks)

#### Step 4.1: Add Provider Pattern
1. **Create `providers.py`** with abstract interfaces
2. **Create `factory.py`** with factory methods
3. **Update Settings class** to use provider pattern
4. **Test**: Verify new patterns work correctly

#### Step 4.2: Update Import Interface
1. **Update `__init__.py`** to maintain backward compatibility
2. **Add deprecation warnings** for old patterns
3. **Document new usage patterns**
4. **Test**: Ensure backward compatibility works

#### Step 4.3: Update Dependent Code
1. **Identify all files** using old patterns
2. **Update to use new factory methods** where appropriate
3. **Add tests for new patterns**
4. **Test**: All existing functionality continues to work

### Phase 5: Loader Updates (As Needed)

#### Step 5.1: Assess Loader Tests
1. **Evaluate PostgreSQL loader tests** - determine if updates needed
2. **Update if necessary** - only if they start using real connections
3. **Test**: Ensure loader tests continue to work

## Root Cause Analysis

### Why Integration Tests Are Affected

1. **ConnectionFactory Dependency**: Most integration tests use `ConnectionFactory` methods that read environment variables directly using `os.getenv()`. If the global settings instance is created before test environment variables are set, the connection factory may use production environment variables instead of test ones.

2. **Environment Detection Timing**: Tests that set `ETL_ENVIRONMENT='test'` after module import will not affect the global settings instance that was already created.

3. **Configuration File Loading**: Tests that create temporary configuration files cannot override the hardcoded paths in the Settings class.

### Specific Issues by Test Type

#### **Database Connection Tests**
```python
# These will fail if environment variables aren't set correctly
source_engine = ConnectionFactory.get_opendental_source_test_connection()
repl_engine = ConnectionFactory.get_mysql_replication_test_connection()
analytics_engine = ConnectionFactory.get_postgres_analytics_test_connection()
```

#### **Configuration Loading Tests**
```python
# These will fail because they load production config files
settings = Settings()  # Uses global instance with production config
```

#### **Environment Detection Tests**
```python
# These will fail because environment is detected at import time
assert settings.environment == 'test'  # Will be 'production'
```

## Migration Guide

### For Existing Code
```python
# Old way (still works but deprecated)
from etl_pipeline.config import settings
config = settings.get_database_config('source')

# New way (recommended)
from etl_pipeline.config.factory import ConfigurationFactory
settings = ConfigurationFactory.create_settings_from_env()
config = settings.get_database_config('source')
```

### For Tests
```python
# Old way (problematic)
from etl_pipeline.config import settings
# settings is already created with production config

# New way (proper test isolation)
from etl_pipeline.config.factory import ConfigurationFactory
settings = ConfigurationFactory.create_test_settings(
    pipeline_config=test_pipeline_config,
    tables_config=test_tables_config,
    env_vars=test_env_vars
)
```

### For Integration Tests

#### **Configuration Tests**
```python
# Old way (failing)
def test_environment_detection(self, test_environment_variables):
    with pytest.MonkeyPatch().context() as m:
        for key, value in test_environment_variables.items():
            m.setenv(key, value)
        
        settings = Settings()  # Uses global instance
        assert settings.environment == 'test'  # Fails

# New way (working)
def test_environment_detection(self, test_environment_variables):
    with pytest.MonkeyPatch().context() as m:
        for key, value in test_environment_variables.items():
            m.setenv(key, value)
        
        settings = ConfigurationFactory.create_settings_from_env()
        assert settings.environment == 'test'  # Works
```

#### **Database Connection Tests**
```python
# Old way (may fail due to environment timing)
def test_database_connection(self):
    source_engine = ConnectionFactory.get_opendental_source_test_connection()
    # May use production environment variables

# New way (environment-aware)
def test_database_connection(self, test_settings):
    # test_settings fixture ensures proper environment
    source_engine = ConnectionFactory.get_opendental_source_test_connection()
    # Uses test environment variables
```

#### **Orchestration Tests**
```python
# Old way (environment timing issues)
def test_table_processor(self):
    os.environ['ETL_ENVIRONMENT'] = 'test'  # Too late
    processor = TableProcessor(environment='test')
    # May still use production config

# New way (proper isolation)
def test_table_processor(self, test_settings):
    processor = TableProcessor(environment='test')
    # Uses test configuration properly
```

## Benefits

1. **Testability**: Proper isolation between test and production configurations
2. **Flexibility**: Support for different configuration sources
3. **Maintainability**: Clear separation of concerns
4. **Reliability**: Predictable behavior across different environments
5. **Backward Compatibility**: Existing code continues to work
6. **Future Extensibility**: Easy to add new configuration sources

## Risks and Mitigation

### Risk: Breaking Changes
- **Mitigation**: Maintain backward compatibility through `__init__.py`
- **Mitigation**: Add deprecation warnings for old patterns
- **Mitigation**: Gradual migration strategy

### Risk: Performance Impact
- **Mitigation**: Lazy initialization for global instance
- **Mitigation**: Connection caching maintained
- **Mitigation**: Minimal overhead for new patterns

### Risk: Complexity Increase
- **Mitigation**: Clear documentation and examples
- **Mitigation**: Factory pattern simplifies usage
- **Mitigation**: Backward compatibility reduces migration effort



## Success Criteria

1. **All integration tests pass**
   - Configuration integration tests: ✅ Passing
   - Core integration tests: ✅ Passing
   - Orchestration integration tests: ✅ Passing
   - Loader integration tests: ✅ Passing (no regression)

2. **Existing code continues to work without changes**
   - Backward compatibility maintained
   - No breaking changes for existing imports
   - Gradual migration path available

3. **New code can use improved patterns**
   - Factory pattern available for new code
   - Provider pattern for configuration injection
   - Better test isolation capabilities

4. **Test isolation is properly maintained**
   - Tests don't interfere with each other
   - Environment variables properly isolated
   - Configuration files properly injected

5. **Configuration validation is more robust**
   - Environment-specific validation rules
   - Clear error messages for missing variables
   - Proper validation of test vs production configs

6. **Documentation is clear and comprehensive**
   - Migration guide for existing code
   - Examples for new patterns
   - Clear API documentation

## Testing Strategy

### **Phase 1 Testing**
1. **Unit Tests**: Test each component in isolation
2. **Configuration Integration Tests**: Test configuration loading and validation
3. **Backward Compatibility Tests**: Ensure existing code still works

### **Phase 2 Testing**
1. **Database Connection Tests**: Test environment-aware connections
2. **Core Integration Tests**: Test schema discovery and replication
3. **Performance Tests**: Ensure no significant performance regression

### **Phase 3 Testing**
1. **Orchestration Tests**: Test table processor and pipeline orchestrator
2. **End-to-End Tests**: Test complete pipeline flows
3. **Migration Tests**: Test migration from old to new patterns

### **Phase 4 Testing**
1. **Provider Pattern Tests**: Test new architectural components
2. **Factory Pattern Tests**: Test configuration factory methods
3. **Integration Tests**: Test all components working together

## Risk Mitigation

### **High Risk Areas**
1. **Global Instance Changes**: May break existing code
   - **Mitigation**: Maintain backward compatibility through `__init__.py`
   - **Mitigation**: Add deprecation warnings for old patterns
   - **Mitigation**: Gradual migration strategy

2. **Environment Detection Changes**: May affect all tests
   - **Mitigation**: Test thoroughly with different environment setups
   - **Mitigation**: Add comprehensive logging for debugging
   - **Mitigation**: Provide clear error messages

3. **ConnectionFactory Updates**: May break database connections
   - **Mitigation**: Test with real database connections
   - **Mitigation**: Maintain existing API compatibility
   - **Mitigation**: Add fallback mechanisms

### **Medium Risk Areas**
1. **Configuration File Loading**: May affect configuration management
   - **Mitigation**: Test with various file formats and locations
   - **Mitigation**: Add validation for configuration files
   - **Mitigation**: Provide clear error messages for missing files

2. **Test Infrastructure Changes**: May affect all tests
   - **Mitigation**: Test new fixtures thoroughly
   - **Mitigation**: Ensure proper cleanup after tests
   - **Mitigation**: Add comprehensive test coverage

### **Low Risk Areas**
1. **Provider Pattern**: New architectural component
   - **Mitigation**: Test with mock providers
   - **Mitigation**: Ensure proper interface compliance
   - **Mitigation**: Add comprehensive documentation

2. **Factory Pattern**: New configuration creation method
   - **Mitigation**: Test all factory methods
   - **Mitigation**: Ensure proper error handling
   - **Mitigation**: Add usage examples 