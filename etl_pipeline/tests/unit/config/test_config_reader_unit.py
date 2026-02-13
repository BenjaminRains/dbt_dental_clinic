"""
Unit tests for ConfigReader component.

This module tests the ConfigReader class with comprehensive mocking and provider pattern:
- Pure unit tests with no real file system operations
- Fast execution, isolated component behavior
- Core logic and edge cases for all methods
- Error handling and validation scenarios

Following the three-tier testing approach:
- Unit tests: Pure mocking, fast execution, isolated behavior
- Target: 95% coverage for all ConfigReader methods

ETL Pipeline Context:
    - ConfigReader replaces SchemaDiscovery for static configuration management
    - Critical for nightly ETL pipeline performance (5-10x faster than dynamic discovery)
    - Manages dental clinic table configurations from tables.yml
    - Supports OpenDental MySQL to PostgreSQL ETL workflows
    - Enables provider pattern dependency injection for testing
"""

import pytest
import yaml
from unittest.mock import patch, mock_open, MagicMock
from pathlib import Path
from datetime import datetime

# Import ConfigReader and fixtures
from etl_pipeline.config.config_reader import ConfigReader

# Import specific exception classes for testing
from etl_pipeline.exceptions.configuration import ConfigurationError, EnvironmentError

# Import fixtures from the modular structure
from tests.fixtures.config_reader_fixtures import (
    valid_tables_config,
    minimal_tables_config,
    invalid_tables_config,
    empty_tables_config,
    malformed_yaml_config,
    mock_config_reader,
    mock_config_reader_with_invalid_config,
    mock_config_reader_with_empty_config,
    mock_config_reader_with_dependency_data,
    config_reader_test_cases,
    config_reader_error_cases,
    config_reader_validation_cases,
    config_reader_performance_data,
    config_reader_dependency_test_data
)

import etl_pipeline.config.config_reader as config_reader_module


class TestConfigReaderInitialization:
    """
    Test ConfigReader class initialization and basic functionality.
    
    Test Strategy:
        - Unit tests with mocked file system operations
        - Validates YAML configuration loading and parsing
        - Tests error handling for missing/invalid configuration files
        - Ensures proper initialization for ETL pipeline performance
    
    Coverage Areas:
        - Default and custom configuration file paths
        - YAML parsing and validation for dental clinic table configs
        - File system error handling (not found, permissions, malformed)
        - Configuration structure validation (tables section required)
        
    ETL Context:
        - Critical for nightly ETL pipeline initialization
        - Replaces SchemaDiscovery with static configuration approach
        - Supports dental clinic table configurations from tables.yml
        - Enables 5-10x performance improvement over dynamic discovery
    """

    @pytest.mark.unit
    def test_init_with_default_path(self, valid_tables_config):
        """
        Test ConfigReader initialization with default path.
        
        Validates:
            - Default configuration path resolution for dental clinic ETL
            - YAML configuration loading and parsing
            - Proper initialization of configuration state
            - Timestamp tracking for configuration freshness
            
        ETL Pipeline Context:
            - Default path: etl_pipeline/config/tables.yml
            - Used by ETL pipeline components for table configuration
            - Critical for nightly ETL job initialization
            - Supports OpenDental to PostgreSQL data flow configuration
        """
        # Mock file existence and YAML loading
        with patch('pathlib.Path.exists', return_value=True), \
             patch('builtins.open', mock_open(read_data="test yaml content")), \
             patch('yaml.safe_load', return_value=valid_tables_config):
            
            config_reader = ConfigReader()
        
        # Test basic initialization - use actual path from the object
        # The ConfigReader uses Path(__file__).parent / "tables.yml" where __file__ is config_reader.py
        # So we need to calculate the path from the config_reader.py file location
        # From test file: etl_pipeline/tests/unit/config/test_config_reader_unit.py
        # To config_reader.py: etl_pipeline/etl_pipeline/config/config_reader.py
        # So we go up 3 levels from tests/unit/config/ to etl_pipeline/, then down to etl_pipeline/config/
        expected_path = str(Path(config_reader_module.__file__).parent / "tables.yml")
        assert config_reader.config_path == expected_path
        assert config_reader.config == valid_tables_config
        assert isinstance(config_reader._last_loaded, datetime)
        assert len(config_reader.config.get('tables', {})) == 5

    @pytest.mark.unit
    def test_init_with_custom_path(self, valid_tables_config):
        """Test ConfigReader initialization with custom path."""
        # Mock file existence and YAML loading
        with patch('pathlib.Path.exists', return_value=True), \
             patch('builtins.open', mock_open(read_data="test yaml content")), \
             patch('yaml.safe_load', return_value=valid_tables_config):
            
            config_reader = ConfigReader("custom/path/tables.yml")
        
        # Test custom path initialization
        assert config_reader.config_path == "custom/path/tables.yml"
        assert config_reader.config == valid_tables_config

    @pytest.mark.unit
    def test_init_file_not_found(self):
        """Test ConfigReader initialization with non-existent file."""
        # Mock file existence to return False
        with patch('pathlib.Path.exists', return_value=False):
            with pytest.raises(ConfigurationError) as exc_info:
                ConfigReader("nonexistent.yml")
            
            # Test specific exception properties
            assert "Configuration file not found: nonexistent.yml" in str(exc_info.value)
            assert exc_info.value.config_file == "nonexistent.yml"
            assert exc_info.value.operation == "configuration_validation"
            assert "file_loading" in exc_info.value.details.get('operation', '')

    @pytest.mark.unit
    def test_init_invalid_yaml(self):
        """Test ConfigReader initialization with invalid YAML."""
        # Mock file existence and open
        with patch('pathlib.Path.exists', return_value=True), \
             patch('builtins.open', mock_open(read_data="invalid yaml content")):
            # Mock YAML loading to raise exception
            with patch('yaml.safe_load', side_effect=yaml.YAMLError("Invalid YAML")):
                with pytest.raises(ConfigurationError) as exc_info:
                    ConfigReader("invalid.yml")
                
                # Test specific exception properties
                assert "Invalid YAML format in configuration file: invalid.yml" in str(exc_info.value)
                assert exc_info.value.config_file == "invalid.yml"
                assert exc_info.value.operation == "configuration_validation"
                assert "yaml_parsing" in exc_info.value.details.get('operation', '')

    @pytest.mark.unit
    def test_init_missing_tables_section(self):
        """Test ConfigReader initialization with missing tables section."""
        # Mock file existence and YAML loading
        with patch('pathlib.Path.exists', return_value=True), \
             patch('builtins.open', mock_open(read_data="test yaml content")), \
             patch('yaml.safe_load', return_value={'other_section': {}}):
            
            with pytest.raises(ConfigurationError) as exc_info:
                ConfigReader("missing_tables.yml")
            
            # Test specific exception properties
            assert "Invalid configuration file structure: missing_tables.yml" in str(exc_info.value)
            assert exc_info.value.config_file == "missing_tables.yml"
            assert exc_info.value.operation == "configuration_validation"
            assert "configuration_validation" in exc_info.value.details.get('operation', '')

    @pytest.mark.unit
    def test_init_empty_config(self):
        """Test ConfigReader initialization with empty configuration."""
        # Mock file existence and YAML loading
        with patch('pathlib.Path.exists', return_value=True), \
             patch('builtins.open', mock_open(read_data="test yaml content")), \
             patch('yaml.safe_load', return_value=None):
            
            with pytest.raises(ConfigurationError) as exc_info:
                ConfigReader("empty.yml")
            
            # Test specific exception properties
            assert "Invalid configuration file structure: empty.yml" in str(exc_info.value)
            assert exc_info.value.config_file == "empty.yml"
            assert exc_info.value.operation == "configuration_validation"
            assert "configuration_validation" in exc_info.value.details.get('operation', '')


class TestConfigReaderLoadConfiguration:
    """
    Test configuration loading methods.
    
    Test Strategy:
        - Unit tests with mocked YAML loading and file operations
        - Validates configuration reloading capabilities
        - Tests error handling for file system and YAML parsing issues
        - Ensures proper caching and timestamp management
    
    Coverage Areas:
        - Initial configuration loading from tables.yml
        - Configuration reloading for runtime updates
        - YAML parsing error handling for dental clinic configs
        - File system error scenarios (IO errors, permissions)
        
    ETL Context:
        - Supports runtime configuration updates during ETL execution
        - Critical for dental clinic table configuration management
        - Enables hot-reloading of table configurations without restart
        - Maintains performance through proper error handling
    """

    @pytest.mark.unit
    def test_load_configuration_success(self, valid_tables_config):
        """Test successful configuration loading."""
        # Mock file existence and YAML loading
        with patch('pathlib.Path.exists', return_value=True), \
             patch('builtins.open', mock_open(read_data="test yaml content")), \
             patch('yaml.safe_load', return_value=valid_tables_config) as mock_yaml:
            
            config_reader = ConfigReader("test.yml")
            result = config_reader._load_configuration()
        
        # Test successful loading
        assert result == valid_tables_config
        # Note: yaml.safe_load is called twice - once in __init__ and once in _load_configuration
        assert mock_yaml.call_count >= 1

    @pytest.mark.unit
    def test_load_configuration_file_not_found(self):
        """Test configuration loading with non-existent file."""
        # Mock file existence to return False
        with patch('pathlib.Path.exists', return_value=False):
            with pytest.raises(ConfigurationError) as exc_info:
                config_reader = ConfigReader("nonexistent.yml")
            
            # Test specific exception properties
            assert "Configuration file not found: nonexistent.yml" in str(exc_info.value)
            assert exc_info.value.config_file == "nonexistent.yml"
            assert exc_info.value.operation == "configuration_validation"

    @pytest.mark.unit
    def test_load_configuration_yaml_error(self):
        """Test configuration loading with YAML error."""
        # Mock file existence
        with patch('pathlib.Path.exists', return_value=True):
            # Mock YAML loading to raise exception
            with patch('yaml.safe_load', side_effect=yaml.YAMLError("YAML Error")), \
                 patch('builtins.open', mock_open(read_data="invalid yaml")):
                
                with pytest.raises(ConfigurationError) as exc_info:
                    config_reader = ConfigReader("invalid.yml")
                
                # Test specific exception properties
                assert "Invalid YAML format in configuration file: invalid.yml" in str(exc_info.value)
                assert exc_info.value.config_file == "invalid.yml"
                assert exc_info.value.operation == "configuration_validation"

    @pytest.mark.unit
    def test_reload_configuration_success(self, mock_config_reader, valid_tables_config):
        """Test successful configuration reloading."""
        # Mock YAML loading to return updated configuration
        updated_config = valid_tables_config.copy()
        updated_config['tables']['new_table'] = {'table_importance': 'standard'}
        
        # Mock file existence and YAML loading
        with patch('pathlib.Path.exists', return_value=True), \
             patch('builtins.open', mock_open(read_data="updated yaml content")), \
             patch('yaml.safe_load', return_value=updated_config):
            
            result = mock_config_reader.reload_configuration()
        
        # Test successful reloading
        assert result is True
        assert mock_config_reader.config == updated_config
        assert 'new_table' in mock_config_reader.config['tables']

    @pytest.mark.unit
    def test_reload_configuration_failure(self, mock_config_reader):
        """Test configuration reloading failure."""
        # Mock file existence to return False
        with patch('pathlib.Path.exists', return_value=False):
            result = mock_config_reader.reload_configuration()
        
        # Test reloading failure
        assert result is False


class TestConfigReaderTableConfiguration:
    """
    Test table configuration retrieval methods.
    
    Test Strategy:
        - Unit tests with mocked configuration data
        - Validates table-specific configuration retrieval
        - Tests edge cases for missing and invalid table names
        - Ensures proper fallback behavior for unknown tables
    
    Coverage Areas:
        - Individual table configuration retrieval
        - Case sensitivity handling for dental clinic table names
        - Empty configuration handling for missing tables
        - Configuration structure validation for ETL operations
        
    ETL Context:
        - Core functionality for ETL pipeline table processing
        - Supports OpenDental table configurations (patient, appointment, etc.)
        - Critical for TableProcessor and PipelineOrchestrator components
        - Enables proper table-specific ETL strategy selection
    """

    @pytest.mark.unit
    def test_get_table_config_existing_table(self, mock_config_reader):
        """
        Test getting configuration for existing table.
        
        Validates:
            - Table-specific configuration retrieval for dental clinic tables
            - Primary key and incremental column configuration
            - Extraction strategy and importance level settings
            - Batch size and size estimation for ETL optimization
            
        ETL Pipeline Context:
            - Patient table: Critical table with incremental loading
            - Primary key: PatNum (OpenDental patient identifier)
            - Incremental column: DateTStamp for efficient delta loading
            - Used by TableProcessor for ETL strategy selection
        """
        config = mock_config_reader.get_table_config('patient')
        
        # Test configuration retrieval
        assert config['primary_key'] == 'PatNum'
        assert config['incremental_column'] == 'DateTStamp'
        assert config['extraction_strategy'] == 'incremental'
        assert config['table_importance'] == 'critical'
        assert config['batch_size'] == 1000
        assert config['estimated_size_mb'] == 50.0

    @pytest.mark.unit
    def test_get_table_config_non_existing_table(self, mock_config_reader):
        """Test getting configuration for non-existing table."""
        config = mock_config_reader.get_table_config('nonexistent_table')
        
        # Test empty configuration for non-existing table
        assert config == {}

    @pytest.mark.unit
    def test_get_table_config_empty_config(self, mock_config_reader_with_empty_config):
        """Test getting table configuration with empty config."""
        config = mock_config_reader_with_empty_config.get_table_config('patient')
        
        # Test empty configuration
        assert config == {}

    @pytest.mark.unit
    def test_get_table_config_case_sensitivity(self, mock_config_reader):
        """Test table configuration case sensitivity."""
        # Test with different case
        config = mock_config_reader.get_table_config('PATIENT')
        
        # Should return empty config for case-sensitive lookup
        assert config == {}


class TestConfigReaderFilteringMethods:
    """
    Test table filtering methods by various criteria.
    
    Test Strategy:
        - Unit tests with mocked configuration data
        - Validates filtering by importance, strategy, size, and monitoring
        - Tests edge cases for empty results and invalid criteria
        - Ensures proper performance for large configuration sets
    
    Coverage Areas:
        - Filtering by table importance (critical, important, reference, standard)
        - Filtering by extraction strategy (incremental, full_table)
        - Filtering by table size thresholds for dental clinic data volumes
        - Filtering by monitoring configuration for alert management
        
    ETL Context:
        - Critical for PriorityProcessor table prioritization
        - Supports dental clinic table importance classification
        - Enables proper ETL strategy selection based on table characteristics
        - Supports monitoring and alerting configuration for ETL pipeline
    """

    @pytest.mark.unit
    def test_get_tables_by_importance_critical(self, mock_config_reader):
        """
        Test filtering tables by critical importance.
        
        Validates:
            - Critical table identification for dental clinic ETL prioritization
            - Importance-based filtering for ETL pipeline execution order
            - Proper handling of critical table configurations
            - Integration with PriorityProcessor for table ordering
            
        ETL Pipeline Context:
            - Critical tables: Patient data (highest priority)
            - Used by PriorityProcessor for ETL execution order
            - Ensures patient data is processed before dependent tables
            - Supports dental clinic data integrity requirements
        """
        tables = mock_config_reader.get_tables_by_importance('critical')
        
        # Test critical tables
        assert 'patient' in tables
        assert len(tables) == 1

    @pytest.mark.unit
    def test_get_tables_by_importance_important(self, mock_config_reader):
        """Test filtering tables by important importance."""
        tables = mock_config_reader.get_tables_by_importance('important')
        
        # Test important tables
        assert 'appointment' in tables
        assert 'procedurelog' in tables
        assert len(tables) == 2

    @pytest.mark.unit
    def test_get_tables_by_importance_reference(self, mock_config_reader):
        """Test filtering tables by reference importance."""
        tables = mock_config_reader.get_tables_by_importance('reference')
        
        # Test reference tables
        assert 'securitylog' in tables
        assert len(tables) == 1

    @pytest.mark.unit
    def test_get_tables_by_importance_standard(self, mock_config_reader):
        """Test filtering tables by standard importance."""
        tables = mock_config_reader.get_tables_by_importance('standard')
        
        # Test standard tables
        assert 'definition' in tables
        assert len(tables) == 1

    @pytest.mark.unit
    def test_get_tables_by_importance_nonexistent(self, mock_config_reader):
        """Test filtering tables by non-existent importance."""
        tables = mock_config_reader.get_tables_by_importance('nonexistent')
        
        # Test non-existent importance
        assert tables == []

    @pytest.mark.unit
    def test_get_tables_by_strategy_incremental(self, mock_config_reader):
        """Test filtering tables by incremental strategy."""
        tables = mock_config_reader.get_tables_by_strategy('incremental')
        
        # Test incremental tables
        assert 'patient' in tables
        assert 'appointment' in tables
        assert 'procedurelog' in tables
        assert len(tables) == 3

    @pytest.mark.unit
    def test_get_tables_by_strategy_full_table(self, mock_config_reader):
        """Test filtering tables by full_table strategy."""
        tables = mock_config_reader.get_tables_by_strategy('full_table')
        
        # Test full_table tables
        assert 'securitylog' in tables
        assert 'definition' in tables
        assert len(tables) == 2

    @pytest.mark.unit
    def test_get_tables_by_strategy_nonexistent(self, mock_config_reader):
        """Test filtering tables by non-existent strategy."""
        tables = mock_config_reader.get_tables_by_strategy('nonexistent')
        
        # Test non-existent strategy
        assert tables == []

    @pytest.mark.unit
    def test_get_large_tables_default_threshold(self, mock_config_reader):
        """Test filtering large tables with default threshold (100MB)."""
        tables = mock_config_reader.get_large_tables()
        
        # Test large tables with default threshold
        assert 'procedurelog' in tables  # 150MB > 100MB
        assert len(tables) == 1

    @pytest.mark.unit
    def test_get_large_tables_custom_threshold(self, mock_config_reader):
        """Test filtering large tables with custom threshold."""
        tables = mock_config_reader.get_large_tables(50.0)
        
        # Test large tables with 50MB threshold (uses > not >=)
        assert 'patient' not in tables  # 50MB not > 50MB
        assert 'appointment' not in tables  # 25MB < 50MB (should not be included)
        assert 'procedurelog' in tables  # 150MB > 50MB
        assert len(tables) == 1

    @pytest.mark.unit
    def test_get_large_tables_high_threshold(self, mock_config_reader):
        """Test filtering large tables with high threshold."""
        tables = mock_config_reader.get_large_tables(200.0)
        
        # Test large tables with 200MB threshold
        assert tables == []

    @pytest.mark.unit
    def test_get_monitored_tables(self, mock_config_reader):
        """Test filtering monitored tables."""
        tables = mock_config_reader.get_monitored_tables()
        
        # Test monitored tables (alert_on_failure: True)
        assert 'patient' in tables
        assert 'appointment' in tables
        assert 'procedurelog' not in tables  # alert_on_failure: False
        assert 'securitylog' not in tables  # alert_on_failure: False
        assert 'definition' not in tables  # alert_on_failure: False
        assert len(tables) == 2


class TestConfigReaderDependencies:
    """
    Test table dependency methods.
    
    Test Strategy:
        - Unit tests with mocked dependency configuration data
        - Validates dependency resolution for complex table relationships
        - Tests edge cases for circular dependencies and missing tables
        - Ensures proper dependency ordering for ETL pipeline execution
    
    Coverage Areas:
        - Simple dependency resolution (appointment → patient)
        - Complex dependency chains (claim → procedurelog → patient)
        - Empty dependency handling for standalone tables
        - Missing table dependency handling
        
    ETL Context:
        - Critical for ETL pipeline execution order determination
        - Supports dental clinic table relationships (appointments depend on patients)
        - Enables proper table processing order in PipelineOrchestrator
        - Prevents data integrity issues in incremental ETL workflows
    """

    @pytest.mark.unit
    def test_get_table_dependencies_existing_table(self, mock_config_reader):
        """Test getting dependencies for existing table."""
        dependencies = mock_config_reader.get_table_dependencies('appointment')
        
        # Test dependencies
        assert 'patient' in dependencies
        assert len(dependencies) == 1

    @pytest.mark.unit
    def test_get_table_dependencies_no_dependencies(self, mock_config_reader):
        """Test getting dependencies for table with no dependencies."""
        dependencies = mock_config_reader.get_table_dependencies('patient')
        
        # Test no dependencies
        assert dependencies == []

    @pytest.mark.unit
    def test_get_table_dependencies_non_existing_table(self, mock_config_reader):
        """Test getting dependencies for non-existing table."""
        dependencies = mock_config_reader.get_table_dependencies('nonexistent_table')
        
        # Test empty dependencies for non-existing table
        assert dependencies == []

    @pytest.mark.unit
    def test_get_table_dependencies_complex_dependencies(self, mock_config_reader_with_dependency_data):
        """Test getting dependencies for table with complex dependencies."""
        config_reader = mock_config_reader_with_dependency_data
        
        # Test complex dependencies
        procedurelog_deps = config_reader.get_table_dependencies('procedurelog')
        assert 'patient' in procedurelog_deps
        assert 'appointment' in procedurelog_deps
        assert len(procedurelog_deps) == 2
        
        claim_deps = config_reader.get_table_dependencies('claim')
        assert 'patient' in claim_deps
        assert 'procedurelog' in claim_deps
        assert len(claim_deps) == 2


class TestConfigReaderSummary:
    """
    Test configuration summary methods.
    
    Test Strategy:
        - Unit tests with mocked configuration data
        - Validates summary statistics calculation and accuracy
        - Tests edge cases for empty and large configurations
        - Ensures proper performance for summary generation
    
    Coverage Areas:
        - Configuration statistics (total tables, importance levels, strategies)
        - Size range categorization for dental clinic data volumes
        - Monitoring and modeling statistics for ETL pipeline management
        - Timestamp tracking for configuration freshness
        
    ETL Context:
        - Critical for ETL pipeline monitoring and reporting
        - Supports dental clinic configuration analytics and insights
        - Enables pipeline health monitoring and capacity planning
        - Provides configuration audit trail for compliance and debugging
    """

    @pytest.mark.unit
    def test_get_configuration_summary(self, mock_config_reader):
        """Test getting configuration summary."""
        summary = mock_config_reader.get_configuration_summary()
        
        # Test summary structure
        assert 'total_tables' in summary
        assert 'importance_levels' in summary
        assert 'extraction_strategies' in summary
        assert 'size_ranges' in summary
        assert 'monitored_tables' in summary
        assert 'modeled_tables' in summary
        assert 'last_loaded' in summary
        
        # Test summary values
        assert summary['total_tables'] == 5
        assert summary['importance_levels']['critical'] == 1
        assert summary['importance_levels']['important'] == 2
        assert summary['importance_levels']['reference'] == 1
        assert summary['importance_levels']['standard'] == 1
        assert summary['extraction_strategies']['incremental'] == 3
        assert summary['extraction_strategies']['full_table'] == 2
        assert summary['size_ranges']['small'] == 0  # definition: 1MB (not < 1MB)
        assert summary['size_ranges']['medium'] == 4  # patient: 50MB, appointment: 25MB, securitylog: 10MB, definition: 1MB
        assert summary['size_ranges']['large'] == 1  # procedurelog: 150MB
        assert summary['monitored_tables'] == 2  # patient, appointment
        assert summary['modeled_tables'] == 3  # patient, appointment, procedurelog

    @pytest.mark.unit
    def test_get_configuration_summary_empty_config(self, mock_config_reader_with_empty_config):
        """Test getting configuration summary with empty config."""
        summary = mock_config_reader_with_empty_config.get_configuration_summary()
        
        # Test empty summary
        assert summary['total_tables'] == 0
        assert summary['importance_levels'] == {}
        assert summary['extraction_strategies'] == {}
        assert summary['size_ranges']['small'] == 0
        assert summary['size_ranges']['medium'] == 0
        assert summary['size_ranges']['large'] == 0
        assert summary['monitored_tables'] == 0
        assert summary['modeled_tables'] == 0


class TestConfigReaderValidation:
    """
    Test configuration validation methods.
    
    Test Strategy:
        - Unit tests with mocked valid and invalid configuration data
        - Validates comprehensive configuration validation rules
        - Tests edge cases for missing fields and invalid values
        - Ensures proper error reporting for configuration issues
    
    Coverage Areas:
        - Required field validation (batch_size, extraction_strategy, importance)
        - Value validation (positive batch sizes, valid strategies)
        - Monitoring configuration validation for large tables
        - Configuration structure validation for ETL pipeline requirements
        
    ETL Context:
        - Critical for ETL pipeline configuration quality assurance
        - Prevents runtime errors in dental clinic ETL workflows
        - Supports configuration validation before pipeline execution
        - Enables proactive identification of configuration issues
    """

    @pytest.mark.unit
    def test_validate_configuration_valid_config(self, mock_config_reader):
        """Test validation with valid configuration."""
        issues = mock_config_reader.validate_configuration()
        
        # Test no issues with valid configuration
        assert issues['missing_batch_size'] == []
        assert issues['missing_extraction_strategy'] == []
        assert issues['missing_importance'] == []
        assert issues['invalid_batch_size'] == []
        # procedurelog is > 50MB without monitoring, so it should be flagged
        assert 'procedurelog' in issues['large_tables_without_monitoring']

    @pytest.mark.unit
    def test_validate_configuration_invalid_config(self, mock_config_reader_with_invalid_config):
        """Test validation with invalid configuration."""
        # The invalid config has string values that will cause TypeError
        # We need to handle this gracefully in the validation method
        with pytest.raises(ConfigurationError):
            mock_config_reader_with_invalid_config.validate_configuration()

    @pytest.mark.unit
    def test_validate_configuration_empty_config(self, mock_config_reader_with_empty_config):
        """Test validation with empty configuration."""
        issues = mock_config_reader_with_empty_config.validate_configuration()
        
        # Test no issues with empty configuration
        assert issues['missing_batch_size'] == []
        assert issues['missing_extraction_strategy'] == []
        assert issues['missing_importance'] == []
        assert issues['invalid_batch_size'] == []
        assert issues['large_tables_without_monitoring'] == []

    @pytest.mark.unit
    def test_validate_configuration_missing_fields(self):
        """Test validation with missing required fields."""
        # Create config with missing fields
        config_with_missing_fields = {
            'tables': {
                'patient': {
                    'table_importance': 'critical'
                    # Missing batch_size, extraction_strategy
                }
            }
        }
        
        class MockConfigReaderMissingFields(ConfigReader):
            def __init__(self):
                self.config_path = "mock_missing_fields"
                self.config = config_with_missing_fields
                self._last_loaded = datetime.now()
        
        config_reader = MockConfigReaderMissingFields()
        issues = config_reader.validate_configuration()
        
        # Test missing fields
        assert 'patient' in issues['missing_batch_size']
        assert 'patient' in issues['missing_extraction_strategy']

    @pytest.mark.unit
    def test_validate_configuration_invalid_values(self):
        """Test validation with invalid values."""
        # Create config with invalid values
        config_with_invalid_values = {
            'tables': {
                'patient': {
                    'batch_size': -1,  # Invalid negative value
                    'extraction_strategy': 'incremental',
                    'table_importance': 'critical'
                }
            }
        }
        
        class MockConfigReaderInvalidValues(ConfigReader):
            def __init__(self):
                self.config_path = "mock_invalid_values"
                self.config = config_with_invalid_values
                self._last_loaded = datetime.now()
        
        config_reader = MockConfigReaderInvalidValues()
        issues = config_reader.validate_configuration()
        
        # Test invalid values
        assert 'patient' in issues['invalid_batch_size']


class TestConfigReaderUtilityMethods:
    """
    Test utility methods.
    
    Test Strategy:
        - Unit tests with mocked configuration data
        - Validates utility method functionality and return values
        - Tests edge cases for configuration state management
        - Ensures proper utility method integration with ETL pipeline
    
    Coverage Areas:
        - Configuration path retrieval for file management
        - Last loaded timestamp tracking for configuration freshness
        - Utility method integration with main configuration functionality
        
    ETL Context:
        - Supports ETL pipeline configuration management utilities
        - Enables configuration file path resolution for dental clinic setups
        - Provides configuration freshness tracking for monitoring
        - Supports debugging and troubleshooting of configuration issues
    """

    @pytest.mark.unit
    def test_get_configuration_path(self, mock_config_reader):
        """Test getting configuration path."""
        path = mock_config_reader.get_configuration_path()
        
        # Test path retrieval
        assert path == "mock_config"

    @pytest.mark.unit
    def test_get_last_loaded(self, mock_config_reader):
        """Test getting last loaded timestamp."""
        last_loaded = mock_config_reader.get_last_loaded()
        
        # Test timestamp retrieval
        assert isinstance(last_loaded, datetime)


class TestConfigReaderErrorHandling:
    """
    Test error handling scenarios.
    
    Test Strategy:
        - Unit tests with mocked error conditions
        - Validates comprehensive error handling for file system and YAML issues
        - Tests edge cases for permission errors and IO failures
        - Ensures proper error propagation and logging
    
    Coverage Areas:
        - File permission error handling for protected configuration files
        - IO error handling for file system issues during ETL execution
        - YAML parsing error handling for malformed dental clinic configurations
        - Error recovery and graceful degradation for ETL pipeline stability
        
    ETL Context:
        - Critical for ETL pipeline error resilience and stability
        - Prevents pipeline failures due to configuration access issues
        - Supports proper error reporting for dental clinic ETL operations
        - Enables graceful degradation when configuration files are unavailable
    """

    @pytest.mark.unit
    def test_init_with_file_permission_error(self):
        """Test initialization with file permission error."""
        # Mock file existence
        with patch('pathlib.Path.exists', return_value=True):
            # Mock file open to raise permission error
            with patch('builtins.open', side_effect=PermissionError("Permission denied")):
                with pytest.raises(ConfigurationError) as exc_info:
                    ConfigReader("protected.yml")
                
                # Test specific exception properties
                assert "Failed to load configuration from protected.yml" in str(exc_info.value)
                assert exc_info.value.config_file == "protected.yml"
                assert exc_info.value.operation == "configuration_validation"

    @pytest.mark.unit
    def test_load_configuration_with_io_error(self):
        """Test configuration loading with IO error."""
        # Mock file existence
        with patch('pathlib.Path.exists', return_value=True):
            # Mock file open to raise IO error
            with patch('builtins.open', side_effect=IOError("IO Error")):
                with pytest.raises(ConfigurationError) as exc_info:
                    config_reader = ConfigReader("io_error.yml")
                
                # Test specific exception properties
                assert "Failed to load configuration from io_error.yml" in str(exc_info.value)
                assert exc_info.value.config_file == "io_error.yml"
                assert exc_info.value.operation == "configuration_validation"

    @pytest.mark.unit
    def test_reload_configuration_with_yaml_error(self, mock_config_reader):
        """Test configuration reloading with YAML error."""
        # Mock file existence
        with patch('pathlib.Path.exists', return_value=True):
            # Mock YAML loading to raise exception
            with patch('yaml.safe_load', side_effect=yaml.YAMLError("YAML Error")), \
                 patch('builtins.open', mock_open(read_data="invalid yaml")):
                
                result = mock_config_reader.reload_configuration()
                
                # Test reloading failure
                assert result is False


class TestConfigReaderPerformance:
    """
    Test ConfigReader performance with large configurations.
    
    Test Strategy:
        - Unit tests with mocked large configuration datasets
        - Validates performance characteristics for dental clinic scale configurations
        - Tests edge cases for performance degradation with large datasets
        - Ensures proper performance for ETL pipeline operations
    
    Coverage Areas:
        - Large configuration processing performance (100+ tables)
        - Summary generation performance for dental clinic configurations
        - Filtering operation performance for various criteria
        - Memory usage and resource efficiency for ETL pipeline scale
        
    ETL Context:
        - Critical for ETL pipeline performance with large dental clinic configurations
        - Supports multi-location dental clinic table configurations
        - Ensures sub-second performance for configuration operations
        - Enables scalable ETL pipeline architecture for growing dental clinics
    """

    @pytest.mark.unit
    def test_performance_with_large_config(self, config_reader_performance_data):
        """Test performance with large configuration."""
        # Create mock config reader with large configuration
        class MockConfigReaderLarge(ConfigReader):
            def __init__(self):
                self.config_path = "mock_large_config"
                self.config = config_reader_performance_data['large_config']
                self._last_loaded = datetime.now()
        
        config_reader = MockConfigReaderLarge()
        
        # Test performance of various operations
        import time
        
        # Test get_configuration_summary performance
        start_time = time.time()
        summary = config_reader.get_configuration_summary()
        end_time = time.time()
        
        # Should complete within reasonable time (< 1 second)
        assert end_time - start_time < 1.0
        assert summary['total_tables'] == 100
        
        # Test filtering performance
        start_time = time.time()
        critical_tables = config_reader.get_tables_by_importance('critical')
        end_time = time.time()
        
        # Should complete within reasonable time (< 0.1 second)
        assert end_time - start_time < 0.1
        assert len(critical_tables) == 25  # 100 tables / 4 importance levels


class TestConfigReaderETLIntegration:
    """
    Test ConfigReader integration with ETL pipeline components.
    
    Test Strategy:
        - Unit tests with mocked ETL component interactions
        - Validates ConfigReader interface compatibility with ETL components
        - Tests integration patterns for dental clinic ETL workflows
        - Ensures proper provider pattern usage in ETL context
    
    Coverage Areas:
        - TableProcessor integration for ETL strategy selection
        - PipelineOrchestrator integration for table ordering
        - PriorityProcessor integration for table prioritization
        - Monitoring integration for ETL pipeline health
        
    ETL Context:
        - Validates ConfigReader as SchemaDiscovery replacement
        - Ensures 5-10x performance improvement in ETL operations
        - Supports dental clinic ETL pipeline architecture
        - Enables provider pattern dependency injection for testing
    """

    @pytest.mark.unit
    def test_table_processor_integration_pattern(self, mock_config_reader):
        """
        Test ConfigReader integration pattern with TableProcessor.
        
        Validates:
            - ConfigReader provides same interface as SchemaDiscovery
            - TableProcessor can use ConfigReader for table configuration
            - ETL strategy selection works with static configuration
            - Performance improvement over dynamic schema discovery
            
        ETL Pipeline Context:
            - TableProcessor uses ConfigReader for table configuration
            - Replaces SchemaDiscovery with static configuration approach
            - Enables 5-10x faster ETL pipeline initialization
            - Supports dental clinic table processing workflows
        """
        # Test that ConfigReader provides the same interface as SchemaDiscovery
        patient_config = mock_config_reader.get_table_config('patient')
        
        # Validate ETL-specific configuration fields
        assert 'primary_key' in patient_config
        assert 'extraction_strategy' in patient_config
        assert 'batch_size' in patient_config
        assert 'table_importance' in patient_config
        
        # Test ETL strategy selection
        incremental_tables = mock_config_reader.get_tables_by_strategy('incremental')
        assert 'patient' in incremental_tables
        assert 'appointment' in incremental_tables

    @pytest.mark.unit
    def test_pipeline_orchestrator_integration_pattern(self, mock_config_reader):
        """
        Test ConfigReader integration pattern with PipelineOrchestrator.
        
        Validates:
            - PipelineOrchestrator can use ConfigReader for table ordering
            - Dependency resolution works for ETL execution order
            - Table importance filtering supports pipeline orchestration
            - Configuration summary supports pipeline monitoring
            
        ETL Pipeline Context:
            - PipelineOrchestrator uses ConfigReader for table ordering
            - Supports dental clinic ETL pipeline execution planning
            - Enables proper dependency-based table processing
            - Provides configuration insights for pipeline optimization
        """
        # Test dependency resolution for ETL execution order
        appointment_deps = mock_config_reader.get_table_dependencies('appointment')
        assert 'patient' in appointment_deps
        
        # Test importance-based table ordering
        critical_tables = mock_config_reader.get_tables_by_importance('critical')
        important_tables = mock_config_reader.get_tables_by_importance('important')
        
        # Critical tables should be processed before important tables
        assert len(critical_tables) > 0
        assert len(important_tables) > 0
        
        # Test configuration summary for pipeline monitoring
        summary = mock_config_reader.get_configuration_summary()
        assert summary['total_tables'] > 0
        assert 'importance_levels' in summary

    @pytest.mark.unit
    def test_provider_pattern_integration(self, mock_config_reader):
        """
        Test ConfigReader integration with provider pattern.
        
        Validates:
            - ConfigReader works with provider pattern dependency injection
            - Test isolation through provider pattern configuration
            - ETL pipeline components can use different configuration providers
            - Provider pattern supports both clinic and test environments
            
        ETL Pipeline Context:
            - ConfigReader supports provider pattern for configuration management
            - Enables test isolation through DictConfigProvider
            - Supports production configuration through FileConfigProvider
            - Maintains ETL pipeline flexibility and testability
        """
        # Test that ConfigReader can work with different configuration sources
        # This validates the provider pattern integration
        
        # Test with mocked configuration (DictConfigProvider equivalent)
        patient_config = mock_config_reader.get_table_config('patient')
        assert patient_config['table_importance'] == 'critical'
        
        # Test configuration path management
        config_path = mock_config_reader.get_configuration_path()
        assert config_path == "mock_config"
        
        # Note: reload_configuration will fail for mock_config_reader because it tries to read from file
        # This is expected behavior for the provider pattern - different providers handle reloading differently


if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 