"""
Integration tests for ConfigReader component.

This module tests the ConfigReader class with real file system operations:
- Real YAML file loading and parsing from actual tables.yml
- File system integration with real configuration files
- Configuration validation with test database data
- Integration with ETL pipeline components
- Error handling for real file system scenarios

Following the three-tier testing approach:
- Integration tests: Real configuration files, test database data, ETL pipeline integration
- Target: Real configuration validation and test database table processing
- Order: 1 (Phase 1: Core ETL Pipeline)

ETL Pipeline Context:
    - ConfigReader replaces SchemaDiscovery for static configuration management
    - Critical for nightly ETL pipeline performance (5-10x faster than dynamic discovery)
    - Manages dental clinic table configurations from real tables.yml
    - Supports OpenDental MySQL to PostgreSQL ETL workflows with test databases
    - Enables provider pattern dependency injection for testing
"""

import pytest
import yaml
import tempfile
import os
from pathlib import Path
from datetime import datetime
from typing import Dict, Any

# Import ConfigReader for integration testing
from etl_pipeline.config.config_reader import ConfigReader

# Import custom exceptions for structured error handling
from etl_pipeline.exceptions.configuration import ConfigurationError, EnvironmentError

# Import fixtures for test data
from tests.fixtures.config_reader_fixtures import (
    valid_tables_config,
    minimal_tables_config,
    invalid_tables_config,
    empty_tables_config,
    malformed_yaml_config
)


class TestConfigReaderIntegration:
    """
    Integration tests for ConfigReader with real file system operations.
    
    Test Strategy:
        - Real file system operations with actual YAML files
        - Integration with dental clinic table configurations from real tables.yml
        - Error handling for real file system scenarios
        - Configuration validation with test database data
        - ETL pipeline integration testing
    
    Coverage Areas:
        - Real YAML file loading and parsing for dental clinic tables
        - File system error handling (permissions, missing files)
        - Configuration structure validation for ETL operations
        - Integration with test database table processing workflows
        - Real configuration reloading and caching behavior
        
    ETL Context:
        - Critical for nightly ETL pipeline initialization
        - Replaces SchemaDiscovery with static configuration approach
        - Supports dental clinic table configurations from real tables.yml
        - Enables 5-10x performance improvement over dynamic discovery
    """

    @pytest.mark.integration
    @pytest.mark.order(1)
    @pytest.mark.config
    def test_init_with_real_tables_yml(self):
        """
        Test ConfigReader initialization with real tables.yml file.
        
        Validates:
            - Real file system integration with actual tables.yml
            - YAML parsing of dental clinic table configurations
            - Configuration structure validation for ETL operations
            - Integration with test database table processing workflows
            - Real configuration loading and caching behavior
            
        ETL Pipeline Context:
            - Uses actual tables.yml from etl_pipeline/config/tables.yml
            - Validates dental clinic table configurations (patient, appointment, etc.)
            - Critical for nightly ETL job initialization
            - Supports OpenDental to PostgreSQL data flow configuration with test databases
        """
        # Test with real tables.yml file
        config_reader = ConfigReader()
        
        # Validate basic initialization
        assert config_reader.config_path is not None
        assert config_reader.config is not None
        assert isinstance(config_reader._last_loaded, datetime)
        
        # Validate configuration structure
        assert 'tables' in config_reader.config
        assert len(config_reader.config['tables']) > 0
        
        # Validate dental clinic table configurations
        tables = config_reader.config['tables']
        
        # Check for common dental clinic tables
        dental_clinic_tables = ['patient', 'appointment', 'procedurelog', 'adjustment']
        found_tables = [table for table in dental_clinic_tables if table in tables]
        assert len(found_tables) > 0, f"Expected dental clinic tables not found. Found: {list(tables.keys())[:10]}"
        
        # Validate table configuration structure
        for table_name, config in tables.items():
            assert isinstance(config, dict), f"Table config for {table_name} should be dict"
            assert 'extraction_strategy' in config, f"Missing extraction_strategy for {table_name}"
            assert 'table_importance' in config, f"Missing table_importance for {table_name}"
            assert 'batch_size' in config, f"Missing batch_size for {table_name}"

    @pytest.mark.integration
    @pytest.mark.order(1)
    @pytest.mark.config
    def test_get_table_config_with_real_data(self):
        """
        Test table configuration retrieval with real configuration files.
        
        Validates:
            - Real table configuration retrieval for dental clinic tables
            - Configuration structure validation for ETL operations
            - Integration with test database table processing workflows
            - Real configuration caching and performance behavior
            
        ETL Pipeline Context:
            - Tests with actual dental clinic table configurations from real tables.yml
            - Validates ETL-specific configuration fields (batch_size, extraction_strategy)
            - Critical for TableProcessor integration
            - Supports test database table processing workflows
        """
        config_reader = ConfigReader()
        
        # Test with real dental clinic tables
        dental_clinic_tables = ['patient', 'appointment', 'procedurelog', 'adjustment']
        
        for table_name in dental_clinic_tables:
            config = config_reader.get_table_config(table_name)
            
            if config:  # Table exists in configuration
                # Validate ETL-specific configuration fields
                assert 'extraction_strategy' in config, f"Missing extraction_strategy for {table_name}"
                assert 'table_importance' in config, f"Missing table_importance for {table_name}"
                assert 'batch_size' in config, f"Missing batch_size for {table_name}"
                assert 'estimated_size_mb' in config, f"Missing estimated_size_mb for {table_name}"
                
                # Validate configuration values
                assert config['extraction_strategy'] in ['incremental', 'full_table', 'incremental_chunked'], \
                    f"Invalid extraction_strategy for {table_name}: {config['extraction_strategy']}"
                assert config['table_importance'] in ['important', 'audit', 'standard'], \
                    f"Invalid table_importance for {table_name}: {config['table_importance']}"
                assert isinstance(config['batch_size'], int), \
                    f"batch_size should be int for {table_name}: {type(config['batch_size'])}"
                assert config['batch_size'] > 0, \
                    f"batch_size should be positive for {table_name}: {config['batch_size']}"

    @pytest.mark.integration
    @pytest.mark.order(1)
    @pytest.mark.config
    def test_get_tables_by_importance_with_real_data(self):
        """
        Test table filtering by importance with real configuration files.
        
        Validates:
            - Real importance-based filtering for dental clinic tables
            - Configuration structure validation for ETL operations
            - Integration with PriorityProcessor for table ordering
            - Real configuration filtering and performance behavior
            
        ETL Pipeline Context:
            - Tests with actual dental clinic table importance classifications from real tables.yml
            - Validates ETL pipeline table prioritization
            - Critical for PriorityProcessor integration
            - Supports test database ETL execution order
        """
        config_reader = ConfigReader()
        
        # Test all importance levels (based on actual tables.yml)
        importance_levels = ['important', 'audit', 'standard']
        
        for importance in importance_levels:
            tables = config_reader.get_tables_by_importance(importance)
            
            # Validate that returned tables have correct importance
            for table_name in tables:
                config = config_reader.get_table_config(table_name)
                assert config['table_importance'] == importance, \
                    f"Table {table_name} has importance {config['table_importance']}, expected {importance}"
            
                    # Log results for monitoring
        if tables:
            print(f"Found {len(tables)} tables with importance '{importance}': {tables[:5]}...")
        else:
            print(f"No tables found with importance '{importance}'")

    @pytest.mark.integration
    @pytest.mark.order(1)
    @pytest.mark.config
    def test_get_tables_by_strategy_with_real_data(self):
        """
        Test table filtering by extraction strategy with real configuration files.
        
        Validates:
            - Real strategy-based filtering for dental clinic tables
            - Configuration structure validation for ETL operations
            - Integration with TableProcessor for ETL strategy selection
            - Real configuration filtering and performance behavior
            
        ETL Pipeline Context:
            - Tests with actual dental clinic table extraction strategies from real tables.yml
            - Validates ETL pipeline strategy selection
            - Critical for TableProcessor integration
            - Supports test database ETL workflow optimization
        """
        config_reader = ConfigReader()
        
        # Test all extraction strategies
        strategies = ['incremental', 'full_table', 'incremental_chunked']
        
        for strategy in strategies:
            tables = config_reader.get_tables_by_strategy(strategy)
            
            # Validate that returned tables have correct strategy
            for table_name in tables:
                config = config_reader.get_table_config(table_name)
                assert config['extraction_strategy'] == strategy, \
                    f"Table {table_name} has strategy {config['extraction_strategy']}, expected {strategy}"
            
                    # Log results for monitoring
        if tables:
            print(f"Found {len(tables)} tables with strategy '{strategy}': {tables[:5]}...")
        else:
            print(f"No tables found with strategy '{strategy}'")

    @pytest.mark.integration
    @pytest.mark.order(1)
    @pytest.mark.config
    def test_get_large_tables_with_real_data(self):
        """
        Test large table identification with real configuration files.
        
        Validates:
            - Real size-based filtering for dental clinic tables
            - Configuration structure validation for ETL operations
            - Integration with monitoring and alerting systems
            - Real configuration filtering and performance behavior
            
        ETL Pipeline Context:
            - Tests with actual dental clinic table size estimates from real tables.yml
            - Validates ETL pipeline monitoring and alerting
            - Critical for performance optimization
            - Supports test database ETL capacity planning
        """
        config_reader = ConfigReader()
        
        # Test different size thresholds
        thresholds = [10.0, 50.0, 100.0, 200.0]
        
        for threshold in thresholds:
            large_tables = config_reader.get_large_tables(threshold)
            
            # Validate that returned tables are actually large
            for table_name in large_tables:
                config = config_reader.get_table_config(table_name)
                size_mb = config.get('estimated_size_mb', 0)
                assert size_mb > threshold, \
                    f"Table {table_name} has size {size_mb}MB, should be > {threshold}MB"
            
                    # Log results for monitoring
        if large_tables:
            print(f"Found {len(large_tables)} tables larger than {threshold}MB: {large_tables}")
        else:
            print(f"No tables found larger than {threshold}MB")

    @pytest.mark.integration
    @pytest.mark.order(1)
    @pytest.mark.config
    def test_get_monitored_tables_with_real_data(self):
        """
        Test monitored table identification with real configuration files.
        
        Validates:
            - Real monitoring-based filtering for dental clinic tables
            - Configuration structure validation for ETL operations
            - Integration with monitoring and alerting systems
            - Real configuration filtering and performance behavior
            
        ETL Pipeline Context:
            - Tests with actual dental clinic table monitoring configurations from real tables.yml
            - Validates ETL pipeline monitoring and alerting
            - Critical for pipeline health monitoring
            - Supports test database ETL reliability
        """
        config_reader = ConfigReader()
        
        monitored_tables = config_reader.get_monitored_tables()
        
        # Validate that returned tables have monitoring enabled
        for table_name in monitored_tables:
            config = config_reader.get_table_config(table_name)
            monitoring_config = config.get('monitoring', {})
            assert monitoring_config.get('alert_on_failure', False), \
                f"Table {table_name} should have alert_on_failure=True"
        
        # Log results for monitoring
        if monitored_tables:
            print(f"Found {len(monitored_tables)} monitored tables: {monitored_tables}")
        else:
            print(f"No monitored tables found")

    @pytest.mark.integration
    @pytest.mark.order(1)
    @pytest.mark.config
    def test_get_table_dependencies_with_real_data(self):
        """
        Test table dependency resolution with real configuration files.
        
        Validates:
            - Real dependency resolution for dental clinic tables
            - Configuration structure validation for ETL operations
            - Integration with PipelineOrchestrator for execution order
            - Real configuration dependency resolution behavior
            
        ETL Pipeline Context:
            - Tests with actual dental clinic table dependencies from real tables.yml
            - Validates ETL pipeline execution order
            - Critical for PipelineOrchestrator integration
            - Supports test database ETL data integrity
        """
        config_reader = ConfigReader()
        
        # Test dependencies for common dental clinic tables
        test_tables = ['appointment', 'procedurelog', 'adjustment', 'claim']
        
        for table_name in test_tables:
            dependencies = config_reader.get_table_dependencies(table_name)
            
            # Validate that dependencies are lists
            assert isinstance(dependencies, list), \
                f"Dependencies for {table_name} should be list, got {type(dependencies)}"
            
            # Validate that dependencies exist in configuration
            for dep_table in dependencies:
                dep_config = config_reader.get_table_config(dep_table)
                assert dep_config, f"Dependency table {dep_table} not found in configuration"
            
            # Log results for monitoring
            if dependencies:
                print(f"Table {table_name} has {len(dependencies)} dependencies: {dependencies}")
            else:
                print(f"Table {table_name} has no dependencies")

    @pytest.mark.integration
    @pytest.mark.order(1)
    @pytest.mark.config
    def test_get_configuration_summary_with_real_data(self):
        """
        Test configuration summary generation with real configuration files.
        
        Validates:
            - Real configuration summary generation for dental clinic tables
            - Configuration structure validation for ETL operations
            - Integration with monitoring and reporting systems
            - Real configuration summary accuracy and completeness
            
        ETL Pipeline Context:
            - Tests with actual dental clinic table configurations from real tables.yml
            - Validates ETL pipeline monitoring and reporting
            - Critical for pipeline health monitoring
            - Supports test database ETL analytics and insights
        """
        config_reader = ConfigReader()
        
        summary = config_reader.get_configuration_summary()
        
        # Validate summary structure
        required_fields = [
            'total_tables', 'importance_levels', 'extraction_strategies',
            'size_ranges', 'monitored_tables', 'modeled_tables', 'last_loaded'
        ]
        
        for field in required_fields:
            assert field in summary, f"Missing field '{field}' in summary"
        
        # Validate summary values
        assert summary['total_tables'] > 0, "Should have at least one table"
        assert isinstance(summary['importance_levels'], dict), "importance_levels should be dict"
        assert isinstance(summary['extraction_strategies'], dict), "extraction_strategies should be dict"
        assert isinstance(summary['size_ranges'], dict), "size_ranges should be dict"
        assert summary['monitored_tables'] >= 0, "monitored_tables should be non-negative"
        assert summary['modeled_tables'] >= 0, "modeled_tables should be non-negative"
        
        # Validate size ranges
        size_ranges = summary['size_ranges']
        assert 'small' in size_ranges, "Missing 'small' size range"
        assert 'medium' in size_ranges, "Missing 'medium' size range"
        assert 'large' in size_ranges, "Missing 'large' size range"
        
        # Log summary for monitoring
        print(f"Configuration Summary:")
        print(f"  Total tables: {summary['total_tables']}")
        print(f"  Importance levels: {summary['importance_levels']}")
        print(f"  Extraction strategies: {summary['extraction_strategies']}")
        print(f"  Size ranges: {summary['size_ranges']}")
        print(f"  Monitored tables: {summary['monitored_tables']}")
        print(f"  Modeled tables: {summary['modeled_tables']}")

    @pytest.mark.integration
    @pytest.mark.order(1)
    @pytest.mark.config
    def test_validate_configuration_with_real_data(self):
        """
        Test configuration validation with real dental clinic data.
        
        Validates:
            - Real configuration validation for dental clinic tables
            - Configuration structure validation for ETL operations
            - Integration with ETL pipeline quality assurance
            - Real configuration validation accuracy and completeness
            
        ETL Pipeline Context:
            - Tests with actual dental clinic table configurations
            - Validates ETL pipeline configuration quality
            - Critical for pipeline reliability and performance
            - Supports dental clinic ETL quality assurance
        """
        config_reader = ConfigReader()
        
        issues = config_reader.validate_configuration()
        
        # Validate issues structure
        required_issue_types = [
            'missing_batch_size', 'missing_extraction_strategy', 'missing_importance',
            'invalid_batch_size', 'large_tables_without_monitoring'
        ]
        
        for issue_type in required_issue_types:
            assert issue_type in issues, f"Missing issue type '{issue_type}'"
            assert isinstance(issues[issue_type], list), f"{issue_type} should be list"
        
        # Log validation results for monitoring
        print(f"Configuration Validation Results:")
        for issue_type, tables in issues.items():
            if tables:
                print(f"  {issue_type}: {len(tables)} tables affected")
                print(f"    Tables: {tables[:5]}...")
            else:
                print(f"  {issue_type}: No issues found")

    @pytest.mark.integration
    @pytest.mark.order(1)
    @pytest.mark.config
    def test_reload_configuration_with_real_file(self):
        """
        Test configuration reloading with real tables.yml file.
        
        Validates:
            - Real configuration reloading from actual tables.yml
            - File system integration and caching behavior
            - Configuration freshness tracking for ETL operations
            - Real configuration reloading performance and reliability
            
        ETL Pipeline Context:
            - Tests with actual tables.yml file from ETL pipeline
            - Validates configuration hot-reloading capability
            - Critical for runtime configuration updates
            - Supports dental clinic ETL configuration management
        """
        config_reader = ConfigReader()
        
        # Get initial configuration
        initial_config = config_reader.config.copy()
        initial_last_loaded = config_reader._last_loaded
        
        # Reload configuration
        success = config_reader.reload_configuration()
        
        # Validate reloading success
        assert success is True, "Configuration reloading should succeed"
        
        # Validate configuration is still valid
        assert config_reader.config is not None, "Configuration should not be None after reload"
        assert 'tables' in config_reader.config, "Configuration should have 'tables' section"
        
        # Validate timestamp update
        assert config_reader._last_loaded > initial_last_loaded, \
            "Last loaded timestamp should be updated after reload"
        
        # Validate configuration consistency
        assert len(config_reader.config['tables']) == len(initial_config['tables']), \
            "Number of tables should remain consistent after reload"

    @pytest.mark.integration
    @pytest.mark.order(1)
    @pytest.mark.config
    def test_get_configuration_path_with_real_file(self):
        """
        Test configuration path retrieval with real tables.yml file.
        
        Validates:
            - Real configuration path resolution for actual tables.yml
            - File system integration and path handling
            - Configuration path consistency for ETL operations
            - Real configuration path accuracy and reliability
            
        ETL Pipeline Context:
            - Tests with actual tables.yml file path from ETL pipeline
            - Validates configuration file path resolution
            - Critical for configuration file management
            - Supports dental clinic ETL configuration deployment
        """
        config_reader = ConfigReader()
        
        config_path = config_reader.get_configuration_path()
        
        # Validate path format
        assert isinstance(config_path, str), "Configuration path should be string"
        assert config_path.endswith('tables.yml'), "Configuration path should end with 'tables.yml'"
        
        # Validate file exists
        path_obj = Path(config_path)
        assert path_obj.exists(), f"Configuration file should exist: {config_path}"
        assert path_obj.is_file(), f"Configuration path should be a file: {config_path}"
        
        # Log path for monitoring
        print(f"Configuration file path: {config_path}")

    @pytest.mark.integration
    @pytest.mark.order(1)
    @pytest.mark.config
    def test_get_last_loaded_with_real_file(self):
        """
        Test last loaded timestamp retrieval with real tables.yml file.
        
        Validates:
            - Real timestamp tracking for actual tables.yml loading
            - Configuration freshness tracking for ETL operations
            - Timestamp accuracy and consistency
            - Real timestamp behavior and reliability
            
        ETL Pipeline Context:
            - Tests with actual tables.yml file loading timestamps
            - Validates configuration freshness monitoring
            - Critical for configuration cache management
            - Supports dental clinic ETL configuration monitoring
        """
        config_reader = ConfigReader()
        
        last_loaded = config_reader.get_last_loaded()
        
        # Validate timestamp format
        assert isinstance(last_loaded, datetime), "Last loaded should be datetime"
        
        # Validate timestamp is recent (within last hour)
        now = datetime.now()
        time_diff = now - last_loaded
        assert time_diff.total_seconds() < 3600, \
            f"Last loaded timestamp should be recent, but is {time_diff.total_seconds()} seconds old"
        
        # Log timestamp for monitoring
        print(f"Configuration last loaded: {last_loaded}")

    @pytest.mark.integration
    @pytest.mark.order(1)
    @pytest.mark.config
    def test_etl_pipeline_integration_patterns(self):
        """
        Test ConfigReader integration patterns with ETL pipeline components.
        
        Validates:
            - Real integration patterns with ETL pipeline components
            - Configuration structure compatibility for ETL operations
            - Provider pattern integration for dependency injection
            - Real ETL pipeline integration behavior and reliability
            
        ETL Pipeline Context:
            - Tests with actual ETL pipeline integration patterns
            - Validates ConfigReader as SchemaDiscovery replacement
            - Critical for ETL pipeline component integration
            - Supports dental clinic ETL pipeline architecture
        """
        config_reader = ConfigReader()
        
        # Test TableProcessor integration pattern
        # ConfigReader should provide same interface as SchemaDiscovery
        patient_config = config_reader.get_table_config('patient')
        if patient_config:
            # Validate ETL-specific configuration fields
            assert 'extraction_strategy' in patient_config, "Missing extraction_strategy for ETL"
            assert 'batch_size' in patient_config, "Missing batch_size for ETL"
            assert 'table_importance' in patient_config, "Missing table_importance for ETL"
        
        # Test PipelineOrchestrator integration pattern
        # ConfigReader should support dependency resolution
        appointment_deps = config_reader.get_table_dependencies('appointment')
        assert isinstance(appointment_deps, list), "Dependencies should be list for orchestration"
        
        # Test PriorityProcessor integration pattern
        # ConfigReader should support importance-based filtering
        important_tables = config_reader.get_tables_by_importance('important')
        audit_tables = config_reader.get_tables_by_importance('audit')
        assert isinstance(important_tables, list), "Important tables should be list for prioritization"
        assert isinstance(audit_tables, list), "Audit tables should be list for prioritization"
        
        # Test monitoring integration pattern
        # ConfigReader should support monitoring configuration
        monitored_tables = config_reader.get_monitored_tables()
        assert isinstance(monitored_tables, list), "Monitored tables should be list for monitoring"
        
        # Log integration patterns for monitoring
        print(f"ETL Pipeline Integration Patterns:")
        print(f"  Patient config available: {bool(patient_config)}")
        print(f"  Appointment dependencies: {len(appointment_deps)}")
        print(f"  Important tables: {len(important_tables)}")
        print(f"  Audit tables: {len(audit_tables)}")
        print(f"  Monitored tables: {len(monitored_tables)}")

    @pytest.mark.integration
    @pytest.mark.order(1)
    @pytest.mark.config
    def test_dental_clinic_specific_configurations(self):
        """
        Test dental clinic specific configuration patterns.
        
        Validates:
            - Real dental clinic table configuration patterns
            - Configuration structure validation for dental clinic ETL
            - Integration with dental clinic data processing workflows
            - Real dental clinic configuration accuracy and completeness
            
        ETL Pipeline Context:
            - Tests with actual dental clinic table configurations
            - Validates dental clinic specific ETL patterns
            - Critical for dental clinic data processing
            - Supports dental clinic ETL workflow optimization
        """
        config_reader = ConfigReader()
        
        # Test dental clinic specific tables (based on actual tables.yml)
        dental_clinic_tables = [
            'patient', 'appointment', 'procedurelog', 'adjustment', 'claim',
            'allergy', 'medication', 'recall', 'payment', 'insurance'
        ]
        
        found_tables = []
        for table_name in dental_clinic_tables:
            config = config_reader.get_table_config(table_name)
            if config:
                found_tables.append(table_name)
                
                # Validate dental clinic specific configuration patterns
                assert 'extraction_strategy' in config, f"Missing extraction_strategy for {table_name}"
                assert 'table_importance' in config, f"Missing table_importance for {table_name}"
                assert 'batch_size' in config, f"Missing batch_size for {table_name}"
                
                # Validate dental clinic specific values
                assert config['extraction_strategy'] in ['incremental', 'full_table', 'incremental_chunked'], \
                    f"Invalid extraction_strategy for {table_name}"
                assert config['table_importance'] in ['important', 'audit', 'standard'], \
                    f"Invalid table_importance for {table_name}"
                assert config['batch_size'] > 0, f"Invalid batch_size for {table_name}"
        
        # Validate that we found some dental clinic tables
        assert len(found_tables) > 0, "Should find at least some dental clinic tables"
        
        # Log dental clinic configuration for monitoring
        print(f"Dental Clinic Configuration:")
        print(f"  Found {len(found_tables)} dental clinic tables: {found_tables}")
        
        # Test dental clinic specific importance patterns
        important_tables = config_reader.get_tables_by_importance('important')
        audit_tables = config_reader.get_tables_by_importance('audit')
        
        print(f"  Important tables: {important_tables}")
        print(f"  Audit tables: {audit_tables}")

    @pytest.mark.integration
    @pytest.mark.order(1)
    @pytest.mark.config
    def test_performance_with_real_configuration(self):
        """
        Test ConfigReader performance with real dental clinic configuration.
        
        Validates:
            - Real performance characteristics with actual tables.yml
            - Configuration loading and caching performance
            - Query performance for large dental clinic configurations
            - Real performance behavior and optimization
            
        ETL Pipeline Context:
            - Tests with actual dental clinic table configurations
            - Validates ETL pipeline performance requirements
            - Critical for nightly ETL job performance
            - Supports dental clinic ETL performance optimization
        """
        import time
        
        config_reader = ConfigReader()
        
        # Test configuration loading performance
        start_time = time.time()
        config_reader.reload_configuration()
        load_time = time.time() - start_time
        
        # Configuration loading should be fast (< 1 second)
        assert load_time < 1.0, f"Configuration loading took {load_time:.2f}s, should be < 1s"
        
        # Test query performance
        start_time = time.time()
        summary = config_reader.get_configuration_summary()
        summary_time = time.time() - start_time
        
        # Summary generation should be fast (< 0.1 second)
        assert summary_time < 0.1, f"Summary generation took {summary_time:.3f}s, should be < 0.1s"
        
        # Test filtering performance
        start_time = time.time()
        important_tables = config_reader.get_tables_by_importance('important')
        audit_tables = config_reader.get_tables_by_importance('audit')
        filtering_time = time.time() - start_time
        
        # Filtering should be fast (< 0.1 second)
        assert filtering_time < 0.1, f"Filtering took {filtering_time:.3f}s, should be < 0.1s"
        
        # Log performance metrics for monitoring
        print(f"Performance Metrics:")
        print(f"  Configuration loading: {load_time:.3f}s")
        print(f"  Summary generation: {summary_time:.3f}s")
        print(f"  Filtering operations: {filtering_time:.3f}s")
        print(f"  Total tables: {summary['total_tables']}")
        print(f"  Important tables: {len(important_tables)}")
        print(f"  Audit tables: {len(audit_tables)}")

    # Removed test_loads_latest_versioned_tables_yml - we only use tables.yml with metadata versioning


class TestConfigReaderErrorHandlingIntegration:
    """
    Integration tests for ConfigReader error handling with real file system.
    
    Test Strategy:
        - Real file system error scenarios
        - Error handling for missing and invalid files
        - Configuration validation error handling
        - Real error recovery and graceful degradation
    
    Coverage Areas:
        - File not found error handling
        - Permission error handling
        - YAML parsing error handling
        - Configuration validation error handling
        - Error recovery and graceful degradation
        
    ETL Context:
        - Critical for ETL pipeline error resilience
        - Prevents pipeline failures due to configuration issues
        - Supports proper error reporting for dental clinic ETL
        - Enables graceful degradation when configuration is unavailable
    """

    @pytest.mark.integration
    @pytest.mark.order(1)
    @pytest.mark.config
    def test_file_not_found_error_handling(self):
        """
        Test error handling for non-existent configuration file.
        
        Validates:
            - Real file not found error handling
            - Error message clarity and accuracy
            - Error propagation for ETL pipeline
            - Real error handling behavior and reliability
            
        ETL Pipeline Context:
            - Tests with actual file system error scenarios
            - Validates ETL pipeline error resilience
            - Critical for configuration file management
            - Supports dental clinic ETL error handling
        """
        # Test with non-existent file
        non_existent_path = "/path/to/nonexistent/tables.yml"
        
        with pytest.raises(ConfigurationError) as exc_info:
            ConfigReader(non_existent_path)
        
        # Validate error message
        error_message = str(exc_info.value)
        assert "Configuration file not found" in error_message
        assert non_existent_path in error_message

    @pytest.mark.integration
    @pytest.mark.order(1)
    @pytest.mark.config
    def test_invalid_yaml_error_handling(self, temp_invalid_yaml_file):
        """
        Test error handling for invalid YAML configuration.
        
        Validates:
            - Real YAML parsing error handling
            - Error message clarity and accuracy
            - Error propagation for ETL pipeline
            - Real error handling behavior and reliability
            
        ETL Pipeline Context:
            - Tests with actual YAML parsing error scenarios
            - Validates ETL pipeline error resilience
            - Critical for configuration file validation
            - Supports dental clinic ETL error handling
        """
        # Test with invalid YAML file
        with pytest.raises(ConfigurationError):
            ConfigReader(temp_invalid_yaml_file)

    @pytest.mark.integration
    @pytest.mark.order(1)
    @pytest.mark.config
    def test_missing_tables_section_error_handling(self, temp_missing_tables_file):
        """
        Test error handling for configuration with missing tables section.
        
        Validates:
            - Real configuration structure error handling
            - Error message clarity and accuracy
            - Error propagation for ETL pipeline
            - Real error handling behavior and reliability
            
        ETL Pipeline Context:
            - Tests with actual configuration structure error scenarios
            - Validates ETL pipeline error resilience
            - Critical for configuration file validation
            - Supports dental clinic ETL error handling
        """
        # Test with configuration missing tables section
        with pytest.raises(ConfigurationError) as exc_info:
            ConfigReader(temp_missing_tables_file)
        
        # Validate error message
        error_message = str(exc_info.value)
        assert "Invalid configuration file" in error_message

    @pytest.fixture
    def temp_invalid_yaml_file(self):
        """Temporary invalid YAML file for testing."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
            f.write("""
            tables:
              patient:
                table_importance: important
                extraction_strategy: incremental
                batch_size: 1000
              appointment:
                table_importance: standard
                extraction_strategy: incremental
                batch_size: 500
                dependencies: [patient
                # Missing closing bracket - malformed YAML
            """)
            config_path = f.name
        
        yield config_path
        
        # Clean up
        if os.path.exists(config_path):
            os.unlink(config_path)

    @pytest.fixture
    def temp_missing_tables_file(self):
        """Temporary configuration file missing tables section for testing."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
            f.write("""
            # Configuration without tables section
            other_section:
              some_value: true
            """)
            config_path = f.name
        
        yield config_path
        
        # Clean up
        if os.path.exists(config_path):
            os.unlink(config_path)


if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 