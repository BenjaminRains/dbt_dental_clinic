"""
Shared validation utilities for configuration tests.

This module provides common validation functions used across all configuration test files
to eliminate duplication and ensure consistent validation logic.
"""


def validate_general_section(config):
    """Validate the general section of pipeline configuration."""
    general = config['general']
    
    # Test required fields
    required_fields = ['pipeline_name', 'batch_size', 'parallel_jobs']
    for field in required_fields:
        assert field in general, f"Required field '{field}' missing from general section"
    
    # Test data types
    assert isinstance(general['pipeline_name'], str)
    assert isinstance(general['environment'], str)
    assert isinstance(general['timezone'], str)
    assert isinstance(general['max_retries'], int)
    assert isinstance(general['retry_delay_seconds'], int)
    assert isinstance(general['batch_size'], int)
    assert isinstance(general['parallel_jobs'], int)
    
    # Test value ranges
    assert general['max_retries'] >= 0
    assert general['retry_delay_seconds'] >= 0
    assert general['batch_size'] > 0
    assert general['parallel_jobs'] > 0
    assert general['parallel_jobs'] <= 20  # Reasonable upper limit


def validate_connections_section(config):
    """Validate the connections section of pipeline configuration."""
    connections = config['connections']
    
    # Test required connection types
    required_connections = ['source', 'replication', 'analytics']
    for conn_type in required_connections:
        assert conn_type in connections, f"Required connection '{conn_type}' missing"
    
    # Test connection parameters
    for conn_type, conn_config in connections.items():
        # Test required parameters
        required_params = ['pool_size', 'pool_timeout', 'pool_recycle']
        for param in required_params:
            assert param in conn_config, f"Required parameter '{param}' missing from {conn_type}"
            assert isinstance(conn_config[param], int), f"Parameter '{param}' in {conn_type} must be int"
            assert conn_config[param] > 0, f"Parameter '{param}' in {conn_type} must be positive"


def validate_stages_section(config):
    """Validate the stages section of pipeline configuration."""
    stages = config['stages']
    
    # Test required stages
    required_stages = ['extract', 'load', 'transform']
    for stage in required_stages:
        assert stage in stages, f"Required stage '{stage}' missing"
    
    # Test stage parameters
    for stage_name, stage_config in stages.items():
        # Test required parameters
        required_params = ['enabled', 'timeout_minutes', 'error_threshold']
        for param in required_params:
            assert param in stage_config, f"Required parameter '{param}' missing from {stage_name}"
        
        # Test data types
        assert isinstance(stage_config['enabled'], bool)
        assert isinstance(stage_config['timeout_minutes'], int)
        assert isinstance(stage_config['error_threshold'], float)
        
        # Test value ranges
        assert stage_config['timeout_minutes'] > 0
        assert 0.0 <= stage_config['error_threshold'] <= 1.0


def validate_logging_section(config):
    """Validate the logging section of pipeline configuration."""
    logging_config = config['logging']
    
    # Test required fields
    required_fields = ['level', 'format', 'file', 'console']
    for field in required_fields:
        assert field in logging_config, f"Required field '{field}' missing from logging section"
    
    # Test log level
    valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
    assert logging_config['level'] in valid_levels
    
    # Test file logging config
    file_config = logging_config['file']
    assert isinstance(file_config['enabled'], bool)
    assert isinstance(file_config['path'], str)
    assert isinstance(file_config['max_size_mb'], int)
    assert isinstance(file_config['backup_count'], int)
    assert file_config['max_size_mb'] > 0
    assert file_config['backup_count'] >= 0


def validate_error_handling_section(config):
    """Validate the error handling section of pipeline configuration."""
    error_config = config['error_handling']
    
    # Test required fields
    required_fields = ['max_consecutive_failures', 'failure_notification_threshold', 'auto_retry']
    for field in required_fields:
        assert field in error_config, f"Required field '{field}' missing from error_handling section"
    
    # Test auto_retry config
    auto_retry = error_config['auto_retry']
    assert isinstance(auto_retry['enabled'], bool)
    assert isinstance(auto_retry['max_attempts'], int)
    assert isinstance(auto_retry['delay_minutes'], int)
    assert auto_retry['max_attempts'] > 0
    assert auto_retry['delay_minutes'] >= 0


def validate_business_rules(config):
    """Validate business rules for pipeline configuration."""
    general = config['general']
    connections = config['connections']
    stages = config['stages']
    
    # Business rule: batch size should be reasonable for ETL processing
    batch_size = general['batch_size']
    assert 1000 <= batch_size <= 100000, f"Batch size {batch_size} is outside reasonable range"
    
    # Business rule: parallel jobs should be reasonable for database connections
    parallel_jobs = general['parallel_jobs']
    assert 1 <= parallel_jobs <= 20, f"Parallel jobs {parallel_jobs} is outside reasonable range"
    
    # Business rule: timeouts should be reasonable for ETL operations
    for stage_name, stage_config in stages.items():
        timeout = stage_config['timeout_minutes']
        assert 5 <= timeout <= 180, f"Timeout {timeout} for {stage_name} is outside reasonable range"
    
    # Business rule: error thresholds should be reasonable for data quality
    for stage_name, stage_config in stages.items():
        threshold = stage_config['error_threshold']
        assert 0.0 <= threshold <= 0.1, f"Error threshold {threshold} for {stage_name} is too high"
    
    # Business rule: pool sizes should be reasonable for database connections
    for conn_type, conn_config in connections.items():
        pool_size = conn_config['pool_size']
        assert 1 <= pool_size <= 20, f"Pool size {pool_size} for {conn_type} is outside reasonable range"
    
    # Business rule: pool recycle should be reasonable
    for conn_type, conn_config in connections.items():
        pool_recycle = conn_config['pool_recycle']
        assert 300 <= pool_recycle <= 7200, f"Pool recycle {pool_recycle} for {conn_type} is outside reasonable range"


def validate_configuration_consistency(config):
    """Validate consistency across configuration sections."""
    general = config['general']
    connections = config['connections']
    stages = config['stages']
    
    # Business rule: parallel jobs should not exceed the smallest connection pool size
    parallel_jobs = general['parallel_jobs']
    min_pool_size = min(config['pool_size'] for config in connections.values())
    assert parallel_jobs <= min_pool_size, f"Parallel jobs ({parallel_jobs}) exceeds minimum pool size ({min_pool_size})"
    
    # Business rule: extract should be fastest, transform should be slowest
    extract_timeout = stages['extract']['timeout_minutes']
    load_timeout = stages['load']['timeout_minutes']
    transform_timeout = stages['transform']['timeout_minutes']
    
    assert extract_timeout <= load_timeout, "Extract timeout should be <= load timeout"
    assert load_timeout <= transform_timeout, "Load timeout should be <= transform timeout"
    
    # Business rule: error thresholds should be consistent across stages
    thresholds = [config['error_threshold'] for config in stages.values()]
    assert len(set(thresholds)) <= 2, "Too many different error threshold values"


def validate_complete_pipeline_config(config):
    """Validate a complete pipeline configuration."""
    validate_general_section(config)
    validate_connections_section(config)
    validate_stages_section(config)
    validate_logging_section(config)
    validate_error_handling_section(config)
    validate_business_rules(config)
    validate_configuration_consistency(config) 