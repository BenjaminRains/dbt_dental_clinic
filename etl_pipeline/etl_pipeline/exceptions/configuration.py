"""
Configuration-Related Exception Classes

This module provides custom exceptions for configuration-related operations in the ETL pipeline,
including configuration validation and environment setup errors.
"""

from typing import Optional, Dict, Any
from .base import ETLException


class ConfigurationError(ETLException):
    """
    Raised when configuration is invalid or missing.
    
    This exception is raised when there are issues with configuration files,
    missing required settings, or invalid configuration values.
    """
    
    def __init__(
        self,
        message: str,
        config_file: Optional[str] = None,
        missing_keys: Optional[list] = None,
        invalid_values: Optional[Dict[str, Any]] = None,
        details: Optional[Dict[str, Any]] = None,
        original_exception: Optional[Exception] = None
    ):
        """
        Initialize ConfigurationError.
        
        Args:
            message: Human-readable error message
            config_file: Path to the configuration file with issues
            missing_keys: List of missing configuration keys
            invalid_values: Dictionary of invalid configuration values
            details: Additional error details
            original_exception: The original exception that caused this error
        """
        config_details = {
            'config_file': config_file,
            'missing_keys': missing_keys,
            'invalid_values': invalid_values
        }
        if details:
            config_details.update(details)
        
        super().__init__(
            message=message,
            operation="configuration_validation",
            details=config_details,
            original_exception=original_exception
        )
        self.config_file = config_file
        self.missing_keys = missing_keys
        self.invalid_values = invalid_values


class EnvironmentError(ETLException):
    """
    Raised when environment configuration is invalid.
    
    This exception is raised when there are issues with environment setup,
    missing environment variables, or invalid environment configuration.
    """
    
    def __init__(
        self,
        message: str,
        environment: Optional[str] = None,
        missing_variables: Optional[list] = None,
        env_file: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        original_exception: Optional[Exception] = None
    ):
        """
        Initialize EnvironmentError.
        
        Args:
            message: Human-readable error message
            environment: The environment that failed validation
            missing_variables: List of missing environment variables
            env_file: Path to the environment file with issues
            details: Additional error details
            original_exception: The original exception that caused this error
        """
        env_details = {
            'environment': environment,
            'missing_variables': missing_variables,
            'env_file': env_file
        }
        if details:
            env_details.update(details)
        
        super().__init__(
            message=message,
            operation="environment_validation",
            details=env_details,
            original_exception=original_exception
        )
        self.environment = environment
        self.missing_variables = missing_variables
        self.env_file = env_file 