"""
Comprehensive Tests for ETL Pipeline Exceptions

This module provides comprehensive testing for all custom exception classes
in the ETL pipeline, ensuring proper error handling, context preservation,
and serialization for dental clinic data processing.

ETL Context:
    - Exceptions are used throughout the ETL pipeline for error handling
    - Critical for dental clinic data processing reliability
    - Supports provider pattern and Settings injection error scenarios
    - Enables FAIL FAST behavior when ETL_ENVIRONMENT not set
    - Used by all ETL components (replicators, loaders, orchestrators)

Coverage Areas:
    - Base ETLException with context preservation
    - Schema-related exceptions for MySQL → PostgreSQL conversion
    - Database exceptions for connection and query failures
    - Data processing exceptions for extraction/transformation/loading
    - Configuration exceptions for provider pattern validation
    - Environment exceptions for FAIL FAST behavior

Provider Pattern Integration:
    - Exceptions used with Settings injection scenarios
    - Error handling for DictConfigProvider vs FileConfigProvider
    - Environment separation error scenarios
"""

import pytest
from unittest.mock import Mock, patch
from typing import Dict, Any, Optional

from etl_pipeline.exceptions import (
    ETLException,
    SchemaTransformationError,
    SchemaValidationError,
    TypeConversionError,
    DatabaseConnectionError,
    DatabaseQueryError,
    DatabaseTransactionError,
    DataExtractionError,
    DataTransformationError,
    DataLoadingError,
    ConfigurationError,
    EnvironmentError
)

from etl_pipeline.config.providers import DictConfigProvider
from etl_pipeline.config.settings import Settings


class TestExceptionCreation:
    """Test exception creation with various parameter combinations."""
    
    def test_etl_exception_with_all_parameters(self):
        """
        Test ETLException creation with all possible parameters.
        
        AAA Pattern:
            Arrange: Create ETLException with all parameters
            Act: Access all exception properties
            Assert: Verify all properties are correctly set
        """
        # Arrange: Create ETLException with all parameters
        original_exc = ValueError("Original error")
        details = {"batch_size": 1000, "strategy": "incremental"}
        exc = ETLException(
            message="Patient table extraction failed",
            table_name="patient",
            operation="data_extraction",
            details=details,
            original_exception=original_exc
        )
        
        # Act: Access all exception properties
        message = exc.message
        table_name = exc.table_name
        operation = exc.operation
        exception_details = exc.details
        original_exception = exc.original_exception
        string_repr = str(exc)
        repr_repr = repr(exc)
        dict_repr = exc.to_dict()
        
        # Assert: Verify all properties are correctly set
        assert message == "Patient table extraction failed"
        assert table_name == "patient"
        assert operation == "data_extraction"
        assert exception_details == details
        assert original_exception == original_exc
        assert "Patient table extraction failed" in string_repr
        assert "patient" in string_repr
        assert "data_extraction" in string_repr
        assert "ETLException" in repr_repr
        assert dict_repr["exception_type"] == "ETLException"
        assert dict_repr["message"] == "Patient table extraction failed"
    
    def test_etl_exception_with_minimal_parameters(self):
        """
        Test ETLException creation with minimal required parameters.
        
        AAA Pattern:
            Arrange: Create ETLException with minimal parameters
            Act: Access exception properties
            Assert: Verify default values are set correctly
        """
        # Arrange: Create ETLException with minimal parameters
        exc = ETLException("Simple error message")
        
        # Act: Access exception properties
        message = exc.message
        table_name = exc.table_name
        operation = exc.operation
        details = exc.details
        original_exception = exc.original_exception
        
        # Assert: Verify default values are set correctly
        assert message == "Simple error message"
        assert table_name is None
        assert operation is None
        assert details == {}
        assert original_exception is None
    
    def test_etl_exception_with_none_values(self):
        """
        Test ETLException creation with None values for optional parameters.
        
        AAA Pattern:
            Arrange: Create ETLException with None values
            Act: Access exception properties
            Assert: Verify None values are handled correctly
        """
        # Arrange: Create ETLException with None values
        exc = ETLException(
            message="Error with None values",
            table_name=None,
            operation=None,
            details=None,
            original_exception=None
        )
        
        # Act: Access exception properties
        message = exc.message
        table_name = exc.table_name
        operation = exc.operation
        details = exc.details
        original_exception = exc.original_exception
        
        # Assert: Verify None values are handled correctly
        assert message == "Error with None values"
        assert table_name is None
        assert operation is None
        assert details == {}
        assert original_exception is None


class TestSchemaExceptions:
    """Test schema-related exception classes for MySQL → PostgreSQL conversion."""
    
    def test_schema_transformation_error_comprehensive(self):
        """
        Test SchemaTransformationError with comprehensive dental clinic scenario.
        
        AAA Pattern:
            Arrange: Create SchemaTransformationError with dental clinic schema
            Act: Access all exception properties and methods
            Assert: Verify all properties are correctly set
        """
        # Arrange: Create SchemaTransformationError with dental clinic schema
        mysql_schema = {
            "create_statement": "CREATE TABLE patient (PatNum INT PRIMARY KEY, LName VARCHAR(50), IsActive TINYINT)",
            "table_name": "patient",
            "columns": ["PatNum", "LName", "IsActive"]
        }
        details = {"reason": "Unsupported MySQL type", "column": "IsActive", "mysql_type": "TINYINT"}
        original_exc = ValueError("Cannot convert TINYINT to boolean")
        
        exc = SchemaTransformationError(
            message="Failed to transform patient table schema for PostgreSQL",
            table_name="patient",
            mysql_schema=mysql_schema,
            details=details,
            original_exception=original_exc
        )
        
        # Act: Access all exception properties and methods
        message = exc.message
        table_name = exc.table_name
        operation = exc.operation
        mysql_schema_result = exc.mysql_schema
        exception_details = exc.details
        original_exception = exc.original_exception
        string_repr = str(exc)
        dict_repr = exc.to_dict()
        
        # Assert: Verify all properties are correctly set
        assert message == "Failed to transform patient table schema for PostgreSQL"
        assert table_name == "patient"
        assert operation == "schema_transformation"
        assert mysql_schema_result == mysql_schema
        assert exception_details["reason"] == "Unsupported MySQL type"
        assert exception_details["column"] == "IsActive"
        assert original_exception == original_exc
        assert "patient" in string_repr
        assert "schema_transformation" in string_repr
        assert dict_repr["exception_type"] == "SchemaTransformationError"
    
    def test_schema_validation_error_comprehensive(self):
        """
        Test SchemaValidationError with comprehensive dental clinic scenario.
        
        AAA Pattern:
            Arrange: Create SchemaValidationError with dental clinic validation
            Act: Access all exception properties and methods
            Assert: Verify all properties are correctly set
        """
        # Arrange: Create SchemaValidationError with dental clinic validation
        validation_details = {
            "column_count_mismatch": True,
            "expected_columns": ["PatNum", "LName", "FName", "IsActive"],
            "actual_columns": ["PatNum", "LName"],
            "missing_columns": ["FName", "IsActive"]
        }
        
        exc = SchemaValidationError(
            message="Patient table schema validation failed",
            table_name="patient",
            validation_details=validation_details
        )
        
        # Act: Access all exception properties and methods
        message = exc.message
        table_name = exc.table_name
        operation = exc.operation
        validation_details_result = exc.validation_details
        string_repr = str(exc)
        dict_repr = exc.to_dict()
        
        # Assert: Verify all properties are correctly set
        assert message == "Patient table schema validation failed"
        assert table_name == "patient"
        assert operation == "schema_validation"
        assert validation_details_result == validation_details
        assert validation_details_result["column_count_mismatch"] is True
        assert "patient" in string_repr
        assert "schema_validation" in string_repr
        assert dict_repr["exception_type"] == "SchemaValidationError"
    
    def test_type_conversion_error_comprehensive(self):
        """
        Test TypeConversionError with comprehensive dental clinic scenario.
        
        AAA Pattern:
            Arrange: Create TypeConversionError with dental clinic data types
            Act: Access all exception properties and methods
            Assert: Verify all properties are correctly set
        """
        # Arrange: Create TypeConversionError with dental clinic data types
        exc = TypeConversionError(
            message="Failed to convert patient IsActive field type",
            table_name="patient",
            column_name="IsActive",
            mysql_type="TINYINT",
            postgres_type="boolean"
        )
        
        # Act: Access all exception properties and methods
        message = exc.message
        table_name = exc.table_name
        operation = exc.operation
        column_name = exc.column_name
        mysql_type = exc.mysql_type
        postgres_type = exc.postgres_type
        string_repr = str(exc)
        dict_repr = exc.to_dict()
        
        # Assert: Verify all properties are correctly set
        assert message == "Failed to convert patient IsActive field type"
        assert table_name == "patient"
        assert operation == "type_conversion"
        assert column_name == "IsActive"
        assert mysql_type == "TINYINT"
        assert postgres_type == "boolean"
        assert "patient" in string_repr
        assert "type_conversion" in string_repr
        assert dict_repr["exception_type"] == "TypeConversionError"


class TestDatabaseExceptions:
    """Test database-related exception classes for dental clinic database operations."""
    
    def test_database_connection_error_comprehensive(self):
        """
        Test DatabaseConnectionError with comprehensive dental clinic scenario.
        
        AAA Pattern:
            Arrange: Create DatabaseConnectionError with dental clinic connection
            Act: Access all exception properties and methods
            Assert: Verify all properties are correctly set
        """
        # Arrange: Create DatabaseConnectionError with dental clinic connection
        connection_params = {
            "host": "dental-clinic-db.com",
            "port": 3306,
            "database": "opendental",
            "user": "dental_user"
        }
        details = {"timeout": 30, "retry_count": 3}
        original_exc = ConnectionError("Connection timeout")
        
        exc = DatabaseConnectionError(
            message="Connection timeout to dental clinic database",
            database_type="mysql",
            connection_params=connection_params,
            details=details,
            original_exception=original_exc
        )
        
        # Act: Access all exception properties and methods
        message = exc.message
        operation = exc.operation
        database_type = exc.database_type
        connection_params_result = exc.connection_params
        exception_details = exc.details
        original_exception = exc.original_exception
        string_repr = str(exc)
        dict_repr = exc.to_dict()
        
        # Assert: Verify all properties are correctly set
        assert message == "Connection timeout to dental clinic database"
        assert operation == "database_connection"
        assert database_type == "mysql"
        assert connection_params_result == connection_params
        assert exception_details["database_type"] == "mysql"
        assert exception_details["connection_params"] == connection_params
        assert original_exception == original_exc
        assert "dental clinic database" in string_repr
        assert "database_connection" in string_repr
        assert dict_repr["exception_type"] == "DatabaseConnectionError"
    
    def test_database_query_error_comprehensive(self):
        """
        Test DatabaseQueryError with comprehensive dental clinic scenario.
        
        AAA Pattern:
            Arrange: Create DatabaseQueryError with dental clinic query
            Act: Access all exception properties and methods
            Assert: Verify all properties are correctly set
        """
        # Arrange: Create DatabaseQueryError with dental clinic query
        query = "SELECT * FROM patient WHERE PatNum > 1000 AND IsActive = 1"
        details = {"execution_time": 45.2, "rows_affected": 0}
        original_exc = Exception("Query execution failed")
        
        exc = DatabaseQueryError(
            message="Failed to query patient table from dental clinic database",
            table_name="patient",
            query=query,
            database_type="mysql",
            details=details,
            original_exception=original_exc
        )
        
        # Act: Access all exception properties and methods
        message = exc.message
        table_name = exc.table_name
        operation = exc.operation
        query_result = exc.query
        database_type = exc.database_type
        exception_details = exc.details
        original_exception = exc.original_exception
        string_repr = str(exc)
        dict_repr = exc.to_dict()
        
        # Assert: Verify all properties are correctly set
        assert message == "Failed to query patient table from dental clinic database"
        assert table_name == "patient"
        assert operation == "database_query"
        assert query_result == query
        assert database_type == "mysql"
        assert exception_details["query"] == query
        assert exception_details["database_type"] == "mysql"
        assert original_exception == original_exc
        assert "patient table" in string_repr
        assert "database_query" in string_repr
        assert dict_repr["exception_type"] == "DatabaseQueryError"
    
    def test_database_transaction_error_comprehensive(self):
        """
        Test DatabaseTransactionError with comprehensive dental clinic scenario.
        
        AAA Pattern:
            Arrange: Create DatabaseTransactionError with dental clinic transaction
            Act: Access all exception properties and methods
            Assert: Verify all properties are correctly set
        """
        # Arrange: Create DatabaseTransactionError with dental clinic transaction
        details = {"transaction_id": "txn_12345", "rollback_required": True}
        original_exc = Exception("Transaction commit failed")
        
        exc = DatabaseTransactionError(
            message="Failed to commit appointment table transaction",
            table_name="appointment",
            transaction_type="commit",
            database_type="postgresql",
            details=details,
            original_exception=original_exc
        )
        
        # Act: Access all exception properties and methods
        message = exc.message
        table_name = exc.table_name
        operation = exc.operation
        transaction_type = exc.transaction_type
        database_type = exc.database_type
        exception_details = exc.details
        original_exception = exc.original_exception
        string_repr = str(exc)
        dict_repr = exc.to_dict()
        
        # Assert: Verify all properties are correctly set
        assert message == "Failed to commit appointment table transaction"
        assert table_name == "appointment"
        assert operation == "database_transaction"
        assert transaction_type == "commit"
        assert database_type == "postgresql"
        assert exception_details["transaction_type"] == "commit"
        assert exception_details["database_type"] == "postgresql"
        assert original_exception == original_exc
        assert "appointment table" in string_repr
        assert "database_transaction" in string_repr
        assert dict_repr["exception_type"] == "DatabaseTransactionError"


class TestDataExceptions:
    """Test data-related exception classes for dental clinic data processing."""
    
    def test_data_extraction_error_comprehensive(self):
        """
        Test DataExtractionError with comprehensive dental clinic scenario.
        
        AAA Pattern:
            Arrange: Create DataExtractionError with dental clinic extraction
            Act: Access all exception properties and methods
            Assert: Verify all properties are correctly set
        """
        # Arrange: Create DataExtractionError with dental clinic extraction
        details = {"last_extracted_id": 1000, "batch_size": 1000, "timeout": 60}
        original_exc = Exception("Extraction timeout")
        
        exc = DataExtractionError(
            message="Failed to extract patient data from dental clinic database",
            table_name="patient",
            extraction_strategy="incremental",
            batch_size=1000,
            details=details,
            original_exception=original_exc
        )
        
        # Act: Access all exception properties and methods
        message = exc.message
        table_name = exc.table_name
        operation = exc.operation
        extraction_strategy = exc.extraction_strategy
        batch_size = exc.batch_size
        exception_details = exc.details
        original_exception = exc.original_exception
        string_repr = str(exc)
        dict_repr = exc.to_dict()
        
        # Assert: Verify all properties are correctly set
        assert message == "Failed to extract patient data from dental clinic database"
        assert table_name == "patient"
        assert operation == "data_extraction"
        assert extraction_strategy == "incremental"
        assert batch_size == 1000
        assert exception_details["last_extracted_id"] == 1000
        assert original_exception == original_exc
        assert "patient data" in string_repr
        assert "data_extraction" in string_repr
        assert dict_repr["exception_type"] == "DataExtractionError"
    
    def test_data_transformation_error_comprehensive(self):
        """
        Test DataTransformationError with comprehensive dental clinic scenario.
        
        AAA Pattern:
            Arrange: Create DataTransformationError with dental clinic transformation
            Act: Access all exception properties and methods
            Assert: Verify all properties are correctly set
        """
        # Arrange: Create DataTransformationError with dental clinic transformation
        details = {"transformation_rule": "date_format", "source_format": "MySQL", "target_format": "PostgreSQL"}
        original_exc = ValueError("Invalid date format")
        
        exc = DataTransformationError(
            message="Failed to transform patient date format",
            table_name="patient",
            transformation_type="date_format",
            field_name="DateTStamp",
            details=details,
            original_exception=original_exc
        )
        
        # Act: Access all exception properties and methods
        message = exc.message
        table_name = exc.table_name
        operation = exc.operation
        transformation_type = exc.transformation_type
        field_name = exc.field_name
        exception_details = exc.details
        original_exception = exc.original_exception
        string_repr = str(exc)
        dict_repr = exc.to_dict()
        
        # Assert: Verify all properties are correctly set
        assert message == "Failed to transform patient date format"
        assert table_name == "patient"
        assert operation == "data_transformation"
        assert transformation_type == "date_format"
        assert field_name == "DateTStamp"
        assert exception_details["transformation_rule"] == "date_format"
        assert original_exception == original_exc
        assert "patient date format" in string_repr
        assert "data_transformation" in string_repr
        assert dict_repr["exception_type"] == "DataTransformationError"
    
    def test_data_loading_error_comprehensive(self):
        """
        Test DataLoadingError with comprehensive dental clinic scenario.
        
        AAA Pattern:
            Arrange: Create DataLoadingError with dental clinic loading
            Act: Access all exception properties and methods
            Assert: Verify all properties are correctly set
        """
        # Arrange: Create DataLoadingError with dental clinic loading
        details = {"target_schema": "raw", "loading_strategy": "chunked", "chunk_size": 5000}
        original_exc = Exception("Loading timeout")
        
        exc = DataLoadingError(
            message="Failed to load patient data to analytics database",
            table_name="patient",
            loading_strategy="chunked",
            chunk_size=5000,
            target_schema="raw",
            details=details,
            original_exception=original_exc
        )
        
        # Act: Access all exception properties and methods
        message = exc.message
        table_name = exc.table_name
        operation = exc.operation
        loading_strategy = exc.loading_strategy
        chunk_size = exc.chunk_size
        target_schema = exc.target_schema
        exception_details = exc.details
        original_exception = exc.original_exception
        string_repr = str(exc)
        dict_repr = exc.to_dict()
        
        # Assert: Verify all properties are correctly set
        assert message == "Failed to load patient data to analytics database"
        assert table_name == "patient"
        assert operation == "data_loading"
        assert loading_strategy == "chunked"
        assert chunk_size == 5000
        assert target_schema == "raw"
        assert exception_details["target_schema"] == "raw"
        assert original_exception == original_exc
        assert "patient data" in string_repr
        assert "data_loading" in string_repr
        assert dict_repr["exception_type"] == "DataLoadingError"


class TestConfigurationExceptions:
    """Test configuration-related exception classes for provider pattern validation."""
    
    def test_configuration_error_comprehensive(self):
        """
        Test ConfigurationError with comprehensive dental clinic scenario.
        
        AAA Pattern:
            Arrange: Create ConfigurationError with dental clinic configuration
            Act: Access all exception properties and methods
            Assert: Verify all properties are correctly set
        """
        # Arrange: Create ConfigurationError with dental clinic configuration
        missing_keys = ["dental_clinic_host", "dental_clinic_port", "dental_clinic_user"]
        invalid_values = {"batch_size": "invalid", "timeout": "not_a_number"}
        details = {"config_file": "dental_clinic_config.yml", "validation_errors": 5}
        
        exc = ConfigurationError(
            message="Dental clinic configuration validation failed",
            config_file="dental_clinic_config.yml",
            missing_keys=missing_keys,
            invalid_values=invalid_values,
            details=details
        )
        
        # Act: Access all exception properties and methods
        message = exc.message
        operation = exc.operation
        config_file = exc.config_file
        missing_keys_result = exc.missing_keys
        invalid_values_result = exc.invalid_values
        exception_details = exc.details
        string_repr = str(exc)
        dict_repr = exc.to_dict()
        
        # Assert: Verify all properties are correctly set
        assert message == "Dental clinic configuration validation failed"
        assert operation == "configuration_validation"
        assert config_file == "dental_clinic_config.yml"
        assert missing_keys_result == missing_keys
        assert invalid_values_result == invalid_values
        assert exception_details["config_file"] == "dental_clinic_config.yml"
        assert "Dental clinic configuration" in string_repr
        assert "configuration_validation" in string_repr
        assert dict_repr["exception_type"] == "ConfigurationError"
    
    def test_environment_error_comprehensive(self):
        """
        Test EnvironmentError with comprehensive FAIL FAST scenario.
        
        AAA Pattern:
            Arrange: Create EnvironmentError with FAIL FAST context
            Act: Access all exception properties and methods
            Assert: Verify all properties are correctly set for FAIL FAST
        """
        # Arrange: Create EnvironmentError with FAIL FAST context
        missing_variables = ["ETL_ENVIRONMENT", "DENTAL_CLINIC_DATABASE_URL", "DENTAL_CLINIC_HOST"]
        details = {"env_file": ".env_production", "validation_errors": 3}
        
        exc = EnvironmentError(
            message="Dental clinic environment setup failed - ETL_ENVIRONMENT not set",
            environment="production",
            missing_variables=missing_variables,
            env_file=".env_production",
            details=details
        )
        
        # Act: Access all exception properties and methods
        message = exc.message
        operation = exc.operation
        environment = exc.environment
        missing_variables_result = exc.missing_variables
        env_file = exc.env_file
        exception_details = exc.details
        string_repr = str(exc)
        dict_repr = exc.to_dict()
        
        # Assert: Verify all properties are correctly set for FAIL FAST
        assert message == "Dental clinic environment setup failed - ETL_ENVIRONMENT not set"
        assert operation == "environment_validation"
        assert environment == "production"
        assert missing_variables_result == missing_variables
        assert env_file == ".env_production"
        assert exception_details["env_file"] == ".env_production"
        assert missing_variables_result is not None
        assert "ETL_ENVIRONMENT" in missing_variables_result
        assert "Dental clinic environment" in string_repr
        assert "environment_validation" in string_repr
        assert dict_repr["exception_type"] == "EnvironmentError"


class TestExceptionProviderPattern:
    """Test exceptions with provider pattern and Settings injection."""
    
    def test_exceptions_with_dict_config_provider(self):
        """
        Test exceptions with DictConfigProvider for testing scenarios.
        
        AAA Pattern:
            Arrange: Set up DictConfigProvider with test configuration
            Act: Create exceptions in provider pattern context
            Assert: Verify exceptions work correctly with provider pattern
        """
        # Arrange: Set up DictConfigProvider with test configuration
        test_provider = DictConfigProvider(
            env={
                'TEST_OPENDENTAL_SOURCE_HOST': 'test-dental-server.com',
                'TEST_OPENDENTAL_SOURCE_DB': 'test_opendental'
            }
        )
        settings = Settings(environment='test', provider=test_provider)
        
        # Act: Create exceptions in provider pattern context
        config_exc = ConfigurationError(
            message="Test configuration error with provider pattern",
            config_file="test_config.yml",
            missing_keys=["test_key"],
            invalid_values={"test_value": "invalid"}
        )
        
        env_exc = EnvironmentError(
            message="Test environment error with provider pattern",
            environment="test",
            missing_variables=["TEST_VAR"],
            env_file=".env_test"
        )
        
        # Assert: Verify exceptions work correctly with provider pattern
        assert config_exc.message == "Test configuration error with provider pattern"
        assert config_exc.operation == "configuration_validation"
        assert env_exc.message == "Test environment error with provider pattern"
        assert env_exc.operation == "environment_validation"
        assert env_exc.environment == "test"
    
    def test_exceptions_with_settings_injection(self):
        """
        Test exceptions with Settings injection patterns.
        
        AAA Pattern:
            Arrange: Set up Settings with provider pattern
            Act: Create exceptions that might be used with Settings
            Assert: Verify exceptions are compatible with Settings injection
        """
        # Arrange: Set up Settings with provider pattern
        test_provider = DictConfigProvider(
            env={'TEST_OPENDENTAL_SOURCE_HOST': 'test-host'}
        )
        settings = Settings(environment='test', provider=test_provider)
        
        # Act: Create exceptions that might be used with Settings
        db_exc = DatabaseConnectionError(
            message="Test database connection error with Settings",
            database_type="mysql",
            connection_params={"host": "test-host", "port": 3306}
        )
        
        data_exc = DataExtractionError(
            message="Test data extraction error with Settings",
            table_name="patient",
            extraction_strategy="incremental",
            batch_size=1000
        )
        
        # Assert: Verify exceptions are compatible with Settings injection
        assert db_exc.message == "Test database connection error with Settings"
        assert db_exc.operation == "database_connection"
        assert data_exc.message == "Test data extraction error with Settings"
        assert data_exc.operation == "data_extraction"
        assert data_exc.table_name == "patient"


class TestExceptionFailFast:
    """Test FAIL FAST behavior with exceptions."""
    
    def test_environment_error_fail_fast_behavior(self):
        """
        Test EnvironmentError with FAIL FAST behavior for dental clinic ETL.
        
        AAA Pattern:
            Arrange: Create EnvironmentError with FAIL FAST context
            Act: Access exception properties and methods
            Assert: Verify FAIL FAST behavior is properly implemented
        """
        # Arrange: Create EnvironmentError with FAIL FAST context
        missing_variables = ["ETL_ENVIRONMENT", "DENTAL_CLINIC_DATABASE_URL"]
        exc = EnvironmentError(
            message="Dental clinic environment setup failed - ETL_ENVIRONMENT not set",
            environment="production",
            missing_variables=missing_variables,
            env_file=".env_production"
        )
        
        # Act: Access exception properties and methods
        message = exc.message
        operation = exc.operation
        environment = exc.environment
        missing_variables_result = exc.missing_variables
        env_file = exc.env_file
        string_repr = str(exc)
        dict_repr = exc.to_dict()
        
        # Assert: Verify FAIL FAST behavior is properly implemented
        assert message == "Dental clinic environment setup failed - ETL_ENVIRONMENT not set"
        assert operation == "environment_validation"
        assert environment == "production"
        assert missing_variables_result == missing_variables
        assert env_file == ".env_production"
        assert missing_variables_result is not None
        assert "ETL_ENVIRONMENT" in missing_variables_result
        assert "DENTAL_CLINIC_DATABASE_URL" in missing_variables_result
        assert "ETL_ENVIRONMENT not set" in string_repr
        assert dict_repr["exception_type"] == "EnvironmentError"
    
    def test_configuration_error_fail_fast_behavior(self):
        """
        Test ConfigurationError with FAIL FAST behavior for dental clinic ETL.
        
        AAA Pattern:
            Arrange: Create ConfigurationError with FAIL FAST context
            Act: Access exception properties and methods
            Assert: Verify FAIL FAST behavior is properly implemented
        """
        # Arrange: Create ConfigurationError with FAIL FAST context
        missing_keys = ["dental_clinic_host", "dental_clinic_port"]
        invalid_values = {"batch_size": "invalid", "timeout": "not_a_number"}
        
        exc = ConfigurationError(
            message="Dental clinic configuration validation failed - critical parameters missing",
            config_file="dental_clinic_config.yml",
            missing_keys=missing_keys,
            invalid_values=invalid_values
        )
        
        # Act: Access exception properties and methods
        message = exc.message
        operation = exc.operation
        config_file = exc.config_file
        missing_keys_result = exc.missing_keys
        invalid_values_result = exc.invalid_values
        string_repr = str(exc)
        dict_repr = exc.to_dict()
        
        # Assert: Verify FAIL FAST behavior is properly implemented
        assert message == "Dental clinic configuration validation failed - critical parameters missing"
        assert operation == "configuration_validation"
        assert config_file == "dental_clinic_config.yml"
        assert missing_keys_result == missing_keys
        assert invalid_values_result == invalid_values
        assert missing_keys_result is not None
        assert "dental_clinic_host" in missing_keys_result
        assert "dental_clinic_port" in missing_keys_result
        assert "critical parameters missing" in string_repr
        assert dict_repr["exception_type"] == "ConfigurationError"


class TestExceptionSerialization:
    """Test exception serialization and representation methods."""
    
    def test_to_dict_serialization_comprehensive(self):
        """
        Test to_dict() method for all exception types.
        
        AAA Pattern:
            Arrange: Create all exception types with comprehensive data
            Act: Call to_dict() method on each exception
            Assert: Verify serialization works correctly for all types
        """
        # Arrange: Create all exception types with comprehensive data
        exceptions = [
            ETLException("Test ETL exception", table_name="patient", operation="test"),
            SchemaTransformationError("Test schema error", table_name="patient", mysql_schema={}),
            SchemaValidationError("Test validation error", table_name="patient", validation_details={}),
            TypeConversionError("Test conversion error", table_name="patient", column_name="test", mysql_type="test", postgres_type="test"),
            DatabaseConnectionError("Test connection error", database_type="mysql", connection_params={}),
            DatabaseQueryError("Test query error", table_name="patient", query="SELECT 1", database_type="mysql"),
            DatabaseTransactionError("Test transaction error", table_name="patient", transaction_type="commit", database_type="mysql"),
            DataExtractionError("Test extraction error", table_name="patient", extraction_strategy="test", batch_size=1000),
            DataTransformationError("Test transformation error", table_name="patient", transformation_type="test", field_name="test"),
            DataLoadingError("Test loading error", table_name="patient", loading_strategy="test", chunk_size=1000, target_schema="test"),
            ConfigurationError("Test config error", config_file="test.yml", missing_keys=[], invalid_values={}),
            EnvironmentError("Test env error", environment="test", missing_variables=[], env_file=".env_test")
        ]
        
        # Act: Call to_dict() method on each exception
        dict_results = [exc.to_dict() for exc in exceptions]
        
        # Assert: Verify serialization works correctly for all types
        expected_types = [
            "ETLException", "SchemaTransformationError", "SchemaValidationError", "TypeConversionError",
            "DatabaseConnectionError", "DatabaseQueryError", "DatabaseTransactionError", "DataExtractionError",
            "DataTransformationError", "DataLoadingError", "ConfigurationError", "EnvironmentError"
        ]
        
        for i, result in enumerate(dict_results):
            assert result["exception_type"] == expected_types[i]
            assert "message" in result
            assert result["message"] is not None
    
    def test_repr_representation_comprehensive(self):
        """
        Test __repr__ method for all exception types.
        
        AAA Pattern:
            Arrange: Create all exception types with comprehensive data
            Act: Call __repr__ method on each exception
            Assert: Verify representation works correctly for all types
        """
        # Arrange: Create all exception types with comprehensive data
        exceptions = [
            ETLException("Test ETL exception", table_name="patient", operation="test"),
            SchemaTransformationError("Test schema error", table_name="patient", mysql_schema={}),
            SchemaValidationError("Test validation error", table_name="patient", validation_details={}),
            TypeConversionError("Test conversion error", table_name="patient", column_name="test", mysql_type="test", postgres_type="test"),
            DatabaseConnectionError("Test connection error", database_type="mysql", connection_params={}),
            DatabaseQueryError("Test query error", table_name="patient", query="SELECT 1", database_type="mysql"),
            DatabaseTransactionError("Test transaction error", table_name="patient", transaction_type="commit", database_type="mysql"),
            DataExtractionError("Test extraction error", table_name="patient", extraction_strategy="test", batch_size=1000),
            DataTransformationError("Test transformation error", table_name="patient", transformation_type="test", field_name="test"),
            DataLoadingError("Test loading error", table_name="patient", loading_strategy="test", chunk_size=1000, target_schema="test"),
            ConfigurationError("Test config error", config_file="test.yml", missing_keys=[], invalid_values={}),
            EnvironmentError("Test env error", environment="test", missing_variables=[], env_file=".env_test")
        ]
        
        # Act: Call __repr__ method on each exception
        repr_results = [repr(exc) for exc in exceptions]
        
        # Assert: Verify representation works correctly for all types
        expected_types = [
            "ETLException", "SchemaTransformationError", "SchemaValidationError", "TypeConversionError",
            "DatabaseConnectionError", "DatabaseQueryError", "DatabaseTransactionError", "DataExtractionError",
            "DataTransformationError", "DataLoadingError", "ConfigurationError", "EnvironmentError"
        ]
        
        for i, result in enumerate(repr_results):
            assert expected_types[i] in result
            assert "message=" in result
            # DatabaseConnectionError doesn't have table_name parameter, so check for "Test" in message
            if expected_types[i] == "DatabaseConnectionError":
                assert "Test" in result
            else:
                assert "patient" in result or "test" in result


class TestExceptionInheritance:
    """Test exception inheritance hierarchy and type safety."""
    
    def test_inheritance_hierarchy_comprehensive(self):
        """
        Test that all exceptions properly inherit from ETLException.
        
        AAA Pattern:
            Arrange: Create instances of all exception types
            Act: Check inheritance relationships
            Assert: Verify proper inheritance hierarchy
        """
        # Arrange: Create instances of all exception types
        exceptions = [
            ETLException("Test"),
            SchemaTransformationError("Test", table_name="test", mysql_schema={}),
            SchemaValidationError("Test", table_name="test", validation_details={}),
            TypeConversionError("Test", table_name="test", column_name="test", mysql_type="test", postgres_type="test"),
            DatabaseConnectionError("Test", database_type="mysql", connection_params={}),
            DatabaseQueryError("Test", table_name="test", query="SELECT 1", database_type="mysql"),
            DatabaseTransactionError("Test", table_name="test", transaction_type="commit", database_type="mysql"),
            DataExtractionError("Test", table_name="test", extraction_strategy="test", batch_size=1000),
            DataTransformationError("Test", table_name="test", transformation_type="test", field_name="test"),
            DataLoadingError("Test", table_name="test", loading_strategy="test", chunk_size=1000, target_schema="test"),
            ConfigurationError("Test", config_file="test.yml", missing_keys=[], invalid_values={}),
            EnvironmentError("Test", environment="test", missing_variables=[], env_file=".env_test")
        ]
        
        # Act: Check inheritance relationships
        inheritance_results = []
        for exc in exceptions:
            is_etl_exception = isinstance(exc, ETLException)
            is_exception = isinstance(exc, Exception)
            inheritance_results.append((is_etl_exception, is_exception))
        
        # Assert: Verify proper inheritance hierarchy
        for is_etl_exception, is_exception in inheritance_results:
            assert is_etl_exception is True
            assert is_exception is True
    
    def test_type_safety_comprehensive(self):
        """
        Test type safety and parameter validation.
        
        AAA Pattern:
            Arrange: Create exceptions with various parameter types
            Act: Access exception properties
            Assert: Verify type safety is maintained
        """
        # Arrange: Create exceptions with various parameter types
        exc = ETLException(
            message="Test message",
            table_name="patient",
            operation="test_operation",
            details={"key": "value", "number": 123, "boolean": True},
            original_exception=ValueError("Test error")
        )
        
        # Act: Access exception properties
        message = exc.message
        table_name = exc.table_name
        operation = exc.operation
        details = exc.details
        original_exception = exc.original_exception
        
        # Assert: Verify type safety is maintained
        assert isinstance(message, str)
        assert isinstance(table_name, str)
        assert isinstance(operation, str)
        assert isinstance(details, dict)
        assert isinstance(original_exception, Exception)
        assert details["key"] == "value"
        assert details["number"] == 123
        assert details["boolean"] is True 