"""
Unit Tests for Custom Exception Classes

This module tests the custom exception classes to ensure they provide
proper error handling, context preservation, and serialization.
"""

import pytest
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


class TestETLExceptionBase:
    """Test the base ETLException class."""
    
    def test_basic_exception_creation(self):
        """Test basic exception creation with minimal parameters."""
        exc = ETLException("Test error message")
        assert str(exc) == "Test error message"
        assert exc.message == "Test error message"
        assert exc.table_name is None
        assert exc.operation is None
        assert exc.details == {}
        assert exc.original_exception is None
    
    def test_exception_with_context(self):
        """Test exception creation with full context."""
        original_exc = ValueError("Original error")
        exc = ETLException(
            message="Database connection failed",
            table_name="patient",
            operation="data_extraction",
            details={"host": "localhost", "port": 3306},
            original_exception=original_exc
        )
        
        assert exc.message == "Database connection failed"
        assert exc.table_name == "patient"
        assert exc.operation == "data_extraction"
        assert exc.details == {"host": "localhost", "port": 3306}
        assert exc.original_exception == original_exc
        
        # Test string representation
        expected = "Database connection failed | Table: patient | Operation: data_extraction | Details: host=localhost, port=3306 | Original error: Original error"
        assert str(exc) == expected
    
    def test_exception_to_dict(self):
        """Test exception serialization to dictionary."""
        exc = ETLException(
            message="Test error",
            table_name="test_table",
            operation="test_operation",
            details={"key": "value"}
        )
        
        result = exc.to_dict()
        assert result["exception_type"] == "ETLException"
        assert result["message"] == "Test error"
        assert result["table_name"] == "test_table"
        assert result["operation"] == "test_operation"
        assert result["details"] == {"key": "value"}
        assert result["original_exception"] is None
    
    def test_exception_repr(self):
        """Test exception representation."""
        exc = ETLException(
            message="Test error",
            table_name="test_table",
            operation="test_operation",
            details={"key": "value"}
        )
        
        expected = "ETLException(message='Test error', table_name='test_table', operation='test_operation', details={'key': 'value'})"
        assert repr(exc) == expected


class TestSchemaExceptions:
    """Test schema-related exception classes."""
    
    def test_schema_transformation_error(self):
        """Test SchemaTransformationError."""
        mysql_schema = {"create_statement": "CREATE TABLE test..."}
        exc = SchemaTransformationError(
            message="Failed to transform schema",
            table_name="patient",
            mysql_schema=mysql_schema,
            details={"reason": "Unsupported type"}
        )
        
        assert exc.message == "Failed to transform schema"
        assert exc.table_name == "patient"
        assert exc.operation == "schema_transformation"
        assert exc.mysql_schema == mysql_schema
        assert exc.details["reason"] == "Unsupported type"
    
    def test_schema_validation_error(self):
        """Test SchemaValidationError."""
        validation_details = {"column_count_mismatch": True}
        exc = SchemaValidationError(
            message="Schema validation failed",
            table_name="appointment",
            validation_details=validation_details
        )
        
        assert exc.message == "Schema validation failed"
        assert exc.table_name == "appointment"
        assert exc.operation == "schema_validation"
        assert exc.validation_details == validation_details
    
    def test_type_conversion_error(self):
        """Test TypeConversionError."""
        exc = TypeConversionError(
            message="Type conversion failed",
            table_name="patient",
            column_name="IsActive",
            mysql_type="TINYINT",
            postgres_type="boolean"
        )
        
        assert exc.message == "Type conversion failed"
        assert exc.table_name == "patient"
        assert exc.operation == "type_conversion"
        assert exc.column_name == "IsActive"
        assert exc.mysql_type == "TINYINT"
        assert exc.postgres_type == "boolean"


class TestDatabaseExceptions:
    """Test database-related exception classes."""
    
    def test_database_connection_error(self):
        """Test DatabaseConnectionError."""
        connection_params = {"host": "localhost", "port": 3306}
        exc = DatabaseConnectionError(
            message="Connection timeout",
            database_type="mysql",
            connection_params=connection_params
        )
        
        assert exc.message == "Connection timeout"
        assert exc.operation == "database_connection"
        assert exc.database_type == "mysql"
        assert exc.connection_params == connection_params
    
    def test_database_query_error(self):
        """Test DatabaseQueryError."""
        exc = DatabaseQueryError(
            message="Query execution failed",
            table_name="patient",
            query="SELECT * FROM patient",
            database_type="postgresql"
        )
        
        assert exc.message == "Query execution failed"
        assert exc.table_name == "patient"
        assert exc.operation == "database_query"
        assert exc.query == "SELECT * FROM patient"
        assert exc.database_type == "postgresql"
    
    def test_database_transaction_error(self):
        """Test DatabaseTransactionError."""
        exc = DatabaseTransactionError(
            message="Transaction commit failed",
            table_name="appointment",
            transaction_type="commit",
            database_type="postgresql"
        )
        
        assert exc.message == "Transaction commit failed"
        assert exc.table_name == "appointment"
        assert exc.operation == "database_transaction"
        assert exc.transaction_type == "commit"
        assert exc.database_type == "postgresql"


class TestDataExceptions:
    """Test data-related exception classes."""
    
    def test_data_extraction_error(self):
        """Test DataExtractionError."""
        exc = DataExtractionError(
            message="Extraction failed",
            table_name="patient",
            extraction_strategy="incremental",
            batch_size=1000
        )
        
        assert exc.message == "Extraction failed"
        assert exc.table_name == "patient"
        assert exc.operation == "data_extraction"
        assert exc.extraction_strategy == "incremental"
        assert exc.batch_size == 1000
    
    def test_data_transformation_error(self):
        """Test DataTransformationError."""
        exc = DataTransformationError(
            message="Transformation failed",
            table_name="patient",
            transformation_type="date_format",
            field_name="DateTStamp"
        )
        
        assert exc.message == "Transformation failed"
        assert exc.table_name == "patient"
        assert exc.operation == "data_transformation"
        assert exc.transformation_type == "date_format"
        assert exc.field_name == "DateTStamp"
    
    def test_data_loading_error(self):
        """Test DataLoadingError."""
        exc = DataLoadingError(
            message="Loading failed",
            table_name="patient",
            loading_strategy="chunked",
            chunk_size=5000,
            target_schema="raw"
        )
        
        assert exc.message == "Loading failed"
        assert exc.table_name == "patient"
        assert exc.operation == "data_loading"
        assert exc.loading_strategy == "chunked"
        assert exc.chunk_size == 5000
        assert exc.target_schema == "raw"


class TestConfigurationExceptions:
    """Test configuration-related exception classes."""
    
    def test_configuration_error(self):
        """Test ConfigurationError."""
        missing_keys = ["database_host", "database_port"]
        invalid_values = {"batch_size": "invalid"}
        exc = ConfigurationError(
            message="Configuration validation failed",
            config_file="config.yml",
            missing_keys=missing_keys,
            invalid_values=invalid_values
        )
        
        assert exc.message == "Configuration validation failed"
        assert exc.operation == "configuration_validation"
        assert exc.config_file == "config.yml"
        assert exc.missing_keys == missing_keys
        assert exc.invalid_values == invalid_values
    
    def test_environment_error(self):
        """Test EnvironmentError."""
        missing_variables = ["ETL_ENVIRONMENT", "DATABASE_URL"]
        exc = EnvironmentError(
            message="Environment setup failed",
            environment="production",
            missing_variables=missing_variables,
            env_file=".env_production"
        )
        
        assert exc.message == "Environment setup failed"
        assert exc.operation == "environment_validation"
        assert exc.environment == "production"
        assert exc.missing_variables == missing_variables
        assert exc.env_file == ".env_production"


class TestExceptionInheritance:
    """Test that all exceptions properly inherit from ETLException."""
    
    @pytest.mark.parametrize("exception_class", [
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
    ])
    def test_exception_inheritance(self, exception_class):
        """Test that all exceptions inherit from ETLException."""
        exc = exception_class("Test message")
        assert isinstance(exc, ETLException)
        assert isinstance(exc, Exception) 