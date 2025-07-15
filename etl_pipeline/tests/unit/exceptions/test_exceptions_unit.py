"""
Unit Tests for Custom Exception Classes

This module tests the custom exception classes to ensure they provide
proper error handling, context preservation, and serialization for the
dental clinic ETL pipeline.

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
    """Test the base ETLException class for dental clinic ETL pipeline."""
    
    def test_basic_exception_creation(self):
        """
        Test basic exception creation with minimal parameters.
        
        AAA Pattern:
            Arrange: Create exception with minimal parameters
            Act: Access exception properties and methods
            Assert: Verify all properties are correctly set
            
        ETL Context:
            - Used when basic error occurs without specific context
            - Common in dental clinic ETL pipeline error handling
            - Supports provider pattern error scenarios
        """
        # Arrange: Create exception with minimal parameters
        exc = ETLException("Test error message")
        
        # Act: Access exception properties and methods
        message = exc.message
        table_name = exc.table_name
        operation = exc.operation
        details = exc.details
        original_exception = exc.original_exception
        string_representation = str(exc)
        
        # Assert: Verify all properties are correctly set
        assert message == "Test error message"
        assert table_name is None
        assert operation is None
        assert details == {}
        assert original_exception is None
        assert string_representation == "Test error message"
    
    def test_exception_with_dental_clinic_context(self):
        """
        Test exception creation with full dental clinic context.
        
        AAA Pattern:
            Arrange: Create exception with dental clinic specific context
            Act: Access exception properties and methods
            Assert: Verify all properties are correctly set with dental clinic context
            
        ETL Context:
            - Used when dental clinic specific errors occur
            - Includes table names from OpenDental schema
            - Contains operation context for ETL pipeline debugging
            - Supports provider pattern with dental clinic configuration
        """
        # Arrange: Create exception with dental clinic specific context
        original_exc = ValueError("Original error")
        exc = ETLException(
            message="Database connection failed for dental clinic",
            table_name="patient",
            operation="data_extraction",
            details={"host": "dental-clinic-server.com", "port": 3306},
            original_exception=original_exc
        )
        
        # Act: Access exception properties and methods
        message = exc.message
        table_name = exc.table_name
        operation = exc.operation
        details = exc.details
        original_exception = exc.original_exception
        string_representation = str(exc)
        
        # Assert: Verify all properties are correctly set with dental clinic context
        assert message == "Database connection failed for dental clinic"
        assert table_name == "patient"
        assert operation == "data_extraction"
        assert details == {"host": "dental-clinic-server.com", "port": 3306}
        assert original_exception == original_exc
        
        # Test string representation with dental clinic context
        expected = "Database connection failed for dental clinic | Table: patient | Operation: data_extraction | Details: host=dental-clinic-server.com, port=3306 | Original error: Original error"
        assert string_representation == expected
    
    def test_exception_to_dict_serialization(self):
        """
        Test exception serialization to dictionary for logging and monitoring.
        
        AAA Pattern:
            Arrange: Create exception with dental clinic context
            Act: Call to_dict() method for serialization
            Assert: Verify dictionary contains all expected fields
            
        ETL Context:
            - Used for logging and monitoring in dental clinic ETL pipeline
            - Supports provider pattern configuration error reporting
            - Enables structured error reporting for dental clinic operations
        """
        # Arrange: Create exception with dental clinic context
        exc = ETLException(
            message="Patient table extraction failed",
            table_name="patient",
            operation="data_extraction",
            details={"batch_size": 1000, "extraction_strategy": "incremental"}
        )
        
        # Act: Call to_dict() method for serialization
        result = exc.to_dict()
        
        # Assert: Verify dictionary contains all expected fields
        assert result["exception_type"] == "ETLException"
        assert result["message"] == "Patient table extraction failed"
        assert result["table_name"] == "patient"
        assert result["operation"] == "data_extraction"
        assert result["details"] == {"batch_size": 1000, "extraction_strategy": "incremental"}
        assert result["original_exception"] is None
    
    def test_exception_repr_representation(self):
        """
        Test exception representation for debugging and development.
        
        AAA Pattern:
            Arrange: Create exception with dental clinic context
            Act: Call repr() method for representation
            Assert: Verify representation contains all expected information
            
        ETL Context:
            - Used for debugging dental clinic ETL pipeline issues
            - Supports development and troubleshooting
            - Enables clear error identification in dental clinic operations
        """
        # Arrange: Create exception with dental clinic context
        exc = ETLException(
            message="Appointment table loading failed",
            table_name="appointment",
            operation="data_loading",
            details={"target_schema": "raw", "loading_strategy": "chunked"}
        )
        
        # Act: Call repr() method for representation
        representation = repr(exc)
        
        # Assert: Verify representation contains all expected information
        expected = "ETLException(message='Appointment table loading failed', table_name='appointment', operation='data_loading', details={'target_schema': 'raw', 'loading_strategy': 'chunked'})"
        assert representation == expected


class TestSchemaExceptions:
    """Test schema-related exception classes for MySQL → PostgreSQL conversion."""
    
    def test_schema_transformation_error_with_dental_clinic_schema(self):
        """
        Test SchemaTransformationError with dental clinic specific schema.
        
        AAA Pattern:
            Arrange: Create schema transformation error with dental clinic schema
            Act: Access exception properties and methods
            Assert: Verify all properties are correctly set for dental clinic context
            
        ETL Context:
            - Used when MySQL schema cannot be converted to PostgreSQL
            - Critical for dental clinic data migration reliability
            - Supports provider pattern schema validation scenarios
        """
        # Arrange: Create schema transformation error with dental clinic schema
        mysql_schema = {
            "create_statement": "CREATE TABLE patient (PatNum INT PRIMARY KEY, LName VARCHAR(50))",
            "table_name": "patient"
        }
        exc = SchemaTransformationError(
            message="Failed to transform patient table schema",
            table_name="patient",
            mysql_schema=mysql_schema,
            details={"reason": "Unsupported MySQL type", "column": "IsActive"}
        )
        
        # Act: Access exception properties and methods
        message = exc.message
        table_name = exc.table_name
        operation = exc.operation
        mysql_schema_result = exc.mysql_schema
        details = exc.details
        
        # Assert: Verify all properties are correctly set for dental clinic context
        assert message == "Failed to transform patient table schema"
        assert table_name == "patient"
        assert operation == "schema_transformation"
        assert mysql_schema_result == mysql_schema
        assert details is not None
        assert details["reason"] == "Unsupported MySQL type"
        assert details["column"] == "IsActive"
    
    def test_schema_validation_error_with_dental_clinic_tables(self):
        """
        Test SchemaValidationError with dental clinic table validation.
        
        AAA Pattern:
            Arrange: Create schema validation error with dental clinic table context
            Act: Access exception properties and methods
            Assert: Verify all properties are correctly set for dental clinic validation
            
        ETL Context:
            - Used when dental clinic table schemas don't match expectations
            - Critical for data quality in dental clinic ETL pipeline
            - Supports provider pattern validation scenarios
        """
        # Arrange: Create schema validation error with dental clinic table context
        validation_details = {
            "column_count_mismatch": True,
            "expected_columns": ["PatNum", "LName", "FName"],
            "actual_columns": ["PatNum", "LName"]
        }
        exc = SchemaValidationError(
            message="Patient table schema validation failed",
            table_name="patient",
            validation_details=validation_details
        )
        
        # Act: Access exception properties and methods
        message = exc.message
        table_name = exc.table_name
        operation = exc.operation
        validation_details_result = exc.validation_details
        
        # Assert: Verify all properties are correctly set for dental clinic validation
        assert message == "Patient table schema validation failed"
        assert table_name == "patient"
        assert operation == "schema_validation"
        assert validation_details_result == validation_details
        assert validation_details_result["column_count_mismatch"] is True
    
    def test_type_conversion_error_with_dental_clinic_types(self):
        """
        Test TypeConversionError with dental clinic specific data types.
        
        AAA Pattern:
            Arrange: Create type conversion error with dental clinic data types
            Act: Access exception properties and methods
            Assert: Verify all properties are correctly set for dental clinic type conversion
            
        ETL Context:
            - Used when MySQL types cannot be converted to PostgreSQL
            - Critical for dental clinic data type compatibility
            - Supports provider pattern type conversion scenarios
        """
        # Arrange: Create type conversion error with dental clinic data types
        exc = TypeConversionError(
            message="Failed to convert patient IsActive field type",
            table_name="patient",
            column_name="IsActive",
            mysql_type="TINYINT",
            postgres_type="boolean"
        )
        
        # Act: Access exception properties and methods
        message = exc.message
        table_name = exc.table_name
        operation = exc.operation
        column_name = exc.column_name
        mysql_type = exc.mysql_type
        postgres_type = exc.postgres_type
        
        # Assert: Verify all properties are correctly set for dental clinic type conversion
        assert message == "Failed to convert patient IsActive field type"
        assert table_name == "patient"
        assert operation == "type_conversion"
        assert column_name == "IsActive"
        assert mysql_type == "TINYINT"
        assert postgres_type == "boolean"


class TestDatabaseExceptions:
    """Test database-related exception classes for dental clinic database operations."""
    
    def test_database_connection_error_with_dental_clinic_connection(self):
        """
        Test DatabaseConnectionError with dental clinic database connection.
        
        AAA Pattern:
            Arrange: Create database connection error with dental clinic connection params
            Act: Access exception properties and methods
            Assert: Verify all properties are correctly set for dental clinic connection
            
        ETL Context:
            - Used when dental clinic database connections fail
            - Critical for ETL pipeline reliability with dental clinic databases
            - Supports provider pattern connection scenarios
        """
        # Arrange: Create database connection error with dental clinic connection params
        connection_params = {
            "host": "dental-clinic-db.com",
            "port": 3306,
            "database": "opendental"
        }
        exc = DatabaseConnectionError(
            message="Connection timeout to dental clinic database",
            database_type="mysql",
            connection_params=connection_params
        )
        
        # Act: Access exception properties and methods
        message = exc.message
        operation = exc.operation
        database_type = exc.database_type
        connection_params_result = exc.connection_params
        
        # Assert: Verify all properties are correctly set for dental clinic connection
        assert message == "Connection timeout to dental clinic database"
        assert operation == "database_connection"
        assert database_type == "mysql"
        assert connection_params_result == connection_params
        assert connection_params_result is not None
        assert connection_params_result["host"] == "dental-clinic-db.com"
    
    def test_database_query_error_with_dental_clinic_query(self):
        """
        Test DatabaseQueryError with dental clinic specific query.
        
        AAA Pattern:
            Arrange: Create database query error with dental clinic query context
            Act: Access exception properties and methods
            Assert: Verify all properties are correctly set for dental clinic query
            
        ETL Context:
            - Used when dental clinic database queries fail
            - Critical for data extraction from dental clinic databases
            - Supports provider pattern query scenarios
        """
        # Arrange: Create database query error with dental clinic query context
        exc = DatabaseQueryError(
            message="Failed to query patient table from dental clinic database",
            table_name="patient",
            query="SELECT * FROM patient WHERE PatNum > 1000",
            database_type="mysql"
        )
        
        # Act: Access exception properties and methods
        message = exc.message
        table_name = exc.table_name
        operation = exc.operation
        query = exc.query
        database_type = exc.database_type
        
        # Assert: Verify all properties are correctly set for dental clinic query
        assert message == "Failed to query patient table from dental clinic database"
        assert table_name == "patient"
        assert operation == "database_query"
        assert query == "SELECT * FROM patient WHERE PatNum > 1000"
        assert database_type == "mysql"
    
    def test_database_transaction_error_with_dental_clinic_transaction(self):
        """
        Test DatabaseTransactionError with dental clinic transaction context.
        
        AAA Pattern:
            Arrange: Create database transaction error with dental clinic transaction context
            Act: Access exception properties and methods
            Assert: Verify all properties are correctly set for dental clinic transaction
            
        ETL Context:
            - Used when dental clinic database transactions fail
            - Critical for data consistency in dental clinic ETL pipeline
            - Supports provider pattern transaction scenarios
        """
        # Arrange: Create database transaction error with dental clinic transaction context
        exc = DatabaseTransactionError(
            message="Failed to commit appointment table transaction",
            table_name="appointment",
            transaction_type="commit",
            database_type="postgresql"
        )
        
        # Act: Access exception properties and methods
        message = exc.message
        table_name = exc.table_name
        operation = exc.operation
        transaction_type = exc.transaction_type
        database_type = exc.database_type
        
        # Assert: Verify all properties are correctly set for dental clinic transaction
        assert message == "Failed to commit appointment table transaction"
        assert table_name == "appointment"
        assert operation == "database_transaction"
        assert transaction_type == "commit"
        assert database_type == "postgresql"


class TestDataExceptions:
    """Test data-related exception classes for dental clinic data processing."""
    
    def test_data_extraction_error_with_dental_clinic_extraction(self):
        """
        Test DataExtractionError with dental clinic data extraction context.
        
        AAA Pattern:
            Arrange: Create data extraction error with dental clinic extraction context
            Act: Access exception properties and methods
            Assert: Verify all properties are correctly set for dental clinic extraction
            
        ETL Context:
            - Used when dental clinic data extraction fails
            - Critical for reliable data extraction from dental clinic databases
            - Supports provider pattern extraction scenarios
        """
        # Arrange: Create data extraction error with dental clinic extraction context
        exc = DataExtractionError(
            message="Failed to extract patient data from dental clinic database",
            table_name="patient",
            extraction_strategy="incremental",
            batch_size=1000
        )
        
        # Act: Access exception properties and methods
        message = exc.message
        table_name = exc.table_name
        operation = exc.operation
        extraction_strategy = exc.extraction_strategy
        batch_size = exc.batch_size
        
        # Assert: Verify all properties are correctly set for dental clinic extraction
        assert message == "Failed to extract patient data from dental clinic database"
        assert table_name == "patient"
        assert operation == "data_extraction"
        assert extraction_strategy == "incremental"
        assert batch_size == 1000
    
    def test_data_transformation_error_with_dental_clinic_transformation(self):
        """
        Test DataTransformationError with dental clinic data transformation context.
        
        AAA Pattern:
            Arrange: Create data transformation error with dental clinic transformation context
            Act: Access exception properties and methods
            Assert: Verify all properties are correctly set for dental clinic transformation
            
        ETL Context:
            - Used when dental clinic data transformation fails
            - Critical for data quality in dental clinic ETL pipeline
            - Supports provider pattern transformation scenarios
        """
        # Arrange: Create data transformation error with dental clinic transformation context
        exc = DataTransformationError(
            message="Failed to transform patient date format",
            table_name="patient",
            transformation_type="date_format",
            field_name="DateTStamp"
        )
        
        # Act: Access exception properties and methods
        message = exc.message
        table_name = exc.table_name
        operation = exc.operation
        transformation_type = exc.transformation_type
        field_name = exc.field_name
        
        # Assert: Verify all properties are correctly set for dental clinic transformation
        assert message == "Failed to transform patient date format"
        assert table_name == "patient"
        assert operation == "data_transformation"
        assert transformation_type == "date_format"
        assert field_name == "DateTStamp"
    
    def test_data_loading_error_with_dental_clinic_loading(self):
        """
        Test DataLoadingError with dental clinic data loading context.
        
        AAA Pattern:
            Arrange: Create data loading error with dental clinic loading context
            Act: Access exception properties and methods
            Assert: Verify all properties are correctly set for dental clinic loading
            
        ETL Context:
            - Used when dental clinic data loading fails
            - Critical for reliable data loading to analytics database
            - Supports provider pattern loading scenarios
        """
        # Arrange: Create data loading error with dental clinic loading context
        exc = DataLoadingError(
            message="Failed to load patient data to analytics database",
            table_name="patient",
            loading_strategy="chunked",
            chunk_size=5000,
            target_schema="raw"
        )
        
        # Act: Access exception properties and methods
        message = exc.message
        table_name = exc.table_name
        operation = exc.operation
        loading_strategy = exc.loading_strategy
        chunk_size = exc.chunk_size
        target_schema = exc.target_schema
        
        # Assert: Verify all properties are correctly set for dental clinic loading
        assert message == "Failed to load patient data to analytics database"
        assert table_name == "patient"
        assert operation == "data_loading"
        assert loading_strategy == "chunked"
        assert chunk_size == 5000
        assert target_schema == "raw"


class TestConfigurationExceptions:
    """Test configuration-related exception classes for provider pattern validation."""
    
    def test_configuration_error_with_dental_clinic_config(self):
        """
        Test ConfigurationError with dental clinic configuration context.
        
        AAA Pattern:
            Arrange: Create configuration error with dental clinic configuration context
            Act: Access exception properties and methods
            Assert: Verify all properties are correctly set for dental clinic configuration
            
        ETL Context:
            - Used when dental clinic configuration validation fails
            - Critical for provider pattern validation in dental clinic ETL
            - Supports FAIL FAST behavior for configuration errors
        """
        # Arrange: Create configuration error with dental clinic configuration context
        missing_keys = ["dental_clinic_host", "dental_clinic_port"]
        invalid_values = {"batch_size": "invalid"}
        exc = ConfigurationError(
            message="Dental clinic configuration validation failed",
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
        
        # Assert: Verify all properties are correctly set for dental clinic configuration
        assert message == "Dental clinic configuration validation failed"
        assert operation == "configuration_validation"
        assert config_file == "dental_clinic_config.yml"
        assert missing_keys_result == missing_keys
        assert invalid_values_result == invalid_values
    
    def test_environment_error_with_fail_fast_behavior(self):
        """
        Test EnvironmentError with FAIL FAST behavior for dental clinic ETL.
        
        AAA Pattern:
            Arrange: Create environment error with FAIL FAST context
            Act: Access exception properties and methods
            Assert: Verify all properties are correctly set for FAIL FAST behavior
            
        ETL Context:
            - Used when ETL_ENVIRONMENT is not set (FAIL FAST requirement)
            - Critical for dental clinic ETL pipeline security
            - Supports provider pattern environment validation
            - Enables FAIL FAST behavior to prevent production accidents
        """
        # Arrange: Create environment error with FAIL FAST context
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
        
        # Assert: Verify all properties are correctly set for FAIL FAST behavior
        assert message == "Dental clinic environment setup failed - ETL_ENVIRONMENT not set"
        assert operation == "environment_validation"
        assert environment == "production"
        assert missing_variables_result == missing_variables
        assert env_file == ".env_production"
        assert missing_variables_result is not None
        assert "ETL_ENVIRONMENT" in missing_variables_result


class TestExceptionInheritance:
    """Test that all exceptions properly inherit from ETLException for dental clinic ETL."""
    
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
    def test_exception_inheritance_for_dental_clinic_etl(self, exception_class):
        """
        Test that all exceptions inherit from ETLException for dental clinic ETL pipeline.
        
        AAA Pattern:
            Arrange: Create exception instance with dental clinic context
            Act: Check inheritance relationships
            Assert: Verify proper inheritance hierarchy
            
        ETL Context:
            - Ensures all exceptions work consistently in dental clinic ETL pipeline
            - Supports provider pattern error handling
            - Enables unified error handling across all ETL components
        """
        # Arrange: Create exception instance with dental clinic context
        exc = exception_class("Dental clinic ETL error")
        
        # Act: Check inheritance relationships
        is_etl_exception = isinstance(exc, ETLException)
        is_exception = isinstance(exc, Exception)
        
        # Assert: Verify proper inheritance hierarchy
        assert is_etl_exception is True
        assert is_exception is True 