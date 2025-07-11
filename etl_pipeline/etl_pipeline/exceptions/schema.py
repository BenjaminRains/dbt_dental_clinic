"""
Schema-Related Exception Classes

This module provides custom exceptions for schema-related operations in the ETL pipeline,
including schema transformation, validation, and type conversion errors.
"""

from typing import Optional, Dict, Any
from .base import ETLException


class SchemaTransformationError(ETLException):
    """
    Raised when MySQL to PostgreSQL schema transformation fails.
    
    This exception is raised when there are issues converting MySQL schema
    definitions to PostgreSQL-compatible schema definitions.
    """
    
    def __init__(
        self,
        message: str,
        table_name: Optional[str] = None,
        mysql_schema: Optional[Dict[str, Any]] = None,
        details: Optional[Dict[str, Any]] = None,
        original_exception: Optional[Exception] = None
    ):
        """
        Initialize SchemaTransformationError.
        
        Args:
            message: Human-readable error message
            table_name: Name of the table being transformed
            mysql_schema: The MySQL schema that failed to transform
            details: Additional error details
            original_exception: The original exception that caused this error
        """
        super().__init__(
            message=message,
            table_name=table_name,
            operation="schema_transformation",
            details=details,
            original_exception=original_exception
        )
        self.mysql_schema = mysql_schema


class SchemaValidationError(ETLException):
    """
    Raised when schema validation fails.
    
    This exception is raised when there are issues validating that the
    PostgreSQL schema matches the expected MySQL schema.
    """
    
    def __init__(
        self,
        message: str,
        table_name: Optional[str] = None,
        validation_details: Optional[Dict[str, Any]] = None,
        details: Optional[Dict[str, Any]] = None,
        original_exception: Optional[Exception] = None
    ):
        """
        Initialize SchemaValidationError.
        
        Args:
            message: Human-readable error message
            table_name: Name of the table being validated
            validation_details: Details about the validation failure
            details: Additional error details
            original_exception: The original exception that caused this error
        """
        super().__init__(
            message=message,
            table_name=table_name,
            operation="schema_validation",
            details=details,
            original_exception=original_exception
        )
        self.validation_details = validation_details or {}


class TypeConversionError(ETLException):
    """
    Raised when MySQL type conversion to PostgreSQL fails.
    
    This exception is raised when there are issues converting MySQL data types
    to PostgreSQL-compatible data types.
    """
    
    def __init__(
        self,
        message: str,
        table_name: Optional[str] = None,
        column_name: Optional[str] = None,
        mysql_type: Optional[str] = None,
        postgres_type: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        original_exception: Optional[Exception] = None
    ):
        """
        Initialize TypeConversionError.
        
        Args:
            message: Human-readable error message
            table_name: Name of the table containing the column
            column_name: Name of the column with type conversion issues
            mysql_type: The MySQL data type that failed to convert
            postgres_type: The target PostgreSQL data type
            details: Additional error details
            original_exception: The original exception that caused this error
        """
        conversion_details = {
            'column_name': column_name,
            'mysql_type': mysql_type,
            'postgres_type': postgres_type
        }
        if details:
            conversion_details.update(details)
        
        super().__init__(
            message=message,
            table_name=table_name,
            operation="type_conversion",
            details=conversion_details,
            original_exception=original_exception
        )
        self.column_name = column_name
        self.mysql_type = mysql_type
        self.postgres_type = postgres_type 