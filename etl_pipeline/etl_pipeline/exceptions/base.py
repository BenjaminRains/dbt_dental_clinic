"""
Base ETL Exception Classes

This module provides the base exception class for the ETL pipeline.
All custom exceptions inherit from ETLException to ensure consistent
error handling and context preservation.
"""

from typing import Optional, Dict, Any


class ETLException(Exception):
    """
    Base exception for ETL pipeline errors.
    
    This class provides structured error handling with context preservation,
    enabling better error categorization, debugging, and recovery strategies.
    
    Attributes:
        message: Human-readable error message
        table_name: Name of the table involved in the error (if applicable)
        operation: Name of the operation that failed (if applicable)
        details: Additional error details as a dictionary
        original_exception: The original exception that caused this error (if applicable)
    """
    
    def __init__(
        self, 
        message: str, 
        table_name: Optional[str] = None,
        operation: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        original_exception: Optional[Exception] = None
    ):
        """
        Initialize ETLException.
        
        Args:
            message: Human-readable error message
            table_name: Name of the table involved in the error
            operation: Name of the operation that failed
            details: Additional error details as a dictionary
            original_exception: The original exception that caused this error
        """
        self.message = message
        self.table_name = table_name
        self.operation = operation
        self.details = details or {}
        self.original_exception = original_exception
        
        # Build the full error message
        full_message = self._build_error_message()
        super().__init__(full_message)
    
    def _build_error_message(self) -> str:
        """Build a comprehensive error message with context."""
        parts = [self.message]
        
        if self.table_name:
            parts.append(f"Table: {self.table_name}")
        
        if self.operation:
            parts.append(f"Operation: {self.operation}")
        
        if self.details:
            detail_str = ", ".join([f"{k}={v}" for k, v in self.details.items()])
            parts.append(f"Details: {detail_str}")
        
        if self.original_exception:
            parts.append(f"Original error: {str(self.original_exception)}")
        
        return " | ".join(parts)
    
    def __str__(self) -> str:
        """Return the error message."""
        return self._build_error_message()
    
    def __repr__(self) -> str:
        """Return a detailed representation of the exception."""
        return (
            f"{self.__class__.__name__}("
            f"message='{self.message}', "
            f"table_name='{self.table_name}', "
            f"operation='{self.operation}', "
            f"details={self.details})"
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert exception to dictionary for serialization.
        
        Returns:
            Dictionary representation of the exception
        """
        return {
            'exception_type': self.__class__.__name__,
            'message': self.message,
            'table_name': self.table_name,
            'operation': self.operation,
            'details': self.details,
            'original_exception': str(self.original_exception) if self.original_exception else None
        } 