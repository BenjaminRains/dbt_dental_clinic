"""
Database-Related Exception Classes

This module provides custom exceptions for database-related operations in the ETL pipeline,
including connection, query, and transaction errors.
"""

from typing import Optional, Dict, Any
from .base import ETLException


class DatabaseConnectionError(ETLException):
    """
    Raised when database connection fails.
    
    This exception is raised when there are issues establishing or maintaining
    database connections for MySQL or PostgreSQL databases.
    """
    
    def __init__(
        self,
        message: str,
        database_type: Optional[str] = None,
        connection_params: Optional[Dict[str, Any]] = None,
        details: Optional[Dict[str, Any]] = None,
        original_exception: Optional[Exception] = None
    ):
        """
        Initialize DatabaseConnectionError.
        
        Args:
            message: Human-readable error message
            database_type: Type of database (mysql, postgresql)
            connection_params: Connection parameters that failed
            details: Additional error details
            original_exception: The original exception that caused this error
        """
        connection_details = {
            'database_type': database_type,
            'connection_params': connection_params
        }
        if details:
            connection_details.update(details)
        
        super().__init__(
            message=message,
            operation="database_connection",
            details=connection_details,
            original_exception=original_exception
        )
        self.database_type = database_type
        self.connection_params = connection_params


class DatabaseQueryError(ETLException):
    """
    Raised when database query execution fails.
    
    This exception is raised when there are issues executing SQL queries
    against MySQL or PostgreSQL databases.
    """
    
    def __init__(
        self,
        message: str,
        table_name: Optional[str] = None,
        query: Optional[str] = None,
        database_type: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        original_exception: Optional[Exception] = None
    ):
        """
        Initialize DatabaseQueryError.
        
        Args:
            message: Human-readable error message
            table_name: Name of the table involved in the query
            query: The SQL query that failed
            database_type: Type of database (mysql, postgresql)
            details: Additional error details
            original_exception: The original exception that caused this error
        """
        query_details = {
            'query': query,
            'database_type': database_type
        }
        if details:
            query_details.update(details)
        
        super().__init__(
            message=message,
            table_name=table_name,
            operation="database_query",
            details=query_details,
            original_exception=original_exception
        )
        self.query = query
        self.database_type = database_type


class DatabaseTransactionError(ETLException):
    """
    Raised when database transaction fails.
    
    This exception is raised when there are issues with database transactions,
    including commit, rollback, or transaction management operations.
    """
    
    def __init__(
        self,
        message: str,
        table_name: Optional[str] = None,
        transaction_type: Optional[str] = None,
        database_type: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        original_exception: Optional[Exception] = None
    ):
        """
        Initialize DatabaseTransactionError.
        
        Args:
            message: Human-readable error message
            table_name: Name of the table involved in the transaction
            transaction_type: Type of transaction (commit, rollback, begin)
            database_type: Type of database (mysql, postgresql)
            details: Additional error details
            original_exception: The original exception that caused this error
        """
        transaction_details = {
            'transaction_type': transaction_type,
            'database_type': database_type
        }
        if details:
            transaction_details.update(details)
        
        super().__init__(
            message=message,
            table_name=table_name,
            operation="database_transaction",
            details=transaction_details,
            original_exception=original_exception
        )
        self.transaction_type = transaction_type
        self.database_type = database_type 