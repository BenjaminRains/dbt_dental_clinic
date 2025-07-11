"""
Data-Related Exception Classes

This module provides custom exceptions for data-related operations in the ETL pipeline,
including extraction, transformation, and loading errors.
"""

from typing import Optional, Dict, Any
from .base import ETLException


class DataExtractionError(ETLException):
    """
    Raised when data extraction from source fails.
    
    This exception is raised when there are issues extracting data from
    the source MySQL database to the replication database.
    """
    
    def __init__(
        self,
        message: str,
        table_name: Optional[str] = None,
        extraction_strategy: Optional[str] = None,
        batch_size: Optional[int] = None,
        details: Optional[Dict[str, Any]] = None,
        original_exception: Optional[Exception] = None
    ):
        """
        Initialize DataExtractionError.
        
        Args:
            message: Human-readable error message
            table_name: Name of the table being extracted
            extraction_strategy: Strategy used for extraction (full, incremental)
            batch_size: Size of the extraction batch
            details: Additional error details
            original_exception: The original exception that caused this error
        """
        extraction_details = {
            'extraction_strategy': extraction_strategy,
            'batch_size': batch_size
        }
        if details:
            extraction_details.update(details)
        
        super().__init__(
            message=message,
            table_name=table_name,
            operation="data_extraction",
            details=extraction_details,
            original_exception=original_exception
        )
        self.extraction_strategy = extraction_strategy
        self.batch_size = batch_size


class DataTransformationError(ETLException):
    """
    Raised when data transformation fails.
    
    This exception is raised when there are issues transforming data
    between different formats or applying business rules.
    """
    
    def __init__(
        self,
        message: str,
        table_name: Optional[str] = None,
        transformation_type: Optional[str] = None,
        field_name: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        original_exception: Optional[Exception] = None
    ):
        """
        Initialize DataTransformationError.
        
        Args:
            message: Human-readable error message
            table_name: Name of the table being transformed
            transformation_type: Type of transformation applied
            field_name: Name of the field being transformed
            details: Additional error details
            original_exception: The original exception that caused this error
        """
        transformation_details = {
            'transformation_type': transformation_type,
            'field_name': field_name
        }
        if details:
            transformation_details.update(details)
        
        super().__init__(
            message=message,
            table_name=table_name,
            operation="data_transformation",
            details=transformation_details,
            original_exception=original_exception
        )
        self.transformation_type = transformation_type
        self.field_name = field_name


class DataLoadingError(ETLException):
    """
    Raised when data loading to target fails.
    
    This exception is raised when there are issues loading data from
    the replication database to the analytics PostgreSQL database.
    """
    
    def __init__(
        self,
        message: str,
        table_name: Optional[str] = None,
        loading_strategy: Optional[str] = None,
        chunk_size: Optional[int] = None,
        target_schema: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        original_exception: Optional[Exception] = None
    ):
        """
        Initialize DataLoadingError.
        
        Args:
            message: Human-readable error message
            table_name: Name of the table being loaded
            loading_strategy: Strategy used for loading (standard, chunked)
            chunk_size: Size of the loading chunks
            target_schema: Target schema for loading
            details: Additional error details
            original_exception: The original exception that caused this error
        """
        loading_details = {
            'loading_strategy': loading_strategy,
            'chunk_size': chunk_size,
            'target_schema': target_schema
        }
        if details:
            loading_details.update(details)
        
        super().__init__(
            message=message,
            table_name=table_name,
            operation="data_loading",
            details=loading_details,
            original_exception=original_exception
        )
        self.loading_strategy = loading_strategy
        self.chunk_size = chunk_size
        self.target_schema = target_schema 