"""
Base loader class that defines the interface for all loaders.
"""
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any
from datetime import datetime
from sqlalchemy.engine import Engine
from etl_pipeline.config import DatabaseConfig, PipelineConfig

class BaseLoader(ABC):
    """Abstract base class for all data loaders."""
    
    def __init__(self, source_engine: Engine, target_engine: Engine):
        self.source_engine = source_engine
        self.target_engine = target_engine
        self.config = PipelineConfig()
    
    @abstractmethod
    def load_table(self, table_name: str, force_full: bool = False) -> bool:
        """
        Load data from source to target.
        
        Args:
            table_name: Name of the table to load
            force_full: Whether to force a full load
            
        Returns:
            bool: True if load was successful
        """
        pass
    
    @abstractmethod
    def get_last_loaded(self, table_name: str) -> Optional[datetime]:
        """
        Get the last load timestamp for a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Optional[datetime]: Last load timestamp, or None if not found
        """
        pass
    
    @abstractmethod
    def update_load_status(self, table_name: str, rows_loaded: int, 
                          status: str = 'success') -> None:
        """
        Update the load status for a table.
        
        Args:
            table_name: Name of the table
            rows_loaded: Number of rows loaded
            status: Load status ('success', 'failed', etc.)
        """
        pass
    
    @abstractmethod
    def verify_load(self, table_name: str) -> bool:
        """
        Verify that the load was successful.
        
        Args:
            table_name: Name of the table
            
        Returns:
            bool: True if verification passed
        """
        pass
    
    @abstractmethod
    def get_table_schema(self, table_name: str) -> Dict[str, Any]:
        """
        Get the schema information for a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Dict[str, Any]: Schema information
        """
        pass
    
    @abstractmethod
    def has_schema_changed(self, table_name: str, stored_hash: str) -> bool:
        """
        Check if the table schema has changed.
        
        Args:
            table_name: Name of the table
            stored_hash: Previously stored schema hash
            
        Returns:
            bool: True if schema has changed
        """
        pass
    
    @abstractmethod
    def get_table_metadata(self, table_name: str) -> Dict[str, Any]:
        """
        Get metadata about a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Dict[str, Any]: Table metadata
        """
        pass
    
    @abstractmethod
    def get_table_row_count(self, table_name: str) -> int:
        """
        Get the number of rows in a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            int: Number of rows
        """
        pass
    
    @abstractmethod
    def get_table_size(self, table_name: str) -> int:
        """
        Get the size of a table in bytes.
        
        Args:
            table_name: Name of the table
            
        Returns:
            int: Table size in bytes
        """
        pass
    
    @abstractmethod
    def get_table_indexes(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Get the indexes for a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            List[Dict[str, Any]]: List of index information
        """
        pass
    
    @abstractmethod
    def get_table_constraints(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Get the constraints for a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            List[Dict[str, Any]]: List of constraint information
        """
        pass
    
    @abstractmethod
    def get_table_foreign_keys(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Get the foreign keys for a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            List[Dict[str, Any]]: List of foreign key information
        """
        pass
    
    @abstractmethod
    def get_table_columns(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Get the columns for a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            List[Dict[str, Any]]: List of column information
        """
        pass
    
    @abstractmethod
    def get_table_primary_key(self, table_name: str) -> Optional[List[str]]:
        """
        Get the primary key columns for a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Optional[List[str]]: List of primary key column names, or None if no primary key
        """
        pass
    
    @abstractmethod
    def get_table_partitions(self, table_name: str) -> Optional[List[Dict[str, Any]]]:
        """
        Get the partitions for a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Optional[List[Dict[str, Any]]]: List of partition information, or None if not partitioned
        """
        pass
    
    @abstractmethod
    def get_table_grants(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Get the grants for a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            List[Dict[str, Any]]: List of grant information
        """
        pass
    
    @abstractmethod
    def get_table_triggers(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Get the triggers for a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            List[Dict[str, Any]]: List of trigger information
        """
        pass
    
    @abstractmethod
    def get_table_views(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Get the views that reference a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            List[Dict[str, Any]]: List of view information
        """
        pass
    
    @abstractmethod
    def get_table_dependencies(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Get the dependencies for a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            List[Dict[str, Any]]: List of dependency information
        """
        pass 