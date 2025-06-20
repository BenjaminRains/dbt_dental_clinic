"""
Base transformer class that defines the interface for all transformers.

STATUS: OVER-ENGINEERED - Single Implementation Interface
========================================================

This module defines an abstract base class for data transformers that is
significantly over-engineered for the current usage. It defines 15+ abstract
methods but is only used by a single concrete implementation.

CURRENT STATE:
- ✅ ABSTRACT INTERFACE: Well-defined interface for transformers
- ✅ COMPREHENSIVE METHODS: 15+ abstract methods covering all aspects
- ✅ TYPE HINTS: Proper type annotations and documentation
- ✅ SINGLE IMPLEMENTATION: Used by RawToPublicTransformer
- ❌ OVER-ENGINEERED: Too many methods for single implementation
- ❌ UNUSED METHODS: Many methods not implemented or used
- ❌ COMPLEXITY: Unnecessary abstraction for simple use case
- ❌ MAINTENANCE BURDEN: High maintenance cost for minimal benefit

ACTIVE USAGE:
- RawToPublicTransformer: Only concrete implementation
- TableProcessor: Uses RawToPublicTransformer for schema transformation
- PipelineOrchestrator: Initializes RawToPublicTransformer
- Tests: Referenced in transformer tests

ABSTRACT METHODS DEFINED:
1. transform_table: Core transformation logic
2. get_last_transformed: Transformation timestamp tracking
3. update_transform_status: Status management
4. verify_transform: Transformation verification
5. get_table_schema: Schema information retrieval
6. has_schema_changed: Schema change detection
7. get_table_metadata: Table metadata
8. get_table_row_count: Row counting
9. get_table_size: Table size calculation
10. get_table_indexes: Index information
11. get_table_constraints: Constraint information
12. get_table_foreign_keys: Foreign key information
13. get_table_columns: Column information
14. get_table_primary_key: Primary key information
15. get_table_partitions: Partition information
16. get_table_grants: Grant information
17. get_table_triggers: Trigger information
18. get_table_views: View information
19. get_table_dependencies: Dependency information

IMPLEMENTATION STATUS:
- RawToPublicTransformer: Implements all methods but many are basic/placeholder
- No other transformer implementations exist
- Many methods return basic/default values
- Complex methods like schema change detection are simplified

OVER-ENGINEERING ANALYSIS:
1. TOO MANY METHODS: 19 abstract methods for a single implementation
2. UNUSED FUNCTIONALITY: Many methods not actually used in ETL pipeline
3. COMPLEXITY: Unnecessary abstraction layer adds complexity
4. MAINTENANCE: High maintenance burden for minimal benefit
5. FLEXIBILITY: Designed for multiple implementations that don't exist

DEVELOPMENT RECOMMENDATIONS:
1. SIMPLIFY: Reduce to essential methods only (transform_table, verify_transform)
2. REMOVE ABSTRACTION: Convert to concrete class or simple interface
3. CONSOLIDATE: Merge with RawToPublicTransformer if no other implementations planned
4. FOCUS: Keep only methods actually used in the ETL pipeline
5. DOCUMENT: Clearly document which methods are essential vs optional

ALTERNATIVE APPROACHES:
1. SIMPLE INTERFACE: Define only core transformation methods
2. CONCRETE CLASS: Convert to concrete class with default implementations
3. MERGE: Integrate directly into RawToPublicTransformer
4. MINIMAL ABSTRACTION: Keep only essential abstract methods

This base class represents over-engineering for a single implementation
and should be simplified to reduce complexity and maintenance burden.
"""
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any
from datetime import datetime
from sqlalchemy.engine import Engine

class BaseTransformer(ABC):
    """
    Abstract base class for all data transformers.
    
    OVER-ENGINEERING WARNING: This class defines 19 abstract methods but is only
    used by a single concrete implementation (RawToPublicTransformer). This represents
    significant over-engineering for the current use case.
    
    Many of these methods are:
    - Not actually used in the ETL pipeline
    - Implemented with basic/placeholder logic
    - Unnecessary for the single implementation
    - Adding complexity without benefit
    
    Consider simplifying to only essential methods:
    - transform_table: Core transformation logic
    - verify_transform: Basic verification
    - get_last_transformed: Timestamp tracking (if needed)
    
    Or convert to a concrete class with default implementations
    to reduce the maintenance burden and complexity.
    """
    
    def __init__(self, source_engine: Engine, target_engine: Engine):
        self.source_engine = source_engine
        self.target_engine = target_engine
    
    @abstractmethod
    def transform_table(self, table_name: str, force_full: bool = False) -> bool:
        """
        Transform data from source to target.
        
        Args:
            table_name: Name of the table to transform
            force_full: Whether to force a full transformation
            
        Returns:
            bool: True if transformation was successful
        """
        pass
    
    @abstractmethod
    def get_last_transformed(self, table_name: str) -> Optional[datetime]:
        """
        Get the last transformation timestamp for a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Optional[datetime]: Last transformation timestamp, or None if not found
        """
        pass
    
    @abstractmethod
    def update_transform_status(self, table_name: str, rows_transformed: int, 
                              status: str = 'success') -> None:
        """
        Update the transformation status for a table.
        
        Args:
            table_name: Name of the table
            rows_transformed: Number of rows transformed
            status: Transformation status ('success', 'failed', etc.)
        """
        pass
    
    @abstractmethod
    def verify_transform(self, table_name: str) -> bool:
        """
        Verify that the transformation was successful.
        
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
        
        UNUSED METHOD: This method is defined in the interface but not actually
        used in the ETL pipeline. It's part of the over-engineering that adds
        complexity without providing value.
        
        Consider removing this method if it's not needed for the current
        transformation requirements.
        
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
        
        UNUSED METHOD: This method is defined in the interface but not actually
        used in the ETL pipeline. It's part of the over-engineering that adds
        complexity without providing value.
        
        Consider removing this method if it's not needed for the current
        transformation requirements.
        
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
        
        UNUSED METHOD: This method is defined in the interface but not actually
        used in the ETL pipeline. It's part of the over-engineering that adds
        complexity without providing value.
        
        Consider removing this method if it's not needed for the current
        transformation requirements.
        
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
        
        UNUSED METHOD: This method is defined in the interface but not actually
        used in the ETL pipeline. It's part of the over-engineering that adds
        complexity without providing value.
        
        Consider removing this method if it's not needed for the current
        transformation requirements.
        
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
        
        UNUSED METHOD: This method is defined in the interface but not actually
        used in the ETL pipeline. It's part of the over-engineering that adds
        complexity without providing value.
        
        Consider removing this method if it's not needed for the current
        transformation requirements.
        
        Args:
            table_name: Name of the table
            
        Returns:
            List[Dict[str, Any]]: List of dependency information
        """
        pass 