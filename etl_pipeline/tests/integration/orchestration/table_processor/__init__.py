"""
Integration tests for TableProcessor using real test databases.

This package contains integration tests for the TableProcessor that:
- Use real test databases with test environment configuration
- Test actual ETL pipeline functionality (extract + load)
- Validate Settings injection and FileConfigProvider usage
- Test complete ETL pipeline with real data flow
- Use real database connections for comprehensive testing
- Test both incremental and full refresh processing modes
- Validate performance monitoring and metrics collection

Test Organization:
- test_initialization_integration.py: TableProcessor initialization and configuration
- test_processing_context_integration.py: TableProcessingContext and strategy resolution
- test_extract_operations_integration.py: Extract phase operations and SimpleMySQLReplicator integration
- test_load_operations_integration.py: Load phase operations and PostgresLoader integration
- test_incremental_logic_integration.py: Incremental processing logic and primary columns
- test_full_refresh_integration.py: Full refresh processing and data replacement
- test_error_handling_integration.py: Error handling and failure scenarios
- test_performance_integration.py: Performance monitoring and metrics collection
- test_data_integrity_integration.py: Data integrity and validation tests
- test_chunked_loading_integration.py: Chunked loading for large tables

Integration Test Strategy:
- Uses FileConfigProvider for real configuration loading from .env_test
- Uses Settings injection for environment-agnostic connections
- Tests real ETL pipeline with test environment (extract + load)
- Validates complete data flow from source to analytics
- Tests comprehensive error handling with real connections
- Uses order markers for proper integration test execution
- Tests both incremental and full refresh processing modes

ETL Context:
- Critical for nightly ETL pipeline execution with dental clinic data
- Supports MariaDB v11.6 source, MySQL replication, and PostgreSQL analytics
- Uses provider pattern for clean dependency injection and test isolation
- Implements Settings injection for environment-agnostic connections
- Enforces FAIL FAST security to prevent accidental production usage
- Optimized for dental clinic data volumes and processing patterns
- Coordinates SimpleMySQLReplicator and PostgresLoader for complete ETL flow
"""

from .test_initialization_integration import *
from .test_processing_context_integration import *
from .test_extract_operations_integration import *
from .test_load_operations_integration import *
from .test_complete_etl_pipeline_integration import * 