"""
Integration tests for SimpleMySQLReplicator using real test databases.

This package contains integration tests for the SimpleMySQLReplicator that:
- Use real test databases with test environment configuration
- Test actual database replication functionality
- Validate Settings injection and FileConfigProvider usage
- Test tracking table validation and primary column support
- Use real database connections for comprehensive testing

Test Organization:
- test_initialization_integration.py: Replicator initialization and configuration
- test_tracking_tables_integration.py: Tracking table validation and management
- test_copy_operations_integration.py: Table copy operations and strategies
- test_incremental_logic_integration.py: Incremental copy logic and primary columns
- test_error_handling_integration.py: Error handling and failure scenarios
- test_performance_integration.py: Performance and batch processing tests
- test_data_integrity_integration.py: Data integrity and validation tests

Integration Test Strategy:
- Uses FileConfigProvider for real configuration loading from .env_test
- Uses Settings injection for environment-agnostic connections
- Tests real database replication with test environment
- Validates tracking table infrastructure and primary column support
- Tests comprehensive error handling with real connections
- Uses order markers for proper integration test execution

ETL Context:
- Critical for nightly ETL pipeline execution with dental clinic data
- Supports MariaDB v11.6 source and MySQL replication database
- Uses provider pattern for clean dependency injection and test isolation
- Implements Settings injection for environment-agnostic connections
- Enforces FAIL FAST security to prevent accidental production usage
- Optimized for dental clinic data volumes and processing patterns
"""

from .test_initialization_integration import *
from .test_tracking_tables_integration import *
from .test_copy_operations_integration import *
from .test_incremental_logic_integration import *
from .test_error_handling_integration import *
from .test_performance_integration import *
from .test_data_integrity_integration import * 