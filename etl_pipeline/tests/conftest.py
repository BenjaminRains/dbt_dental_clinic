"""
Minimal conftest.py for ETL pipeline tests.

This file imports only the fixtures that are actually used in the test files
to make them available to all tests in the test suite.

Following the connection architecture with:
- Settings injection for environment-agnostic connections
- Provider pattern for dependency injection
- Environment separation with TEST_ prefixed variables
- FAIL FAST validation for ETL_ENVIRONMENT
"""

# Import essential fixtures from modular fixtures directory
from tests.fixtures.env_fixtures import (
    test_env_vars, 
    production_env_vars, 
    setup_test_environment, 
    reset_global_settings,
    test_env_provider,
    production_env_provider,
    comprehensive_test_isolation,
    mock_cleanup,
    load_test_environment_file
)

# Import integration fixtures for integration tests
from tests.fixtures.integration_fixtures import (
    test_data_manager,
    populated_test_databases,
    test_database_engines,
    test_source_engine,
    test_replication_engine,
    test_analytics_engine,
    test_raw_engine,
    test_staging_engine,
    test_intermediate_engine,
    test_marts_engine,
    setup_patient_table,
    setup_etl_tracking
)

# Import config fixtures for settings (using config_fixtures for Settings injection)
from tests.fixtures.config_fixtures import (
    test_pipeline_config,
    test_tables_config,
    test_config_provider,
    production_config_provider,
    valid_pipeline_config,
    minimal_pipeline_config,
    invalid_pipeline_config,
    complete_tables_config,
    mock_settings_environment,
    test_settings,
    production_settings,
    test_settings_with_enums
)

# Import connection fixtures for database connections (actually used)
from tests.fixtures.connection_fixtures import (
    database_types,
    postgres_schemas,
    mock_database_engines,
    mock_source_engine,
    mock_replication_engine,
    mock_analytics_engine,
    test_connection_config,
    production_connection_config,
    test_connection_provider,
    production_connection_provider,
    test_connection_settings,
    production_connection_settings,
    mock_connection_factory_with_settings,
    mock_connection_factory,
    mock_connection_factory_methods,
    mock_postgres_connection,
    mock_mysql_connection,
    mock_engine_with_connection,
    mock_connection_pool,
    mock_database_urls,
    mock_connection_config,
    mock_connection_error,
    mock_connection_timeout,
    connection_factory_test_cases,
    mock_connection_manager,
    connection_string_test_cases,
    pool_config_test_cases
)

# Import orchestrator fixtures for pipeline orchestration (actually used)
from tests.fixtures.orchestrator_fixtures import (
    test_orchestrator_settings,
    mock_components,
    orchestrator_with_settings,
    orchestrator,
    mock_orchestrator_config_with_settings,
    mock_orchestrator_config,
    mock_table_processing_result,
    mock_priority_processing_result,
    mock_priority_processing_result_with_failures,
    mock_schema_discovery_result,
    mock_workflow_steps,
    mock_workflow_execution,
    mock_workflow_execution_with_errors,
    mock_orchestrator_stats,
    mock_dependency_graph,
    mock_orchestrator_error,
    mock_workflow_scheduler,
    mock_workflow_monitor,
    connection_factory_with_orchestrator_settings,
    orchestrator_provider
)

# Import loader fixtures for data loading (actually used)
from tests.fixtures.loader_fixtures import (
    test_settings,
    postgres_loader,
    mock_replication_engine,
    mock_analytics_engine,
    mock_source_engine,
    sample_table_data,
    sample_mysql_schema,
    sample_postgres_schema,
    mock_loader_config,
    mock_loader_stats,
    sample_create_statement,
    sample_drop_statement,
    sample_insert_statement,
    sample_select_statement,
    mock_loader_error,
    mock_validation_error,
    database_configs_with_enums,
    schema_configs_with_enums,
    connection_factory_with_settings
)

# Import replicator fixtures for data replication (actually used)
from tests.fixtures.replicator_fixtures import (
    database_types,
    postgres_schemas,
    sample_mysql_replicator_table_data,
    sample_create_statement,
    mock_source_engine,
    mock_target_engine,
    mock_config_reader,
    test_replication_env_vars,
    test_replication_config_provider,
    test_replication_settings,
    replicator_with_settings,
    replicator,
    mock_replication_config,
    mock_replication_stats,
    sample_table_schemas,
    mock_replication_error,
    sample_replication_queries,
    mock_replication_validation,
    mock_replication_validation_with_errors,
    replication_test_cases
)

# Import postgres schema fixtures for schema management (actually used)
from tests.fixtures.postgres_schema_fixtures import (
    test_postgres_schema_settings,
    postgres_schema_test_settings,
    mock_postgres_schema_engines,
    mock_config_reader,
    sample_mysql_schemas,
    expected_postgres_schemas,
    postgres_schema_test_data,
    mock_postgres_schema_instance,
    real_postgres_schema_instance_with_settings,
    real_postgres_schema_instance,
    real_config_reader_instance,
    postgres_schema_test_tables,
    postgres_schema_error_cases,
    config_reader_with_settings,
    connection_factory_with_schema_settings,
    schema_configs_with_settings
)

# Import priority processor fixtures for priority-based processing (actually used)
from tests.fixtures.priority_processor_fixtures import (
    database_types,
    postgres_schemas,
    priority_processor_env_vars,
    priority_processor_pipeline_config,
    priority_processor_tables_config,
    priority_processor_config_provider,
    priority_processor_settings,
    mock_priority_processor_settings,
    mock_priority_processor_table_processor,
    priority_processor,
    sample_priority_queue,
    mock_priority_table_config,
    mock_priority_processor_stats,
    mock_priority_scheduler,
    mock_priority_validator,
    sample_priority_rules,
    mock_priority_monitor,
    mock_priority_error_handler,
    sample_priority_workflow,
    mock_priority_optimizer
)

# Import transformer fixtures for data transformation (actually used)
from tests.fixtures.transformer_fixtures import (
    database_types,
    postgres_schemas,
    test_transformer_env_vars,
    test_transformer_config_provider,
    test_transformer_settings,
    mock_table_processor_engines,
    table_processor_standard_config,
    table_processor_large_config,
    table_processor_medium_large_config,
    sample_transformation_data,
    mock_transformation_rules,
    mock_transformation_stats,
    mock_transformer_config,
    sample_validation_rules,
    mock_transformation_error,
    mock_validation_result,
    mock_validation_result_with_errors,
    transformer_test_cases
)

# Import CLI fixtures for CLI testing (actually used)
from tests.fixtures.cli_fixtures import (
    cli_runner,
    cli_test_config,
    cli_test_env_vars,
    cli_config_provider,
    cli_test_settings,
    cli_test_config_reader,
    cli_with_injected_config,
    temp_cli_config_file,
    temp_tables_config_file,
    mock_cli_database_connections,
    mock_cli_file_system,
    cli_expected_outputs,
    cli_error_cases,
    cli_performance_thresholds,
    cli_test_scenarios,
    cli_output_validators,
    cli_integration_test_data,
    cli_mock_orchestrator,
    cli_mock_metrics_collector
)

# Import metrics fixtures for monitoring tests (actually used)
from tests.fixtures.metrics_fixtures import (
    mock_unified_metrics_connection,
    unified_metrics_collector_no_persistence,
    mock_metrics_data,
    mock_performance_metrics,
    mock_pipeline_metrics,
    mock_database_metrics,
    mock_metrics_collector,
    mock_metrics_storage,
    mock_metrics_aggregator,
    mock_metrics_alert,
    mock_metrics_dashboard,
    sample_metrics_query,
    mock_metrics_error,
    metrics_collector_with_settings,
    mock_analytics_engine_for_metrics
)

# Import test data fixtures for standardized test data (actually used)
from tests.fixtures.test_data_fixtures import (
    standard_patient_test_data,
    incremental_patient_test_data,
    partial_patient_test_data,
    etl_tracking_test_data,
    invalid_schema_test_data,
    composite_pk_test_data,
    large_table_test_data,
    simple_test_table_data,
    patient_with_all_fields_test_data
)

# Import test data definitions for reusable test data (actually used)
from tests.fixtures.test_data_definitions import (
    get_test_patient_data,
    get_test_appointment_data,
    get_test_data_for_table,
    STANDARD_TEST_PATIENTS,
    STANDARD_TEST_APPOINTMENTS
) 