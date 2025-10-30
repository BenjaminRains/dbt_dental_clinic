# ETL Pipeline Data Contracts

## Overview

This document describes the **Python objects** and **data contracts** that flow through the OpenDental ETL pipeline. While [PIPELINE_ARCHITECTURE.md](PIPELINE_ARCHITECTURE.md) explains the system architecture and component interactions, this document focuses on the **data structures** themselves—what they contain, how they're passed between components, and their lifecycle.

Understanding these data contracts is essential for:
- **Development**: Knowing what objects to create and consume
- **Debugging**: Understanding what data is available at each stage
- **Testing**: Creating mock objects with correct structure
- **Integration**: Connecting new components to the pipeline

## Table of Contents

1. [Core Configuration Objects](#core-configuration-objects)
2. [Table-Specific Data Objects](#table-specific-data-objects)
3. [Result & Metadata Objects](#result--metadata-objects)
4. [Orchestration Objects](#orchestration-objects)
5. [Airflow Integration Objects](#airflow-integration-objects)
6. [Object Lifecycle](#object-lifecycle)
7. [Design Patterns](#design-patterns)
8. [Type Contracts](#type-contracts)

---

## Core Configuration Objects

These objects manage configuration and are created once per pipeline run, then passed to all components.

### Settings Object

**Type**: `etl_pipeline.config.Settings`

**Purpose**: Master configuration manager providing environment-specific database configurations

**Contract**:
```python
class Settings:
    # Attributes
    environment: str                    # 'production' or 'test'
    provider: ConfigProvider            # FileConfigProvider or DictConfigProvider
    pipeline_config: Dict               # From pipeline.yml
    tables_config: Dict                 # From tables.yml
    _env_vars: Dict[str, str]          # Environment variables
    _connection_cache: Dict            # Cached connection configs
    
    # Methods (Key Public Interface)
    get_database_config(db_type: DatabaseType, schema: Optional[PostgresSchema]) -> Dict
    get_source_connection_config() -> Dict
    get_replication_connection_config() -> Dict
    get_analytics_connection_config(schema: PostgresSchema) -> Dict
    get_table_config(table_name: str) -> Dict
    list_tables() -> List[str]
    validate_configs() -> bool
```

**Example Usage**:
```python
from etl_pipeline.config import get_settings

# Get global settings instance
settings = get_settings()

# Access environment
env = settings.environment  # 'production'

# Get database configuration
source_config = settings.get_source_connection_config()
# Returns: {
#     'host': '192.168.1.100',
#     'port': 3306,
#     'database': 'opendental',
#     'user': 'etl_readonly',
#     'password': '***',
#     'connect_timeout': 10,
#     'charset': 'utf8mb4'
# }

# Get table configuration
patient_config = settings.get_table_config('patient')
# Returns: Table config dictionary (see below)
```

**Information Flow**:
```
Entry Point (CLI/Airflow DAG)
    ↓ calls get_settings()
Settings singleton created
    ↓ passed to
PipelineOrchestrator
    ↓ passed to
TableProcessor, PriorityProcessor
    ↓ passed to
SimpleMySQLReplicator, PostgresLoader
```

**Lifetime**: Created once at pipeline initialization, lives for entire run, shared across all components

**Why This Design**:
- ✅ Single source of truth for environment configuration
- ✅ Environment-agnostic (production/test handled automatically)
- ✅ Lazy initialization (connections created on demand)
- ✅ Cached connection configs (avoid repeated computation)
- ✅ Testable (can inject mock Settings)

---

### ConfigReader Object

**Type**: `etl_pipeline.config.ConfigReader`

**Purpose**: Reads and parses `tables.yml` configuration file

**Contract**:
```python
class ConfigReader:
    # Attributes
    config_path: str                    # Path to tables.yml
    _config: Optional[Dict]            # Cached YAML content
    
    # Methods (Key Public Interface)
    def __init__(config_path: Optional[str] = None)
    load_config() -> Dict
    get_table_config(table_name: str) -> Optional[Dict]
    get_all_tables() -> List[str]
    get_tables_by_category(category: str) -> List[str]
    get_metadata() -> Dict
```

**Example Usage**:
```python
from etl_pipeline.config import ConfigReader

# Create reader (auto-detects tables.yml location)
reader = ConfigReader()

# Load full configuration
config = reader.load_config()
# Returns: {
#     'metadata': {...},
#     'tables': {...}
# }

# Get specific table config
patient_config = reader.get_table_config('patient')

# Get all tables in a category
large_tables = reader.get_tables_by_category('large')
# Returns: ['appointment', 'procedurelog', ...]
```

**Information Flow**:
```
PipelineOrchestrator.__init__()
    ↓ creates ConfigReader()
    ↓ stores as self.config_reader
    ↓ passes to
TableProcessor.__init__(config_reader=reader)
PriorityProcessor.__init__(config_reader=reader)
    ↓ components call
reader.get_table_config('patient')
```

**Lifetime**: Created once during orchestrator initialization, reused for all table config lookups

**Why This Design**:
- ✅ Separation of concerns (reading vs. using configuration)
- ✅ Components don't know about YAML files
- ✅ Easy to mock for testing
- ✅ Caches parsed YAML (read once, use many times)

---

## Table-Specific Data Objects

These objects contain configuration and state for individual tables.

### Table Configuration Dictionary

**Type**: `Dict[str, Any]` (from `tables.yml`)

**Purpose**: Complete metadata for a single table

**Contract**:
```python
{
    # Identity
    'table_name': str,                          # 'patient'
    
    # Extraction Configuration
    'extraction_strategy': str,                 # 'full_table' | 'incremental' | 'incremental_chunked'
    'incremental_columns': List[str],           # ['PatNum', 'DateTStamp']
    'primary_incremental_column': str,          # 'DateTStamp'
    'incremental_strategy': str,                # 'single_column' | 'or_logic' | 'and_logic'
    
    # Performance Configuration
    'batch_size': int,                          # 50000
    'performance_category': str,                # 'tiny' | 'small' | 'medium' | 'large'
    'processing_priority': str,                 # 'high' | 'medium' | 'low'
    'time_gap_threshold_days': int,             # 30
    
    # Size Estimates
    'estimated_rows': int,                      # 50000
    'estimated_size_mb': float,                 # 125.5
    'estimated_processing_time_minutes': float, # 2.5
    'memory_requirements_mb': int,              # 256
    
    # Monitoring Configuration
    'monitoring': {
        'alert_on_failure': bool,               # True
        'alert_on_slow_extraction': bool,       # True
        'performance_threshold_records_per_second': int,  # 2000
        'memory_alert_threshold_mb': int        # 512
    },
    
    # Schema Information
    'primary_key': str,                         # 'PatNum'
    'schema_hash': int,                         # 7071511390257913187
    'last_analyzed': str,                       # '2025-10-19T19:21:16.939002'
    
    # dbt Integration
    'is_modeled': bool,                         # False
    'dbt_model_types': List[str]               # ['staging', 'mart']
}
```

**Example**:
```python
patient_config = {
    'table_name': 'patient',
    'extraction_strategy': 'incremental',
    'estimated_rows': 50000,
    'estimated_size_mb': 125.5,
    'batch_size': 50000,
    'incremental_columns': ['PatNum', 'DateTStamp'],
    'primary_incremental_column': 'DateTStamp',
    'incremental_strategy': 'or_logic',
    'performance_category': 'medium',
    'processing_priority': 'high',
    'time_gap_threshold_days': 30,
    'estimated_processing_time_minutes': 2.5,
    'memory_requirements_mb': 256,
    'monitoring': {
        'alert_on_failure': True,
        'alert_on_slow_extraction': True,
        'performance_threshold_records_per_second': 2000,
        'memory_alert_threshold_mb': 512
    },
    'primary_key': 'PatNum',
    'is_modeled': False,
    'dbt_model_types': []
}
```

**Information Flow**:
```
ConfigReader.get_table_config('patient')
    ↓ returns dict
TableProcessor.process_table('patient')
    ↓ reads relevant fields
    ↓ passes to
SimpleMySQLReplicator (uses batch_size, incremental_columns, strategy)
PostgresLoader (uses batch_size, estimated_size_mb)
```

**Lifetime**: Read on-demand for each table, used during table processing, discarded after

**Field Usage by Component**:

| Field | SimpleMySQLReplicator | PostgresLoader | TableProcessor | Monitoring |
|-------|----------------------|----------------|----------------|------------|
| `extraction_strategy` | ✅ | ❌ | ✅ | ❌ |
| `incremental_columns` | ✅ | ❌ | ✅ | ❌ |
| `batch_size` | ✅ | ✅ | ❌ | ❌ |
| `performance_category` | ✅ | ❌ | ✅ | ✅ |
| `estimated_size_mb` | ❌ | ✅ | ✅ | ❌ |
| `monitoring` | ❌ | ❌ | ❌ | ✅ |

---

### TableProcessingContext Object

**Type**: `etl_pipeline.orchestration.table_processor.TableProcessingContext`

**Purpose**: Unified context wrapping table configuration with processing logic

**Contract**:
```python
class TableProcessingContext:
    # Constructor
    def __init__(table_name: str, force_full: bool, config_reader: ConfigReader)
    
    # Attributes (All set during __init__)
    table_name: str                             # 'patient'
    force_full: bool                            # False
    config_reader: ConfigReader                 # Reference to reader
    config: Dict                                # Full table config dict
    
    # Extracted Configuration (for convenience)
    performance_category: str                   # 'medium'
    processing_priority: int                    # 1-10 (converted from 'high')
    estimated_size_mb: float                    # 125.5
    estimated_rows: int                         # 50000
    incremental_columns: List[str]              # ['PatNum', 'DateTStamp']
    primary_column: Optional[str]               # 'DateTStamp'
    
    # Resolved Strategy Information
    strategy_info: Dict[str, Any]               # See below
    
    # Methods
    get_processing_metadata() -> Dict
```

**Strategy Info Structure**:
```python
strategy_info = {
    'strategy': str,                    # 'incremental' | 'full_table'
    'force_full_applied': bool,         # True if forced to full table
    'reason': str,                      # Human-readable explanation
    'incremental_columns': List[str],   # Available incremental columns
    'extraction_strategy': str          # Original strategy from config
}
```

**Example**:
```python
from etl_pipeline.orchestration.table_processor import TableProcessingContext

# Create context
context = TableProcessingContext(
    table_name='patient',
    force_full=False,
    config_reader=config_reader
)

# Access attributes
print(context.table_name)                    # 'patient'
print(context.performance_category)          # 'medium'
print(context.processing_priority)           # 1 (high priority)
print(context.incremental_columns)           # ['PatNum', 'DateTStamp']
print(context.primary_column)                # 'DateTStamp'

# Access resolved strategy
print(context.strategy_info['strategy'])     # 'incremental'
print(context.strategy_info['force_full_applied'])  # False
print(context.strategy_info['reason'])       # 'incremental strategy with available incremental columns'

# Get full metadata
metadata = context.get_processing_metadata()
```

**Information Flow**:
```
TableProcessor.process_table('patient', force_full=False)
    ↓ creates
TableProcessingContext('patient', False, config_reader)
    ↓ used internally by
_extract_to_replication()
_load_to_analytics()
    ↓ avoids repeated config lookups
```

**Lifetime**: Created once per table at start of processing, lives for duration of table processing

**Why This Design**:
- ✅ DRY principle (config loaded once, not repeatedly)
- ✅ Strategy resolution logic centralized
- ✅ Type conversions done once (e.g., priority string → int)
- ✅ Consistent configuration access
- ✅ Validation happens at context creation

---

## Result & Metadata Objects

These objects carry results and metadata from processing operations.

### Extraction Metadata Dictionary

**Type**: `Dict[str, Any]` (returned from `SimpleMySQLReplicator.copy_table()`)

**Purpose**: Results and metrics from extraction phase

**Contract**:
```python
# Success case
{
    'rows_copied': int,                 # 1523
    'strategy_used': str,               # 'incremental' | 'full_table' | 'incremental_chunked'
    'duration': float,                  # 45.2 (seconds)
    'force_full_applied': bool,         # False
    'primary_column': Optional[str],    # 'DateTStamp'
    'last_primary_value': Any,          # '2025-10-22 15:30:00' or 12345
    'batch_size_used': int,             # 50000
    'performance_category': str,        # 'medium'
    'start_time': datetime,             # Start timestamp
    'end_time': datetime                # End timestamp
}

# Failure case
{
    'rows_copied': int,                 # 0
    'strategy_used': str,               # 'error'
    'duration': float,                  # 0.0
    'force_full_applied': bool,         # Original force_full value
    'primary_column': Optional[str],    # None
    'last_primary_value': Any,          # None
    'error': str                        # Error message
}
```

**Return Type**: `Tuple[bool, Dict]`
```python
success: bool                   # True if extraction succeeded
metadata: Dict                  # Metadata dictionary (structure above)
```

**Example**:
```python
from etl_pipeline.core.simple_mysql_replicator import SimpleMySQLReplicator

replicator = SimpleMySQLReplicator(settings=settings)
success, metadata = replicator.copy_table('patient', force_full=False)

if success:
    print(f"Copied {metadata['rows_copied']} rows")
    print(f"Strategy: {metadata['strategy_used']}")
    print(f"Duration: {metadata['duration']:.2f}s")
    print(f"Last value: {metadata['last_primary_value']}")
else:
    print(f"Extraction failed: {metadata.get('error', 'Unknown error')}")
```

**Information Flow**:
```
SimpleMySQLReplicator.copy_table('patient')
    ↓ performs extraction
    ↓ returns (True, metadata)
TableProcessor._extract_to_replication()
    ↓ receives (success, metadata)
    ↓ extracts fields:
        - rows_copied
        - strategy_used
        - last_primary_value
    ↓ passes to
_update_pipeline_status('extract', ...)
    ↓ updates etl_copy_status table
```

**Lifetime**: Created at end of extraction, used for tracking and logging, then discarded

---

### Load Metadata Dictionary

**Type**: `Dict[str, Any]` (returned from `PostgresLoader.load_table()`)

**Purpose**: Results and metrics from load phase

**Contract**:
```python
# Success case
{
    'rows_loaded': int,                 # 1523
    'strategy_used': str,               # 'full_load' | 'incremental' | 'chunked'
    'duration': float,                  # 22.8 (seconds)
    'last_primary_value': Any,          # '2025-10-22 15:30:00' or 12345
    'primary_column': Optional[str],    # 'DateTStamp'
    'schema_converted': bool,           # True
    'chunked_loading': bool,            # False
    'chunk_count': Optional[int],       # 5 (if chunked)
    'verification_passed': bool,        # True
    'start_time': datetime,             # Start timestamp
    'end_time': datetime                # End timestamp
}

# Failure case
{
    'rows_loaded': int,                 # 0
    'strategy_used': str,               # 'error'
    'duration': float,                  # 0.0
    'error': str                        # Error message
}
```

**Return Type**: `Tuple[bool, Dict]`
```python
success: bool                   # True if load succeeded
metadata: Dict                  # Metadata dictionary (structure above)
```

**Example**:
```python
from etl_pipeline.loaders.postgres_loader import PostgresLoader

loader = PostgresLoader(use_test_environment=False, settings=settings)
success, metadata = loader.load_table('patient', force_full=False)

if success:
    print(f"Loaded {metadata['rows_loaded']} rows")
    print(f"Strategy: {metadata['strategy_used']}")
    print(f"Chunked: {metadata['chunked_loading']}")
else:
    print(f"Load failed: {metadata.get('error', 'Unknown error')}")
```

**Information Flow**:
```
PostgresLoader.load_table('patient')
    ↓ performs load
    ↓ returns (True, metadata)
TableProcessor._load_to_analytics()
    ↓ receives (success, metadata)
    ↓ extracts fields:
        - rows_loaded
        - strategy_used
        - last_primary_value
    ↓ passes to
_update_pipeline_status('load', ...)
    ↓ updates etl_load_status table
```

**Lifetime**: Created at end of load, used for tracking and logging, then discarded

---

### Tracking Metadata Dictionary

**Type**: `Dict[str, Any]` (used for updating tracking tables)

**Purpose**: Consolidated metadata for `etl_copy_status` and `etl_load_status` tables

**Contract**:
```python
{
    'rows_processed': int,              # Number of rows processed
    'last_primary_value': Any,          # Last value of primary incremental column
    'primary_column': Optional[str],    # Name of primary incremental column
    'duration': float,                  # Processing duration in seconds
    'strategy_used': str                # Strategy that was used
}
```

**Example**:
```python
# Created from extraction metadata
tracking_metadata = {
    'rows_processed': extraction_metadata['rows_copied'],
    'last_primary_value': extraction_metadata['last_primary_value'],
    'primary_column': extraction_metadata['primary_column'],
    'duration': extraction_metadata['duration'],
    'strategy_used': extraction_metadata['strategy_used']
}

# Used to update tracking table
self._update_pipeline_status('extract', 'success', tracking_metadata)
```

**Information Flow**:
```
TableProcessor._extract_to_replication()
    ↓ receives extraction_metadata from SimpleMySQLReplicator
    ↓ creates tracking_metadata (normalized format)
    ↓ calls _update_pipeline_status('extract', 'success', tracking_metadata)
        ↓ updates opendental_replication.etl_copy_status

TableProcessor._load_to_analytics()
    ↓ receives load_metadata from PostgresLoader
    ↓ creates tracking_metadata (normalized format)
    ↓ calls _update_pipeline_status('load', 'success', tracking_metadata)
        ↓ updates opendental_analytics.raw.etl_load_status
```

**Why This Design**:
- ✅ Unified format for both MySQL and PostgreSQL tracking
- ✅ Only essential fields (no bloat)
- ✅ Consistent field names across systems

---

## Orchestration Objects

These objects coordinate table processing and aggregate results.

### Processing Results by Category

**Type**: `Dict[str, Dict[str, Any]]` (from `PriorityProcessor.process_by_priority()`)

**Purpose**: Results from processing tables grouped by performance category

**Contract**:
```python
{
    'large': {
        'success': List[str],           # ['appointment', 'procedurelog']
        'failed': List[str],            # ['claimproc']
        'total': int                    # 3
    },
    'medium': {
        'success': List[str],           # ['patient', 'provider']
        'failed': List[str],            # []
        'total': int                    # 2
    },
    'small': {
        'success': List[str],           # [...]
        'failed': List[str],            # [...]
        'total': int
    },
    'tiny': {
        'success': List[str],           # [...]
        'failed': List[str],            # [...]
        'total': int
    }
}
```

**Example**:
```python
from etl_pipeline.orchestration.priority_processor import PriorityProcessor

processor = PriorityProcessor(config_reader=reader)
results = processor.process_by_priority(
    importance_levels=None,
    max_workers=5,
    force_full=False
)

# Access results
large_results = results['large']
print(f"Large tables: {large_results['success']}/{large_results['total']} succeeded")
print(f"Failed: {large_results['failed']}")

# Aggregate across all categories
total_success = sum(r['success'].__len__() for r in results.values())
total_failed = sum(r['failed'].__len__() for r in results.values())
```

**Information Flow**:
```
PipelineOrchestrator.process_tables_by_priority()
    ↓ calls
PriorityProcessor.process_by_priority()
    ↓ processes each category
    ↓ builds results dict
    ↓ returns to
PipelineOrchestrator
    ↓ aggregates and reports
```

**Lifetime**: Built incrementally during processing, returned at end, used for reporting

---

### Table Processing Results per Category

**Type**: `Dict[str, bool]` (from `PipelineOrchestrator.process_tables_by_performance_category()`)

**Purpose**: Success/failure status for individual tables within a category

**Contract**:
```python
{
    'patient': bool,                    # True (success)
    'provider': bool,                   # True (success)
    'adjustment': bool,                 # False (failed)
    'payment': bool                     # True (success)
}
# table_name → success boolean
```

**Example**:
```python
orchestrator = PipelineOrchestrator(environment='production')
orchestrator.initialize_connections()

results = orchestrator.process_tables_by_performance_category(
    category='medium',
    max_workers=1,
    force_full=False
)

# Check results
for table_name, success in results.items():
    status = "✓" if success else "✗"
    print(f"{status} {table_name}")

# Calculate success rate
success_count = sum(1 for s in results.values() if s)
total_count = len(results)
success_rate = success_count / total_count * 100
print(f"Success rate: {success_rate:.1f}%")
```

**Information Flow**:
```
PipelineOrchestrator.process_tables_by_performance_category('medium')
    ↓ for each table in category
        ↓ calls TableProcessor.process_table(table)
        ↓ receives True/False
        ↓ stores in results dict
    ↓ returns results dict
```

**Why Simple Booleans**: At orchestration level, we only care about pass/fail. Detailed information is in logs and tracking tables.

---

## Airflow Integration Objects

These objects enable communication between Airflow tasks.

### XCom Data

**Type**: JSON-serializable Python primitives, dictionaries, and lists

**Purpose**: Inter-task communication in Airflow (tasks run in separate processes)

**Constraints**:
- Must be JSON-serializable
- No custom objects (only primitives, dicts, lists)
- No datetime objects (convert to ISO strings)
- Size limit: ~48KB (database-dependent)
- Stored in Airflow metadata database

**Common Patterns in Schema Analysis DAG**:

```python
# Task: backup_existing_config
context['task_instance'].xcom_push(key='backup_created', value=True)
context['task_instance'].xcom_push(key='backup_path', value='/path/to/backup')

# Task: analyze_schema_changes
context['task_instance'].xcom_push(key='has_changes', value=True)
context['task_instance'].xcom_push(key='has_breaking_changes', value=False)
context['task_instance'].xcom_push(key='change_details', value={
    'added_tables': ['new_table1', 'new_table2'],
    'removed_tables': [],
    'added_count': 2,
    'removed_count': 0,
    'change_summary': 'Schema changes detected:\n  Added: 2 tables\n  Removed: 0 tables'
})
```

**Common Patterns in ETL Pipeline DAG**:

```python
# Task: validate_configuration
context['task_instance'].xcom_push(key='config_age_days', value=15)
context['task_instance'].xcom_push(key='total_tables', value=436)
context['task_instance'].xcom_push(key='schema_hash', value='a07eb24347005056...')

# Task: process_large_tables
context['task_instance'].xcom_push(key='success_count', value=8)
context['task_instance'].xcom_push(key='failure_count', value=2)
context['task_instance'].xcom_push(key='total_count', value=10)
context['task_instance'].xcom_push(key='failed_tables', value=['table1', 'table2'])

# Task: generate_pipeline_report (complex nested structure)
context['task_instance'].xcom_push(key='pipeline_report', value={
    'execution_date': '2025-10-22T03:00:00',
    'dag_run_id': 'scheduled__2025-10-22T03:00:00',
    'environment': 'production',
    'configuration': {
        'total_tables_configured': 436,
        'config_age_days': 15,
        'schema_drift_detected': False
    },
    'processing_results': {
        'total_processed': 436,
        'total_success': 430,
        'total_failed': 6,
        'success_rate': '98.6%',
        'by_category': {
            'large': {'success': 8, 'failed': 2, 'total': 10},
            'medium': {'success': 45, 'failed': 3, 'total': 48},
            # ...
        }
    }
})
```

**Retrieving XCom Data**:

```python
# In a downstream task
ti = context['task_instance']

# Pull data from specific task
total_tables = ti.xcom_pull(task_ids='validate_configuration', key='total_tables')
# Returns: 436

failed_tables = ti.xcom_pull(task_ids='process_large_tables', key='failed_tables')
# Returns: ['table1', 'table2']

# Pull complex structure
report = ti.xcom_pull(task_ids='generate_pipeline_report', key='pipeline_report')
# Returns: Full dictionary

# Access nested fields
success_rate = report['processing_results']['success_rate']
```

**Information Flow**:
```
Task 1 (validate_configuration)
    ↓ xcom_push(key='total_tables', value=436)
    ↓ Serialized as JSON
    ↓ Stored in Airflow metastore (PostgreSQL/MySQL)
Task 2 (generate_pipeline_report) [runs later]
    ↓ xcom_pull(task_ids='validate_configuration', key='total_tables')
    ↓ Deserialized from JSON
    ↓ Returns: 436
```

**Best Practices**:
```python
# ✅ GOOD: Simple types
context['ti'].xcom_push(key='count', value=42)
context['ti'].xcom_push(key='name', value='patient')
context['ti'].xcom_push(key='tables', value=['t1', 't2', 't3'])

# ✅ GOOD: Dictionaries with simple types
context['ti'].xcom_push(key='results', value={
    'success': True,
    'count': 42,
    'tables': ['t1', 't2']
})

# ❌ BAD: Custom objects (not JSON-serializable)
context['ti'].xcom_push(key='settings', value=settings_object)

# ❌ BAD: Datetime objects (not JSON-serializable)
context['ti'].xcom_push(key='timestamp', value=datetime.now())

# ✅ GOOD: Convert datetime to string
context['ti'].xcom_push(key='timestamp', value=datetime.now().isoformat())

# ❌ BAD: Very large data (exceeds size limit)
context['ti'].xcom_push(key='all_rows', value=list_of_10000_records)

# ✅ GOOD: Summary instead of full data
context['ti'].xcom_push(key='row_count', value=len(list_of_10000_records))
```

---

## Object Lifecycle

Understanding when objects are created and destroyed helps with debugging and performance optimization.

### Pipeline Run Lifecycle

```
┌────────────────────────────────────────────────────────────────┐
│ PHASE 1: INITIALIZATION (Once per run)                         │
├────────────────────────────────────────────────────────────────┤
│ Objects Created:                                                │
│   • Settings (singleton)                    - Lifetime: Full run│
│   • ConfigReader                            - Lifetime: Full run│
│   • PipelineOrchestrator                    - Lifetime: Full run│
│   • TableProcessor                          - Lifetime: Full run│
│   • PriorityProcessor                       - Lifetime: Full run│
│                                                                  │
│ Memory: ~5-10 MB (mostly tables.yml config)                     │
└────────────────────────────────────────────────────────────────┘
                              ↓
┌────────────────────────────────────────────────────────────────┐
│ PHASE 2: VALIDATION (Once per run)                             │
├────────────────────────────────────────────────────────────────┤
│ Objects Created & Destroyed:                                    │
│   • Database engines (3)                    - Created, disposed │
│   • Test connections (3)                    - Acquired, released│
│   • Validation results (Dict)               - Used, discarded   │
│                                                                  │
│ Memory: Minimal (connections from pool)                         │
└────────────────────────────────────────────────────────────────┘
                              ↓
┌────────────────────────────────────────────────────────────────┐
│ PHASE 3: CATEGORY PROCESSING (4 times: large/med/small/tiny)   │
├────────────────────────────────────────────────────────────────┤
│ Objects Created per Category:                                   │
│   • Category results dict {}                - Built incrementally│
│   • ThreadPoolExecutor (for large only)     - If parallel      │
│                                                                  │
│ Memory: ~1 MB per category                                      │
└────────────────────────────────────────────────────────────────┘
                              ↓
┌────────────────────────────────────────────────────────────────┐
│ PHASE 4: TABLE PROCESSING (400 times)                          │
├────────────────────────────────────────────────────────────────┤
│ Objects Created PER TABLE:                                      │
│   • Table config dict                       - ~2-5 KB           │
│   • TableProcessingContext                  - ~5 KB             │
│   • SimpleMySQLReplicator instance          - Minimal           │
│   • PostgresLoader instance                 - Minimal           │
│   • Extraction metadata dict                - ~1 KB             │
│   • Load metadata dict                      - ~1 KB             │
│   • Tracking metadata dict                  - ~1 KB             │
│                                                                  │
│ Objects Destroyed After Table:                                  │
│   • All of the above (garbage collected)                        │
│                                                                  │
│ Memory per Table: ~10-15 KB                                     │
│ Total for 400 Tables: ~4-6 MB                                   │
│                                                                  │
│ Note: Row data NOT held in memory (streamed/batched)            │
└────────────────────────────────────────────────────────────────┘
                              ↓
┌────────────────────────────────────────────────────────────────┐
│ PHASE 5: REPORTING (Once per run)                              │
├────────────────────────────────────────────────────────────────┤
│ Objects Created:                                                │
│   • Aggregated results dict                 - ~100 KB           │
│   • Pipeline report dict                    - ~50 KB            │
│   • Notification message strings            - ~10 KB            │
│                                                                  │
│ Memory: Minimal (~200 KB)                                       │
└────────────────────────────────────────────────────────────────┘
                              ↓
┌────────────────────────────────────────────────────────────────┐
│ PHASE 6: CLEANUP (Once per run)                                │
├────────────────────────────────────────────────────────────────┤
│ Objects Destroyed:                                              │
│   • All orchestration objects                                   │
│   • Connection pools disposed                                   │
│   • Settings singleton persists (for next run)                  │
│                                                                  │
│ Memory: Released (Python GC)                                    │
└────────────────────────────────────────────────────────────────┘
```

### Memory Footprint

**Total Peak Memory** (approximate):
- Configuration objects: 5-10 MB
- Per-table processing: 10-15 KB × concurrent tables
  - Sequential: ~15 KB (1 table)
  - Parallel (5 workers): ~75 KB (5 tables)
- Connection pools: Managed by SQLAlchemy (minimal)
- Row data: NOT accumulated (streamed/batched)

**Total**: ~5-15 MB overhead + streaming data

**Why Low Memory**:
- ✅ Small, short-lived objects
- ✅ No data accumulation
- ✅ Streaming/batching for row data
- ✅ Garbage collection after each table
- ✅ Connection pooling (not per-table creation)

---

## Design Patterns

### 1. Settings Injection Pattern

**Pattern**: Create Settings once, pass everywhere

```python
# Create once
settings = get_settings()

# Inject into components
orchestrator = PipelineOrchestrator(settings=settings)
replicator = SimpleMySQLReplicator(settings=settings)
loader = PostgresLoader(settings=settings)
```

**Benefits**:
- ✅ Single source of truth
- ✅ Testable (inject mock Settings)
- ✅ Environment-agnostic (production/test handled automatically)
- ✅ No global state (except singleton pattern)

---

### 2. Configuration Reader Pattern

**Pattern**: Separate reading from using configuration

```python
# Component reads config
config_reader = ConfigReader()

# Component uses config
table_config = config_reader.get_table_config('patient')

# Component doesn't know about YAML files
```

**Benefits**:
- ✅ Separation of concerns
- ✅ Easy to mock for testing
- ✅ Can swap implementations (YAML → JSON → Database)
- ✅ Cached reads (efficient)

---

### 3. Context Object Pattern

**Pattern**: Wrap complex configuration in single object

```python
# Instead of passing many parameters
def process(table_name, config, force_full, strategy, columns, ...):
    pass

# Use context object
context = TableProcessingContext(table_name, force_full, config_reader)
def process(context):
    # Access everything through context
    pass
```

**Benefits**:
- ✅ Reduces parameter passing
- ✅ Centralizes logic (strategy resolution)
- ✅ Easy to extend (add new fields without changing signatures)
- ✅ Consistent configuration access

---

### 4. Result Tuple Pattern

**Pattern**: Return `(success: bool, metadata: dict)`

```python
success, metadata = replicator.copy_table('patient')

if success:
    print(f"Copied {metadata['rows_copied']} rows")
else:
    print(f"Error: {metadata.get('error')}")
```

**Benefits**:
- ✅ Quick success check (boolean)
- ✅ Detailed information (dictionary)
- ✅ Consistent interface across components
- ✅ Error info always available

---

### 5. Metadata Dictionary Pattern

**Pattern**: Use dictionaries for flexible data structures

```python
# Easy to create
metadata = {
    'rows_copied': 1523,
    'duration': 45.2,
    'strategy': 'incremental'
}

# Easy to extend (no class changes needed)
metadata['new_field'] = 'value'

# Easy to serialize (JSON)
json.dumps(metadata)

# Easy to pass to Airflow XCom
context['ti'].xcom_push(key='metadata', value=metadata)
```

**Benefits**:
- ✅ Flexible (can add fields without changing code)
- ✅ Serializable (JSON, YAML, XCom)
- ✅ Easy to log and debug
- ✅ No custom class definitions needed

**Trade-off**: No type checking (vs. dataclasses/Pydantic)

---

### 6. XCom Communication Pattern (Airflow)

**Pattern**: Serialize primitives for inter-task communication

```python
# Task 1: Push data
context['ti'].xcom_push(key='count', value=42)
context['ti'].xcom_push(key='tables', value=['t1', 't2'])

# Task 2: Pull data (runs later, possibly different worker)
count = context['ti'].xcom_pull(task_ids='task1', key='count')
tables = context['ti'].xcom_pull(task_ids='task1', key='tables')
```

**Benefits**:
- ✅ Works across processes/machines
- ✅ Persistent (stored in Airflow DB)
- ✅ Accessible in Airflow UI
- ✅ Debugging-friendly

**Constraints**:
- ❌ Must be JSON-serializable
- ❌ Size limits (~48KB)
- ❌ No complex objects

---

## Type Contracts

### Explicit Type Definitions

While Python's duck typing is flexible, here are the explicit type contracts for clarity:

```python
from typing import Dict, List, Optional, Any, Tuple

# Table configuration from tables.yml
TableConfig = Dict[str, Any]

# Extraction metadata
ExtractionMetadata = Dict[str, Any]

# Load metadata
LoadMetadata = Dict[str, Any]

# Processing results (table → success)
TableResults = Dict[str, bool]

# Category results
CategoryResults = Dict[str, Dict[str, Any]]

# Return type from replicator/loader
ProcessingResult = Tuple[bool, Dict[str, Any]]

# XCom data (JSON-serializable)
XComData = Union[str, int, float, bool, List, Dict, None]
```

### Future Improvements: Pydantic Models

For stricter type safety, consider using Pydantic models:

```python
from pydantic import BaseModel
from typing import List, Optional

class TableConfig(BaseModel):
    table_name: str
    extraction_strategy: str
    batch_size: int
    incremental_columns: List[str]
    performance_category: str
    estimated_rows: int
    # ... etc

class ExtractionMetadata(BaseModel):
    rows_copied: int
    strategy_used: str
    duration: float
    last_primary_value: Optional[Any]
    # ... etc

# Usage
config = TableConfig(**table_config_dict)  # Validates structure
print(config.table_name)  # IDE autocomplete works!
```

**Benefits of Pydantic**:
- ✅ Type validation at runtime
- ✅ IDE autocomplete
- ✅ Automatic documentation
- ✅ Easy serialization/deserialization

**Why Not Currently Used**:
- Configuration evolves frequently
- Flexibility valued over strictness
- Legacy code compatibility
- Could be added incrementally

---

## Conclusion

This document provides the complete data contract specifications for the OpenDental ETL pipeline. Key takeaways:

### ✅ **Lightweight Objects**
- Small, short-lived objects (~10-15 KB per table)
- No memory accumulation
- Efficient garbage collection

### ✅ **Simple Contracts**
- Dictionaries for flexibility
- Tuples for results
- Primitives for XCom

### ✅ **Clear Flow**
- Settings → ConfigReader → Context → Processing → Results
- Each component knows what it receives and returns

### ✅ **Testability**
- All objects mockable
- No complex dependencies
- Clear input/output contracts

### ✅ **Extensibility**
- Easy to add new fields
- Flexible dictionary structures
- Minimal code changes needed

For architectural overview and component interactions, see [PIPELINE_ARCHITECTURE.md](PIPELINE_ARCHITECTURE.md).

---

**Last Updated**: 2025-10-22  
**Maintained By**: Data Engineering Team

