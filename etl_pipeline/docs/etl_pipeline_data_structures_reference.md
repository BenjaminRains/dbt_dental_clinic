# ETL Pipeline Data Structures Reference

## Overview

This document provides a comprehensive reference of all data structures used in the ETL pipeline, organized by class and data type. The ETL pipeline follows a **SQL-first, streaming architecture** where data flows through SQL queries rather than being loaded into memory as DataFrames or arrays.

## Key Architectural Principles

1. **SQL-First Approach**: All data processing happens via SQL queries, not in-memory data structures
2. **No Pandas DataFrames**: The pipeline does NOT use pandas DataFrames for data processing
3. **No NumPy Arrays**: No numerical array processing
4. **No Stacks/Queues**: No traditional stack or queue data structures
5. **Connection Objects**: Heavy use of SQLAlchemy Engine objects for database connections
6. **Dictionary-Heavy**: Extensive use of Python dictionaries for configuration and state management
7. **Threading**: Uses ThreadPoolExecutor for parallel processing, not multiprocessing

## Data Structure Types by Class

### 1. Connection Management

#### Class: `ConnectionFactory`
**Location**: `etl_pipeline/core/connections.py`

**Data Structures**:
- **SQLAlchemy Engine Objects**: `sqlalchemy.engine.Engine`
  - `create_mysql_engine()` - Creates MySQL database engines
  - `create_postgres_engine()` - Creates PostgreSQL database engines
  - `get_source_connection()` - Gets source MySQL connection
  - `get_replication_connection()` - Gets replication MySQL connection
  - `get_analytics_connection()` - Gets analytics PostgreSQL connection

**Purpose**: Database connection pools and engines

#### Class: `ConnectionManager`
**Location**: `etl_pipeline/core/connections.py`

**Data Structures**:
- **Connection Pool Management**: `sqlalchemy.engine.Engine` with connection lifecycle
  - `get_connection()` - Manages connection lifecycle
  - `execute_with_retry()` - Handles retry logic for failed queries

### 2. Configuration Management

#### Class: `ConfigReader`
**Location**: `etl_pipeline/config/config_reader.py`

**Data Structures**:
- **YAML Configuration Dictionaries**: `Dict[str, Any]`
  - `get_table_config()` - Returns table-specific configuration
  - `get_all_tables()` - Returns all table configurations
  - `get_tables_by_priority()` - Returns tables grouped by priority

#### Class: `Settings`
**Location**: `etl_pipeline/config/settings.py`

**Data Structures**:
- **Environment Configuration Dictionaries**: `Dict[str, Any]`
  - `get_source_connection_config()` - Returns source database config
  - `get_replication_connection_config()` - Returns replication database config
  - `get_analytics_connection_config()` - Returns analytics database config

### 3. Schema and Data Transformation

#### Class: `PostgresSchema`
**Location**: `etl_pipeline/core/postgres_schema.py`

**Data Structures**:
- **Schema Conversion Dictionaries**: `Dict[str, Any]`
  - `get_table_schema_from_mysql()` - Extracts MySQL table schema
  - `adapt_schema()` - Converts MySQL schema to PostgreSQL
  - `create_postgres_table()` - Creates PostgreSQL tables
  - `convert_row_data_types()` - Converts data types during ETL

### 4. ETL Processing

#### Class: `SimpleMySQLReplicator`
**Location**: `etl_pipeline/core/simple_mysql_replicator.py`

**Data Structures**:
- **Table Replication Status**: `Dict[str, bool]`
- **Batch Processing Metadata**: `Dict[str, Any]`
- **SQLAlchemy Result Objects**: `sqlalchemy.engine.Result`

**Methods**:
- `copy_table()` - Copies individual tables
- `copy_all_tables()` - Copies all configured tables
- `_copy_small_table()` - Handles small table replication
- `_copy_medium_table()` - Handles medium table replication
- `_copy_large_table()` - Handles large table replication

#### Class: `PostgresLoader`
**Location**: `etl_pipeline/loaders/postgres_loader.py`

**Data Structures**:
- **Load Status Tracking**: `Dict[str, Any]`
- **Incremental Processing Metadata**: `Dict[str, Any]`
- **SQLAlchemy Result Objects**: `sqlalchemy.engine.Result`

**Methods**:
- `load_table()` - Loads tables to PostgreSQL
- `load_table_chunked()` - Handles chunked loading for large tables
- `verify_load()` - Validates load completion
- `_build_load_query()` - Constructs SQL queries for loading

### 5. Orchestration

#### Class: `PipelineOrchestrator`
**Location**: `etl_pipeline/orchestration/pipeline_orchestrator.py`

**Data Structures**:
- **Pipeline Execution State**: `Dict[str, Any]`
- **Component Coordination**: `Dict[str, Any]`

**Methods**:
- `run_pipeline_for_table()` - Orchestrates single table processing
- `process_tables_by_priority()` - Orchestrates batch processing
- `initialize_connections()` - Sets up database connections

#### Class: `TableProcessor`
**Location**: `etl_pipeline/orchestration/table_processor.py`

**Data Structures**:
- **Individual Table Processing State**: `Dict[str, Any]`
- **ETL Phase Tracking**: `Dict[str, Any]`

**Methods**:
- `process_table()` - Handles complete ETL pipeline for single table
- `_extract_to_replication()` - Extracts data to replication database
- `_load_to_analytics()` - Loads data to analytics database

#### Class: `PriorityProcessor`
**Location**: `etl_pipeline/orchestration/priority_processor.py`

**Data Structures**:
- **Priority-Based Processing Queues**: `List[str]`
- **Parallel Execution State**: `Dict[str, List[str]]`
- **ThreadPoolExecutor**: `concurrent.futures.ThreadPoolExecutor`

**Methods**:
- `process_by_priority()` - Processes tables by priority level
- `_process_parallel()` - Handles parallel processing
- `_process_sequential()` - Handles sequential processing

### 6. Monitoring and Metrics

#### Class: `UnifiedMetricsCollector`
**Location**: `etl_pipeline/monitoring/unified_metrics.py`

**Data Structures**:
- **Pipeline Metrics Dictionary**: `Dict[str, Any]`
- **Table Processing Statistics**: `Dict[str, Dict[str, Any]]`
- **Error Tracking Lists**: `List[Dict[str, Any]]`
- **Datetime Objects**: `datetime.datetime`

**Methods**:
- `record_table_processed()` - Records table processing metrics
- `record_error()` - Records pipeline errors
- `get_pipeline_status()` - Returns current pipeline status
- `save_metrics()` - Persists metrics to database
- `cleanup_old_metrics()` - Manages metrics retention

### 7. Connection Tracking

#### Class: `ConnectionTracker`
**Location**: `etl_pipeline/scripts/analyze_connection_usage.py`

**Data Structures**:
- **Connection Event Lists**: `List[ConnectionEvent]`
- **Active Connections Set**: `Set[str]`
- **Connection Start Times Dictionary**: `Dict[str, float]`
- **Connection Statistics**: `ConnectionStats` (dataclass)
- **Threading Lock**: `threading.Lock`

**Methods**:
- `track_connection_created()` - Records connection creation events
- `track_connection_disposed()` - Records connection disposal events
- `get_connection_summary()` - Returns connection usage statistics

#### Class: `ConnectionAnalyzer`
**Location**: `etl_pipeline/scripts/analyze_connection_usage.py`

**Data Structures**:
- **Connection Analysis Results**: `Dict[str, Any]`
- **Optimization Recommendations**: `List[str]`
- **Connection Patterns**: `Dict[str, Any]`

**Methods**:
- `analyze_simple_mysql_replicator()` - Analyzes replicator connection usage
- `analyze_connection_patterns()` - Analyzes connection patterns
- `generate_report()` - Generates connection usage reports

## Detailed Data Structure Examples

### Dictionary-Based Configuration
```python
# Configuration dictionaries
self.metrics = {
    'pipeline_id': f"pipeline_{int(time.time())}",
    'start_time': None,
    'end_time': None,
    'total_time': None,
    'tables_processed': 0,
    'total_rows_processed': 0,
    'errors': [],
    'table_metrics': {},
    'status': 'idle'
}
```

### List-Based Collections
```python
# Lists for tracking tables and errors
success_tables = []
failed_tables = []
errors = []
```

### Set-Based Unique Collections
```python
# Sets for tracking unique connections
self.active_connections: Set[str] = set()
```

### Dataclass-Based Structured Data
```python
@dataclass
class ConnectionEvent:
    timestamp: float
    event_type: str
    database_type: str
    database_name: str
    schema: Optional[str] = None
    thread_id: int = field(default_factory=lambda: threading.get_ident())
    connection_id: Optional[str] = None
```

### DefaultDict-Based Aggregation
```python
# For counting and aggregating statistics
database_breakdown: Dict[str, int] = field(default_factory=lambda: defaultdict(int))
schema_breakdown: Dict[str, int] = field(default_factory=lambda: defaultdict(int))
```

### ThreadPoolExecutor-Based Parallel Processing
```python
# Parallel task execution
with ThreadPoolExecutor(max_workers=max_workers) as executor:
    future_to_table = {
        executor.submit(self._process_single_table, table, force_full): table 
        for table in tables
    }
```

### SQLAlchemy Result Objects
```python
# Database query results
result = conn.execute(text(f"SHOW CREATE TABLE `{table_name}`"))
row = result.fetchone()
```

### Tuple-Based Return Values
```python
# Success/failure lists
return success_tables, failed_tables
```

## Data Flow Architecture

### SQL-First Processing Flow
1. **Extract**: SQL queries extract data from source database
2. **Transform**: SQL queries transform data during transfer
3. **Load**: SQL queries load data into target database
4. **No In-Memory Processing**: Data never loaded into DataFrames or arrays

### Connection Management Flow
1. **Connection Factory**: Creates SQLAlchemy Engine objects
2. **Connection Manager**: Manages connection lifecycle
3. **Connection Tracking**: Monitors connection usage patterns
4. **Connection Analysis**: Provides optimization recommendations

### Orchestration Flow
1. **Pipeline Orchestrator**: Coordinates overall pipeline execution
2. **Table Processor**: Handles individual table ETL
3. **Priority Processor**: Manages batch processing by priority
4. **Metrics Collector**: Tracks performance and errors

## Performance Characteristics

### Memory Usage
- **Low Memory Footprint**: No large DataFrames or arrays in memory
- **Streaming Processing**: Data processed in chunks via SQL
- **Connection Pooling**: Efficient database connection management

### Processing Speed
- **SQL-Optimized**: Leverages database engine optimizations
- **Parallel Processing**: ThreadPoolExecutor for concurrent operations
- **Incremental Loading**: Only processes changed data

### Scalability
- **Database-Side Processing**: Leverages database engine capabilities
- **Connection Pooling**: Efficient resource utilization
- **Chunked Processing**: Handles large datasets without memory issues

## Best Practices

### Data Structure Usage
1. **Use Dictionaries for Configuration**: Centralized configuration management
2. **Use Lists for Collections**: Simple, efficient collections
3. **Use Sets for Uniqueness**: Fast membership testing
4. **Use Dataclasses for Structure**: Type-safe structured data
5. **Use SQLAlchemy for Database**: Efficient database operations

### Memory Management
1. **Avoid Large In-Memory Structures**: Process data via SQL
2. **Use Connection Pooling**: Efficient database connections
3. **Implement Proper Cleanup**: Release resources promptly
4. **Monitor Memory Usage**: Track connection and processing patterns

### Error Handling
1. **Structured Error Tracking**: Comprehensive error logging
2. **Graceful Degradation**: Handle failures without pipeline collapse
3. **Retry Mechanisms**: Automatic retry for transient failures
4. **Error Recovery**: Resume processing after failures

## Conclusion

The ETL pipeline uses a **streaming, SQL-first architecture** with Python's built-in data structures for configuration, state management, and orchestration. The design prioritizes:

- **Efficiency**: SQL-based processing leverages database optimizations
- **Scalability**: Streaming architecture handles large datasets
- **Reliability**: Comprehensive error handling and monitoring
- **Maintainability**: Clean separation of concerns and structured code

This approach eliminates the need for large in-memory data structures while providing robust, scalable data processing capabilities. 