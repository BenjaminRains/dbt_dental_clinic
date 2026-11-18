# ETL Pipeline Architecture

## Overview

This document provides a high-level architectural overview of the OpenDental ETL pipeline. The pipeline is designed to extract data from a remote OpenDental MySQL database, replicate it to a local MySQL database, and then load it into a PostgreSQL analytics database for transformation via dbt.

**Related Documentation**:
- **[DATA_CONTRACTS.md](DATA_CONTRACTS.md)**: Detailed specifications of Python objects, data structures, and information flow between components
- This document: System architecture, components, and data flow at a high level

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        ETL PIPELINE ARCHITECTURE                         │
└─────────────────────────────────────────────────────────────────────────┘

 CONFIGURATION LAYER                    EXECUTION LAYER
┌─────────────────────┐              ┌─────────────────────┐
│ analyze_opendental  │              │  pipeline_          │
│ _schema.py          │◄─────────────┤  orchestrator.py    │
│                     │   Uses       │                     │
│ • Analyzes source   │              │ • Main entry point  │
│ • Generates config  │              │ • Coordinates flow  │
│ • Detects changes   │              │ • Manages resources │
└──────────┬──────────┘              └──────────┬──────────┘
           │                                    │
           │ Generates                          │ Delegates to
           ↓                                    ↓
┌─────────────────────┐              ┌─────────────────────┐
│    tables.yml       │◄─────────────┤ priority_processor  │
│                     │   Reads      │                     │
│ • Table metadata    │              │ • Parallel/         │
│ • Batch sizes       │              │   sequential        │
│ • Strategies        │              │   processing        │
│ • Performance data  │              │ • Resource mgmt     │
└──────────┬──────────┘              └──────────┬──────────┘
           │                                    │
           │ Consumed by                        │ Uses
           ↓                                    ↓
┌─────────────────────┐              ┌─────────────────────┐
│    settings.py      │◄─────────────┤  table_processor    │
│                     │   Uses       │                     │
│ • Env management    │              │ • Single table ETL  │
│ • DB configuration  │              │ • Extract phase     │
│ • Validation        │              │ • Load phase        │
└──────────┬──────────┘              └──────────┬──────────┘
           │                                    │
           │ Provides to                        │ Orchestrates
           ↓                                    ↓
┌─────────────────────┐              ┌─────────────────────┐
│   providers.py      │              │  DATA FLOW          │
│   connections.py    │              └─────────────────────┘
│                     │                        │
│ • Config providers  │                        │ Phase 1: Extract
│ • Connection pools  │                        ↓
│ • Performance opts  │              ┌─────────────────────┐
└─────────────────────┘              │ simple_mysql_       │
                                     │ replicator.py       │
                                     │                     │
 DATA SOURCES                        │ • Source → Replic   │
┌─────────────────────┐              │ • Incremental       │
│  OpenDental MySQL   │──────────────┤ • Tracking          │
│  (Remote Source)    │ Extracts     │ • Performance opts  │
└─────────────────────┘              └──────────┬──────────┘
                                                │ Copies to
                                                ↓
                                     ┌─────────────────────┐
                                     │  Replication MySQL  │
                                     │  (Local)            │
                                     └──────────┬──────────┘
                                                │ Phase 2: Load
                                                ↓
                                     ┌─────────────────────┐
                                     │ postgres_loader.py  │
                                     │                     │
                                     │ • Replic → Analytics│
                                     │ • Schema conversion │
                                     │ • Type mapping      │
                                     │ • Chunked loading   │
                                     └──────────┬──────────┘
                                                │ Loads to
                                                ↓
                                     ┌─────────────────────┐
                                     │ PostgreSQL Analytics│
                                     │ (raw schema)        │
                                     └─────────────────────┘
```

## Component Descriptions

### 1. Schema Analysis & Configuration

#### `analyze_opendental_schema.py`
**Purpose**: THE single source of truth for schema analysis and configuration generation.

**Responsibilities**:
- Discovers all tables in the OpenDental MySQL database
- Analyzes table characteristics (row counts, sizes, columns)
- Identifies incremental loading columns (timestamp/datetime fields)
- Determines optimal extraction strategies (full vs. incremental)
- Calculates performance-optimized batch sizes
- Detects schema changes (Slowly Changing Dimensions)
- Generates `tables.yml` configuration file

**Key Features**:
- Uses information_schema for fast, non-locking row counts
- Validates data quality of incremental columns
- Assigns processing priorities (high/medium/low)
- Generates schema change reports
- Performance category classification (large/medium/small/tiny)

**Output**: `etl_pipeline/config/tables.yml`

#### `tables.yml`
**Purpose**: Static configuration file containing complete metadata for all tables.

**Contents**:
- **Metadata Section**: Generation timestamp, environment, schema hash
- **Table Configurations**: For each table:
  - `extraction_strategy`: full_table, incremental, or incremental_chunked
  - `incremental_columns`: List of timestamp columns for incremental loading
  - `primary_incremental_column`: Primary column for change tracking
  - `batch_size`: Optimized batch size for extraction/loading
  - `performance_category`: large, medium, small, or tiny
  - `processing_priority`: high, medium, or low
  - `estimated_rows`: Approximate row count
  - `estimated_size_mb`: Approximate table size
  - `monitoring`: Performance thresholds and alerting configuration

**Usage**: Read by settings.py and consumed throughout the pipeline for table-specific configuration.

### 2. Configuration Management

#### `settings.py`
**Purpose**: Centralized configuration management with environment-aware database connections.

**Responsibilities**:
- Detects and validates environment (production/test)
- Loads environment-specific configuration files (`.env_production`, `.env_test`)
- Provides database connection configurations
- Validates required environment variables
- Manages table configuration access
- Implements fail-fast validation

**Key Features**:
- Environment detection with fail-fast validation
- Separate configuration for source, replication, and analytics databases
- Schema-specific PostgreSQL connections (raw, staging, intermediate, marts)
- Provider pattern for dependency injection
- Connection parameter validation

**Connection Types**:
- `DatabaseType.SOURCE`: OpenDental MySQL (remote, read-only)
- `DatabaseType.REPLICATION`: Local MySQL replication database
- `DatabaseType.ANALYTICS`: PostgreSQL analytics warehouse

#### `providers.py`
**Purpose**: Configuration provider pattern for flexible configuration sources.

**Providers**:
- **FileConfigProvider**: Loads configuration from files (`.env_*`, `.yml`)
- **DictConfigProvider**: In-memory configuration for testing

**Responsibilities**:
- Environment file loading with prefix filtering
- YAML configuration parsing
- Configuration validation
- Environment variable management

#### `connections.py`
**Purpose**: Database connection factory with connection pooling and performance optimizations.

**Components**:

1. **ConnectionFactory**
   - Creates SQLAlchemy engines with optimal pool settings
   - Applies database-specific performance optimizations
   - Validates connection parameters
   - Supports MySQL and PostgreSQL connections

2. **ConnectionManager**
   - Manages connection lifecycle and reuse
   - Implements retry logic with exponential backoff
   - Rate limiting to prevent overwhelming source database
   - Automatic connection cleanup

**Performance Optimizations**:
- **MySQL**: Bulk insert buffers, disabled foreign key checks, optimized transaction settings
- **PostgreSQL**: Increased work_mem, maintenance_work_mem, disabled synchronous_commit
- **Connection Pools**: Configurable pool size, overflow, timeout, and recycle settings

### 3. Data Extraction (Phase 1)

#### `simple_mysql_replicator.py`
**Purpose**: Extracts data from OpenDental MySQL source to local MySQL replication database.

**Architecture**:
- **Source**: Always remote OpenDental server (client location)
- **Target**: Always localhost replication database
- **Strategy**: Cross-server copy only (no same-server logic)

**Extraction Strategies**:
1. **full_table**: Drop and recreate entire table
   - Used for: Tables without incremental columns, first-time extraction
   - Process: DROP TABLE → CREATE TABLE → INSERT in batches

2. **incremental**: Only copy new/changed data
   - Used for: Tables with timestamp/datetime columns
   - Process: Compare last values → INSERT only new rows
   - Tracking: Uses `etl_copy_status` table for last processed values

3. **incremental_chunked**: Incremental with smaller batches
   - Used for: Very large tables (>1M rows)
   - Process: Same as incremental but with optimized chunk sizes

**Copy Methods** (Performance-based):
- **SMALL** (<1MB): Direct cross-server copy with retry logic
- **MEDIUM** (1-100MB): Chunked cross-server copy with rate limiting
- **LARGE** (>100MB): Progress-tracked cross-server copy with optimized batches

**Key Features**:
- ConnectionManager integration for retry logic and rate limiting
- Dynamic batch sizing based on table characteristics
- Performance history tracking for adaptive optimization
- Time gap analysis for intelligent full vs. incremental decisions
- MySQL session optimizations for bulk operations

**Tracking**: Updates `opendental_replication.etl_copy_status` table with:
- Last copy timestamp
- Rows copied
- Copy status (success/failed)
- Last primary key value processed
- Primary column name

### 4. Data Loading (Phase 2)

#### `postgres_loader.py`
**Purpose**: Loads data from MySQL replication database to PostgreSQL analytics database (raw schema).

**Architecture**:
- **Source**: MySQL replication database (localhost)
- **Target**: PostgreSQL analytics database (raw schema)
- **Role**: PURE DATA MOVEMENT LAYER (delegates transformation to PostgresSchema)

**Loading Strategies**:
1. **load_table()**: Standard loading
   - Reads all data from MySQL replication table
   - Converts data types via PostgresSchema
   - Bulk inserts into PostgreSQL
   - Suitable for small to medium tables

2. **load_table_chunked()**: Chunked loading
   - Processes data in configurable chunks
   - Memory-efficient for large tables (>100MB)
   - Progress tracking and error recovery
   - Reduced memory footprint

**Key Features**:
- **Schema Integration**: Uses PostgresSchema for:
  - Schema conversion (MySQL → PostgreSQL types)
  - Data type conversion (MySQL → PostgreSQL values)
  - Table creation and modification
  - Type validation

- **Incremental Loading**:
  - Checks if analytics needs updating from replication
  - Compares last processed values
  - Skips load if analytics is up-to-date
  - Tracks progress in `etl_load_status` table

- **Load Verification**:
  - Row count validation
  - Schema consistency checks
  - Error detection and reporting

**What PostgresLoader Does NOT Do**:
- ❌ Business logic transformations
- ❌ Data quality rules
- ❌ Analytics preparation
- ❌ Schema analysis (delegated to PostgresSchema)

**What PostgresLoader DOES Do**:
- ✅ Data extraction from MySQL replication
- ✅ Data type conversion (via PostgresSchema)
- ✅ Bulk loading to PostgreSQL
- ✅ Load verification and tracking

**Tracking**: Updates `opendental_analytics.raw.etl_load_status` table with:
- Last load timestamp
- Rows loaded
- Load status (success/failed)
- Last primary key value processed

### 5. Pipeline Orchestration

#### `table_processor.py`
**Purpose**: Core ETL component that orchestrates the complete ETL pipeline for individual tables.

**ETL Pipeline Flow**:
```
1. EXTRACT → simple_mysql_replicator.py
   - Copy from Source MySQL to Replication MySQL
   
2. LOAD → postgres_loader.py
   - Copy from Replication MySQL to PostgreSQL Analytics
```

**Responsibilities**:
- Initializes database connections via Settings injection
- Gets table configuration from tables.yml
- Executes extraction phase (SimpleMySQLReplicator)
- Executes load phase (PostgresLoader)
- Tracks performance metrics
- Updates pipeline status in tracking tables
- Error handling and recovery

**Key Features**:
- Settings injection for environment-agnostic operation
- Unified processing context for configuration access
- Performance monitoring and tracking
- Adaptive strategy resolution (force_full handling)
- Comprehensive error handling with custom exceptions

**Configuration Resolution**:
- Reads table config from tables.yml via ConfigReader
- Resolves extraction strategy based on:
  - force_full parameter
  - Available incremental columns
  - Configuration extraction_strategy
- Determines actual strategy and provides reason

**Status Tracking**:
- Updates MySQL tracking: `etl_copy_status` (extraction phase)
- Updates PostgreSQL tracking: `etl_load_status` (load phase)
- Tracks rows processed, duration, strategy used

#### `priority_processor.py`
**Purpose**: Manages table processing based on performance categories with intelligent parallelization.

**Processing Categories** (from schema analyzer):
- **large**: Tables with 1M+ rows → **Parallel processing**
- **medium**: Tables with 100K-1M rows → Sequential processing
- **small**: Tables with 10K-100K rows → Sequential processing
- **tiny**: Tables with <10K rows → Sequential processing

**Processing Logic**:
1. Categorizes tables by `performance_category` from tables.yml
2. Processes large tables in parallel (configurable max_workers)
3. Processes smaller tables sequentially to manage resources
4. Handles failures and propagates errors
5. Returns results summary by category

**Key Features**:
- ThreadPoolExecutor for parallel processing
- Resource management with configurable worker count
- Error handling for individual table failures
- Performance category validation
- Integration with TableProcessor for actual ETL work

**Why Parallel for Large Tables?**
- Large tables take longest to process
- Parallelization reduces total pipeline runtime
- Independent table processing (no dependencies)
- Resource utilization optimization

#### `pipeline_orchestrator.py`
**Purpose**: Main orchestration entry point that coordinates the entire ETL pipeline.

**Responsibilities**:
- Initializes all pipeline components
- Validates environment configuration
- Provides simple interface for pipeline execution
- Manages resource cleanup
- Delegates to TableProcessor and PriorityProcessor

**Entry Points**:
1. **run_pipeline_for_table()**
   - Process single table through complete ETL pipeline
   - Used for: Ad-hoc table processing, testing, recovery

2. **process_tables_by_priority()**
   - Process multiple tables by performance category
   - Used for: Full pipeline runs, scheduled batch processing
   - Intelligent parallelization for large tables

3. **process_tables_by_performance_category()**
   - Process all tables in a specific category
   - Used for: Category-specific processing

**Context Manager**:
- Implements `__enter__` and `__exit__` for automatic cleanup
- Ensures resources are properly released
- Handles exceptions gracefully

**Modern Architecture**:
- Settings injection for environment-agnostic operation
- Components handle their own connections
- No direct connection management
- Provider pattern for configuration
- Fail-fast validation

## Data Flow

### Complete ETL Pipeline Flow

```
┌─────────────────────────────────────────────────────────────┐
│                    COMPLETE DATA FLOW                        │
└─────────────────────────────────────────────────────────────┘

1. SCHEMA ANALYSIS (One-time or when schema changes)
   ┌────────────────────────────────────┐
   │ analyze_opendental_schema.py       │
   │ • Connects to OpenDental MySQL     │
   │ • Analyzes all tables              │
   │ • Generates tables.yml             │
   └────────────┬───────────────────────┘
                │
                ↓
   ┌────────────────────────────────────┐
   │ tables.yml created/updated         │
   └────────────────────────────────────┘

2. PIPELINE INITIALIZATION
   ┌────────────────────────────────────┐
   │ User runs CLI command              │
   │ $ python -m etl_pipeline.cli run  │
   └────────────┬───────────────────────┘
                │
                ↓
   ┌────────────────────────────────────┐
   │ pipeline_orchestrator.py           │
   │ • Loads settings.py                │
   │ • Validates environment            │
   │ • Initializes components           │
   └────────────┬───────────────────────┘
                │
                ↓
   ┌────────────────────────────────────┐
   │ settings.py                        │
   │ • Loads .env_production/.env_test  │
   │ • Validates DB configurations      │
   │ • Reads tables.yml                 │
   └────────────┬───────────────────────┘
                │
                ↓
   ┌────────────────────────────────────┐
   │ connections.py                     │
   │ • Creates connection pools         │
   │ • Applies performance settings     │
   └────────────────────────────────────┘

3. TABLE SELECTION & PRIORITIZATION
   ┌────────────────────────────────────┐
   │ priority_processor.py              │
   │ • Reads tables.yml                 │
   │ • Groups by performance_category   │
   │ • Plans execution order            │
   └────────────┬───────────────────────┘
                │
                ↓
   ┌────────────────────────────────────┐
   │ Large tables → Parallel            │
   │ Other tables → Sequential          │
   └────────────┬───────────────────────┘
                │
                ↓

4. PER-TABLE ETL EXECUTION (for each table)
   ┌────────────────────────────────────┐
   │ table_processor.py                 │
   │ • Gets table config from tables.yml│
   │ • Resolves extraction strategy     │
   └────────────┬───────────────────────┘
                │
                ├─── PHASE 1: EXTRACT ───┐
                ↓                         │
   ┌────────────────────────────────────┐│
   │ simple_mysql_replicator.py         ││
   │                                    ││
   │ Reads from:                        ││
   │ ┌──────────────────────────────┐  ││
   │ │ OpenDental MySQL             │  ││
   │ │ (Remote Source)              │  ││
   │ │ - Production database        │  ││
   │ │ - Read-only access           │  ││
   │ └──────────────────────────────┘  ││
   │                                    ││
   │ Strategy Resolution:                ││
   │ • Check tables.yml configuration    ││
   │ • Determine full vs. incremental    ││
   │ • Calculate batch size              ││
   │                                    ││
   │ Extraction:                        ││
   │ • SELECT data in batches           ││
   │ • Apply incremental filters        ││
   │ • Track progress                   ││
   │                                    ││
   │ Writes to:                         ││
   │ ┌──────────────────────────────┐  ││
   │ │ MySQL Replication            │  ││
   │ │ (Local Staging)              │  ││
   │ │ - Full table replica         │  ││
   │ │ - INSERT in batches          │  ││
   │ └──────────────────────────────┘  ││
   │                                    ││
   │ Tracking:                          ││
   │ • Update etl_copy_status          ││
   │ • Record rows copied              ││
   │ • Record last value processed     ││
   └────────────┬───────────────────────┘│
                │                         │
                ├─── PHASE 2: LOAD ───────┘
                ↓
   ┌────────────────────────────────────┐
   │ postgres_loader.py                 │
   │                                    │
   │ Reads from:                        │
   │ ┌──────────────────────────────┐  │
   │ │ MySQL Replication            │  │
   │ │ (Local Staging)              │  │
   │ │ - Consistent snapshot        │  │
   │ │ - SELECT all data            │  │
   │ └──────────────────────────────┘  │
   │                                    │
   │ Schema Conversion:                 │
   │ • PostgresSchema.adapt_schema()    │
   │ • Convert MySQL → PostgreSQL types │
   │ • Create/update target table       │
   │                                    │
   │ Data Transformation:               │
   │ • PostgresSchema.convert_row_data()│
   │ • Type conversion                  │
   │ • Value normalization              │
   │                                    │
   │ Loading:                           │
   │ • Bulk INSERT or chunked INSERT    │
   │ • Transaction management           │
   │ • Progress tracking                │
   │                                    │
   │ Writes to:                         │
   │ ┌──────────────────────────────┐  │
   │ │ PostgreSQL Analytics         │  │
   │ │ (Raw Schema)                 │  │
   │ │ - Converted schema           │  │
   │ │ - Normalized data            │  │
   │ │ - Ready for dbt              │  │
   │ └──────────────────────────────┘  │
   │                                    │
   │ Verification:                      │
   │ • Validate row counts              │
   │ • Check data integrity             │
   │                                    │
   │ Tracking:                          │
   │ • Update etl_load_status           │
   │ • Record rows loaded               │
   │ • Record load timestamp            │
   └────────────────────────────────────┘

5. RESULTS & MONITORING
   ┌────────────────────────────────────┐
   │ Metrics Collection                 │
   │ • Processing times                 │
   │ • Rows processed                   │
   │ • Success/failure status           │
   └────────────┬───────────────────────┘
                │
                ↓
   ┌────────────────────────────────────┐
   │ Results Summary                    │
   │ • Tables succeeded                 │
   │ • Tables failed                    │
   │ • Performance statistics           │
   └────────────────────────────────────┘
```

### Detailed Phase Breakdown

#### Phase 1: Extract (Source → Replication)

**Component**: `simple_mysql_replicator.py`

1. **Table Configuration Loading**
   - Read table configuration from tables.yml
   - Get extraction_strategy, batch_size, incremental_columns
   - Determine performance_category

2. **Strategy Resolution**
   ```
   IF force_full = True:
       strategy = "full_table"
   ELSE IF no incremental_columns:
       strategy = "full_table"
   ELSE IF extraction_strategy = "incremental":
       strategy = "incremental"
   ELSE:
       strategy = "full_table"
   ```

3. **Extraction Execution**
   
   **Full Table**:
   ```sql
   -- Drop existing table
   DROP TABLE IF EXISTS replication.table_name;
   
   -- Create new table (copy schema from source)
   CREATE TABLE replication.table_name LIKE source.table_name;
   
   -- Copy data in batches
   REPEAT:
       SELECT * FROM source.table_name 
       LIMIT batch_size OFFSET current_offset;
       
       INSERT INTO replication.table_name VALUES (...);
   UNTIL no more rows
   ```

   **Incremental**:
   ```sql
   -- Get last processed value from tracking
   SELECT last_primary_value FROM etl_copy_status 
   WHERE table_name = 'table_name';
   
   -- Copy only new/changed data
   SELECT * FROM source.table_name
   WHERE primary_incremental_column > last_primary_value
   ORDER BY primary_incremental_column
   LIMIT batch_size;
   
   -- Insert into replication
   INSERT INTO replication.table_name VALUES (...);
   
   -- Update tracking
   UPDATE etl_copy_status SET 
       last_primary_value = max_value,
       last_copy_date = NOW(),
       rows_copied = count
   WHERE table_name = 'table_name';
   ```

4. **Performance Optimizations**
   - Dynamic batch sizing based on performance_category
   - MySQL session optimizations (bulk_insert_buffer_size, autocommit)
   - Rate limiting to prevent overwhelming source
   - Connection retry logic with exponential backoff

#### Phase 2: Load (Replication → Analytics)

**Component**: `postgres_loader.py`

1. **Schema Preparation**
   ```python
   # Get MySQL schema from replication database
   mysql_schema = get_table_schema(replication_engine, table_name)
   
   # Convert to PostgreSQL schema
   postgres_schema = PostgresSchema.adapt_schema(mysql_schema)
   
   # Create or update PostgreSQL table
   PostgresSchema.create_postgres_table(analytics_engine, 
                                        table_name, 
                                        postgres_schema)
   ```

2. **Data Extraction from Replication**
   ```sql
   -- Check if analytics needs updating
   SELECT MAX(primary_key) FROM replication.table_name;
   SELECT MAX(primary_key) FROM analytics.raw.table_name;
   
   IF replication_max > analytics_max:
       -- Extract new/changed data
       SELECT * FROM replication.table_name
       WHERE primary_key > analytics_max
       ORDER BY primary_key;
   ELSE:
       -- Skip, analytics is up-to-date
   ```

3. **Data Type Conversion**
   ```python
   for row in mysql_rows:
       # Convert MySQL types to PostgreSQL types
       converted_row = PostgresSchema.convert_row_data_types(
           row, 
           mysql_schema, 
           postgres_schema
       )
       postgres_rows.append(converted_row)
   ```

4. **Loading to Analytics**
   
   **Standard Loading**:
   ```python
   # Begin transaction
   with analytics_engine.begin() as conn:
       # Bulk insert
       conn.execute(
           f"INSERT INTO raw.{table_name} VALUES (...)",
           postgres_rows
       )
       
       # Update tracking
       conn.execute(
           "UPDATE raw.etl_load_status SET ...",
           {...}
       )
   ```

   **Chunked Loading**:
   ```python
   chunk_size = config.get('batch_size', 5000)
   
   for chunk in chunks(postgres_rows, chunk_size):
       with analytics_engine.begin() as conn:
           conn.execute(
               f"INSERT INTO raw.{table_name} VALUES (...)",
               chunk
           )
   ```

5. **Verification**
   ```sql
   -- Verify row counts match
   SELECT COUNT(*) FROM replication.table_name;
   SELECT COUNT(*) FROM analytics.raw.table_name;
   
   -- Log discrepancies if any
   ```

## Configuration Files

### Environment Files

**`.env_production`** and **`.env_test`**

```bash
# Environment designation
ETL_ENVIRONMENT=production

# OpenDental Source (Remote MySQL)
OPENDENTAL_SOURCE_HOST=192.168.1.100
OPENDENTAL_SOURCE_PORT=3306
OPENDENTAL_SOURCE_DB=opendental
OPENDENTAL_SOURCE_USER=readonly_user
OPENDENTAL_SOURCE_PASSWORD=****

# MySQL Replication (Local MySQL)
MYSQL_REPLICATION_HOST=localhost
MYSQL_REPLICATION_PORT=3306
MYSQL_REPLICATION_DB=opendental_replication
MYSQL_REPLICATION_USER=replication_user
MYSQL_REPLICATION_PASSWORD=****

# PostgreSQL Analytics (Local PostgreSQL)
POSTGRES_ANALYTICS_HOST=localhost
POSTGRES_ANALYTICS_PORT=5432
POSTGRES_ANALYTICS_DB=opendental_analytics
POSTGRES_ANALYTICS_SCHEMA=raw
POSTGRES_ANALYTICS_USER=analytics_user
POSTGRES_ANALYTICS_PASSWORD=****
```

### Tables Configuration

**`etl_pipeline/config/tables.yml`**

Structure generated by `analyze_opendental_schema.py`:

```yaml
metadata:
  generated_at: '2025-10-19T19:21:15.521320'
  analyzer_version: 4.0_performance_enhanced
  environment: production
  total_tables: 436
  schema_hash: a07eb24347005056bc67224c40de7037

tables:
  patient:
    table_name: patient
    extraction_strategy: incremental
    estimated_rows: 50000
    estimated_size_mb: 125.5
    batch_size: 50000
    incremental_columns:
      - PatNum
      - DateTStamp
    primary_incremental_column: DateTStamp
    performance_category: medium
    processing_priority: high
    time_gap_threshold_days: 30
    estimated_processing_time_minutes: 2.5
    memory_requirements_mb: 256
    monitoring:
      alert_on_failure: true
      alert_on_slow_extraction: true
      performance_threshold_records_per_second: 2000
    primary_key: PatNum
```

## Error Handling

### Custom Exception Hierarchy

```
ETLPipelineError (base)
├── ConfigurationError
│   ├── EnvironmentError
│   └── ValidationError
├── DatabaseError
│   ├── DatabaseConnectionError
│   ├── DatabaseQueryError
│   └── DatabaseTransactionError
└── DataError
    ├── DataExtractionError
    └── DataLoadingError
```

### Error Handling Strategy

1. **Fail-Fast Validation**
   - Environment configuration validation before processing
   - Connection parameter validation
   - Configuration file validation

2. **Graceful Degradation**
   - Individual table failures don't stop entire pipeline
   - Failed tables logged and reported
   - Successful tables proceed

3. **Automatic Retry**
   - Connection failures retry with exponential backoff
   - Transient errors automatically retried
   - Configurable retry limits and delays

4. **Comprehensive Logging**
   - All errors logged with context
   - Custom exceptions include relevant details
   - Tracking tables record failure status

## Performance Characteristics

### Optimization Features

1. **Schema Analyzer Optimizations**
   - Fast row count estimation (information_schema)
   - Adaptive batch sizing based on table characteristics
   - Intelligent extraction strategy selection
   - Performance category classification

2. **Connection Pooling**
   - Pool size: 20 connections
   - Max overflow: 40 connections
   - Pool timeout: 300 seconds
   - Connection recycling: 1800 seconds

### Connection Lifecycle Example

Understanding how connections are managed is crucial for performance optimization. Here's a complete trace of one table through the pipeline:

```python
# DAG runs process_large_tables task
# ↓
# Calls process_tables_by_category('large')
# ↓
# Creates PipelineOrchestrator
orchestrator = PipelineOrchestrator(environment=ENVIRONMENT)
orchestrator.initialize_connections()  # Validates, but doesn't hold connections

# ↓
# Processes table 'patient'
orchestrator.process_tables_by_performance_category(category='large', ...)

# ↓ Delegates to TableProcessor
# ↓ TableProcessor calls _extract_to_replication('patient')

# Extract Phase:
replicator = SimpleMySQLReplicator(settings)
# ├─ Borrows source connection from pool (connection #1 acquired)
# ├─ Borrows replication connection from pool (connection #2 acquired)
# ├─ Executes: SELECT * FROM patient ... (on source)
# ├─ Executes: INSERT INTO patient ... (on replication)
# ├─ Returns source connection to pool (connection #1 released)
# └─ Returns replication connection to pool (connection #2 released)

# Load Phase:
loader = PostgresLoader(settings)
# ├─ Borrows replication connection from pool (connection #3 acquired)
# ├─ Borrows analytics connection from pool (connection #4 acquired)
# ├─ Executes: SELECT * FROM patient ... (on replication)
# ├─ Executes: INSERT INTO patient ... (on analytics)
# ├─ Returns replication connection to pool (connection #3 released)
# └─ Returns analytics connection to pool (connection #4 released)

# Done! 4 acquisitions, 4 releases
# Physical connections: Still in pool, ready for next table
```

### Connection Acquisition Metrics

For a typical OpenDental deployment with **400 tables**:

**Fixed Overhead (Per DAG Run)**:
- Validation phase: 3 acquisitions (source, replication, analytics)
- Schema drift check: 1 acquisition (source)
- Verification phase: 2 acquisitions (replication, analytics)
- **Subtotal**: 6 acquisitions

**Per Table Processing**:
- Extract phase: 2 acquisitions (source, replication)
- Load phase: 2 acquisitions (replication, analytics)
- **Subtotal**: 4 acquisitions per table

**Total for 400 Tables**:
```
Fixed Overhead:          6 acquisitions
Per Table (400 × 4):  1,600 acquisitions
────────────────────────────────────────
Total:                1,606 acquisitions
```

**Physical Connections** (actual TCP sockets):
- Pool size: 20 persistent connections per database
- Max overflow: 40 additional connections if needed
- **Maximum per database**: 60 physical connections
- **For 3 databases**: Up to 180 max physical connections
- **Typical usage**: 20-40 active connections (well within limits)

### Parallel Processing Impact

**Sequential Processing** (Medium/Small/Tiny Tables):
```
Time ──────────────────────────────────────►

Table 1: [Source + Repl]──[Repl + Analytics]
                                            Table 2: [Source + Repl]──[Repl + Analytics]
                                                                                        Table 3: ...

Peak concurrent connections: 4
```

**Parallel Processing** (Large Tables, max_workers=5):
```
Time ──────────────────────────────────────►

Table 1: [Source + Repl]──[Repl + Analytics]
Table 2: [Source + Repl]──[Repl + Analytics]
Table 3: [Source + Repl]──[Repl + Analytics]
Table 4: [Source + Repl]──[Repl + Analytics]
Table 5: [Source + Repl]──[Repl + Analytics]

Peak concurrent connections: 20
(5 tables × 2 connections during extract phase)
or
(5 tables × 2 connections during load phase)
```

**Key Insight**: Even with 1,606 connection acquisitions, only 20-60 physical TCP connections are actually created due to connection pooling and reuse.

3. **Database Optimizations**
   - **MySQL**: Bulk insert buffers (256MB), disabled constraints during load
   - **PostgreSQL**: Increased work_mem (256MB), maintenance_work_mem (1GB)

4. **Parallel Processing**
   - Large tables processed in parallel (5 workers default)
   - ThreadPoolExecutor for concurrent execution
   - Resource-aware worker management

5. **Incremental Loading**
   - Change data capture using timestamp columns
   - Minimal data transfer for unchanged tables
   - Last-value tracking for continuous updates

### Expected Performance

- **Tiny tables** (<10K rows): < 0.1 minutes
- **Small tables** (10K-100K rows): 0.1-1 minutes
- **Medium tables** (100K-1M rows): 1-10 minutes
- **Large tables** (>1M rows): 10+ minutes

**Throughput**:
- Small tables: ~500-1,000 records/second
- Medium tables: ~1,000-2,000 records/second
- Large tables: ~2,000-3,000 records/second

## Usage Examples

### Running the Complete Pipeline

```bash
# Run full pipeline (all tables)
python -m etl_pipeline.cli run --all

# Run specific table
python -m etl_pipeline.cli run --table patient

# Force full refresh
python -m etl_pipeline.cli run --table patient --force-full

# Run by performance category
python -m etl_pipeline.cli run --category large

# Run with parallel workers
python -m etl_pipeline.cli run --all --max-workers 10
```

### Analyzing Schema

```bash
# Analyze schema and generate tables.yml
python etl_pipeline/scripts/analyze_opendental_schema.py

# Review schema changes
cat logs/schema_analysis/reports/schema_changelog_*.md
```

### Environment Setup

```bash
# Set environment
export ETL_ENVIRONMENT=production  # or 'test'

# Validate configuration
python -m etl_pipeline.cli validate

# Test connections
python -m etl_pipeline.cli test-connections
```

## Best Practices

1. **Schema Analysis**
   - Run `analyze_opendental_schema.py` after schema changes
   - Review schema changelog before running pipeline
   - Keep tables.yml under version control

2. **Configuration Management**
   - Use separate `.env_production` and `.env_test` files
   - Never commit .env files to version control
   - Validate configuration before running pipeline

3. **Incremental Loading**
   - Use incremental loading for large, frequently updated tables
   - Force full refresh periodically for data integrity
   - Monitor incremental column data quality

4. **Performance Tuning**
   - Adjust batch_size in tables.yml for optimal performance
   - Monitor processing times and adjust worker count
   - Use parallel processing for large tables

5. **Error Recovery**
   - Review failed tables in logs
   - Re-run failed tables individually
   - Investigate root cause before forcing full refresh

6. **Monitoring**
   - Check tracking tables (etl_copy_status, etl_load_status)
   - Monitor performance metrics
   - Set up alerts for critical table failures

## Troubleshooting

### Common Issues

1. **Configuration Errors**
   - **Symptom**: Pipeline fails to initialize
   - **Solution**: Validate environment variables, check `.env_*` files
   - **Command**: `python -m etl_pipeline.cli validate`

2. **Connection Failures**
   - **Symptom**: "Database connection failed"
   - **Solution**: Check network connectivity, database credentials, firewall
   - **Command**: `python -m etl_pipeline.cli test-connections`

3. **Schema Mismatches**
   - **Symptom**: "Column not found" or type conversion errors
   - **Solution**: Re-run `analyze_opendental_schema.py`, review schema changelog
   - **Action**: Update tables.yml with new schema

4. **Performance Issues**
   - **Symptom**: Slow processing times
   - **Solution**: 
     - Increase batch_size for large tables
     - Enable parallel processing
     - Review connection pool settings
     - Check database performance

5. **Incremental Loading Issues**
   - **Symptom**: Missing recent data
   - **Solution**: 
     - Check incremental_columns configuration
     - Verify last processed values in tracking tables
     - Force full refresh if necessary

## Conclusion

This ETL pipeline provides a robust, performant, and maintainable solution for replicating OpenDental data to PostgreSQL for analytics. The architecture emphasizes:

- **Separation of Concerns**: Clear boundaries between configuration, extraction, loading, and orchestration
- **Environment Safety**: Separate production and test environments with fail-fast validation
- **Performance**: Optimized batch sizing, parallel processing, and intelligent strategy selection
- **Reliability**: Comprehensive error handling, automatic retry, and graceful degradation
- **Maintainability**: Well-documented code, comprehensive logging, and tracking
- **Flexibility**: Configurable strategies, batch sizes, and processing priorities

For questions or issues, refer to the codebase documentation or consult the development team.

