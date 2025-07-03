# OpenDental ETL Pipeline

A modern, simplified ETL pipeline for extracting data from OpenDental MySQL, replicating it locally, and loading it into a PostgreSQL analytics database with intelligent orchestration and priority-based processing.

## Architecture Overview

The pipeline follows a simplified three-database architecture with intelligent orchestration and **single-source-of-truth schema analysis**:

```
┌─────────────────────┐    ┌─────────────────────┐    ┌─────────────────────────────────────────────┐
│   Source MySQL      │    │   Replication MySQL │    │   Analytics PostgreSQL                       │
│   (OpenDental)      │───▶│   (Local Copy)      │───▶│   (Analytics)                               │
│                     │    │                     │    │                                              │
│ - opendental        │    │ - opendental_repl   │    │ - opendental_analytics                      │
│ - Read-only access  │    │ - Full read/write   │    │ - Schemas:                                  │
│ - Port 3306         │    │ - Port 3305         │    │   • raw (ETL output)                        │
│                     │    │                     │    │   • staging (dbt staging)                   │
│                     │    │                     │    │   • intermediate (dbt intermediate)         │
│                     │    │                     │    │   • marts (dbt marts)                       │
└─────────────────────┘    └─────────────────────┘    └─────────────────────────────────────────────┘
                                    ▲
                                    │
                        ┌─────────────────────┐
                        │   Orchestration     │
                        │   Layer             │
                        │                     │
                        │ • PipelineOrchestrator │
                        │ • TableProcessor    │
                        │ • PriorityProcessor │
                        │ • SchemaDiscovery   │ ← Single Source of Truth
                        └─────────────────────┘
```

## Core Components

### 1. **Orchestration Layer** (`orchestration/`)
The brain of the pipeline that coordinates all operations:

- **`PipelineOrchestrator`**: Main coordinator that manages the entire pipeline lifecycle
- **`TableProcessor`**: Handles individual table ETL processing (extract → load → transform) with **mandatory SchemaDiscovery dependency**
- **`PriorityProcessor`**: Manages batch processing with intelligent parallelization for critical tables, **requires SchemaDiscovery for TableProcessor creation**

### 2. **Core Components** (`core/`)
Essential pipeline building blocks with **single-source-of-truth architecture**:

- **`schema_discovery.py`**: **Enhanced SchemaDiscovery** - **THE single source of truth** for ALL MySQL schema analysis, configuration generation, and replication
- **`mysql_replicator.py`**: **Cleaned ExactMySQLReplicator** - Pure replication executor using SchemaDiscovery for ALL schema operations
- **`connections.py`**: ConnectionFactory and ConnectionManager for efficient database connection management
- **`postgres_schema.py`**: Schema management and conversion utilities
- **`exceptions.py`**: Custom exception classes for error handling

### 3. **Data Loading** (`loaders/`)
PostgreSQL data loading with optimization:

- **`postgres_loader.py`**: PostgresLoader for MySQL-to-PostgreSQL loading with chunked processing for large tables (**updated for simplified configuration structure**)

### 4. **Data Transformation** (`transformers/`)
Schema and data transformation:

- **dbt Models**: Business logic and analytics transformations handled by dbt
- **Staging Models**: Raw data standardization and cleaning
- **Intermediate Models**: Business logic implementation
- **Mart Models**: Final analytics-ready datasets

### 5. **Configuration** (`config/`)
**Simplified, modern configuration management**:

- **`settings.py`**: **Clean Settings class** - centralized configuration management with **single-section tables.yml structure**
- **`logging.py`**: Advanced logging configuration
- **`pipeline.yml`**: Pipeline configuration
- **`tables.yml`**: **Simplified table configurations** with priority levels and comprehensive metadata

### 6. **Command Line Interface** (`cli/`)
User-friendly command interface:

- **`commands.py`**: CLI commands for pipeline operations
- **`main.py`**: CLI entry point

### 7. **Monitoring** (`monitoring/`)
Observability and metrics:

- **`unified_metrics.py`**: UnifiedMetricsCollector for comprehensive metrics collection

### 8. **Schema Analysis** (`scripts/`)
**Single source of truth for schema analysis**:

- **`analyze_opendental_schema.py`**: **THE main schema analysis script** - generates complete `tables.yml` configuration using SchemaDiscovery

## Data Flow

### 1. **Extract Phase**
```
Source MySQL → SchemaDiscovery → ExactMySQLReplicator → Replication MySQL
```
- **Single SchemaDiscovery instance** performs ALL schema analysis
- Exact schema replication with validation
- Intelligent incremental vs full refresh
- Schema change detection and handling

### 2. **Load Phase**
```
Replication MySQL → PostgresLoader → PostgreSQL (raw schema)
```
- Optimized loading with chunked processing for large tables
- Automatic type conversion and schema adaptation
- Connection pooling and resource management

### 3. **Transform Phase**
```
PostgreSQL (raw) → dbt → PostgreSQL (staging/intermediate/marts)
```
- Business logic and analytics transformations
- Data standardization and cleaning via dbt staging models
- Business logic implementation via dbt intermediate models
- Final analytics-ready datasets via dbt mart models

### 4. **Orchestration**
```
PipelineOrchestrator → PriorityProcessor → TableProcessor → SchemaDiscovery
```
- **SchemaDiscovery provides ALL schema information** to all components
- Priority-based table processing (critical → important → audit → reference)
- Parallel processing for critical tables
- Sequential processing for resource management
- Comprehensive error handling and recovery

## Key Features

### **Single Source of Truth Architecture**
- **SchemaDiscovery**: ALL schema analysis happens in one place
- **No Duplicate Code**: Eliminated duplicate schema analysis across components
- **Simplified Configuration**: Single `tables.yml` structure with comprehensive metadata
- **Explicit Dependencies**: All components require SchemaDiscovery for schema operations

### **Intelligent Processing**
- **Priority-Based**: Tables processed by importance (critical, important, audit, reference)
- **Parallel Processing**: Critical tables processed in parallel for speed
- **Resource Management**: Sequential processing for non-critical tables
- **Incremental Loading**: Smart detection of changes for efficient processing

### **Simplified Architecture**
- **Reduced Complexity**: Eliminated over-engineering and unnecessary abstraction layers
- **Efficient Configuration**: Centralized Settings class with caching
- **Streamlined Connections**: Automatic connection management and cleanup
- **Clear Separation**: Each component has a single, well-defined responsibility

### **Robust Error Handling**
- **Comprehensive Logging**: Detailed logs for debugging and monitoring
- **Graceful Degradation**: Failures don't stop the entire pipeline
- **Resource Cleanup**: Automatic cleanup of connections and resources
- **Context Managers**: Proper resource lifecycle management

### **Modern Configuration**
- **Environment Variables**: Secure configuration through environment variables
- **YAML Configuration**: Human-readable table and pipeline configurations
- **Priority Management**: Table importance levels for intelligent processing
- **Flexible Settings**: Support for custom configurations and overrides

## Database Architecture

### **Source MySQL (OpenDental)**
- Production OpenDental database
- Read-only access for safety
- Used for initial data extraction

### **Replication MySQL**
- Local MySQL database for staging
- Exact copy of source data
- Used for validation and intermediate processing

### **Analytics PostgreSQL**
- Multi-schema analytics data warehouse:
  - **`raw`**: ETL pipeline output (exact replica of source)
  - **`staging`**: dbt staging models (data standardization and cleaning)
  - **`intermediate`**: dbt intermediate models (business logic implementation)
  - **`marts`**: dbt final models (analytics-ready datasets for consumption)

## Setup and Configuration

### 1. **Environment Configuration**

Copy the template and configure your environment:
```bash
cp .env.template .env
```

Required environment variables:
```bash
# Source Database (OpenDental Production)
OPENDENTAL_SOURCE_HOST=client-server
OPENDENTAL_SOURCE_PORT=3306
OPENDENTAL_SOURCE_DB=opendental
OPENDENTAL_SOURCE_USER=readonly_user
OPENDENTAL_SOURCE_PASSWORD=your_password

# Replication Database (Local MySQL)
MYSQL_REPLICATION_HOST=localhost
MYSQL_REPLICATION_PORT=3305
MYSQL_REPLICATION_DB=opendental_repl
MYSQL_REPLICATION_USER=replication_user
MYSQL_REPLICATION_PASSWORD=your_password

# Analytics Database (PostgreSQL)
POSTGRES_ANALYTICS_HOST=localhost
POSTGRES_ANALYTICS_PORT=5432
POSTGRES_ANALYTICS_DB=opendental_analytics
POSTGRES_ANALYTICS_USER=analytics_user
POSTGRES_ANALYTICS_PASSWORD=your_password
```

### 2. **Database Setup**

```sql
-- MySQL Replication Database
CREATE DATABASE opendental_repl;
GRANT ALL PRIVILEGES ON opendental_repl.* TO 'replication_user'@'localhost';

-- PostgreSQL Analytics Database
CREATE DATABASE opendental_analytics;
CREATE SCHEMA raw;
CREATE SCHEMA staging;
CREATE SCHEMA intermediate;
CREATE SCHEMA marts;

GRANT ALL PRIVILEGES ON DATABASE opendental_analytics TO analytics_user;
GRANT ALL PRIVILEGES ON SCHEMA raw TO analytics_user;
GRANT ALL PRIVILEGES ON SCHEMA staging TO analytics_user;
GRANT ALL PRIVILEGES ON SCHEMA intermediate TO analytics_user;
GRANT ALL PRIVILEGES ON SCHEMA marts TO analytics_user;
```

### 3. **Install Dependencies**

```bash
pip install -r requirements.txt
```

### 4. **Generate Schema Configuration**

**Single command to generate complete schema analysis and configuration**:
```bash
python etl_pipeline/scripts/analyze_opendental_schema.py
```

This generates:
- `etl_pipeline/config/tables.yml` - Complete table configurations
- Detailed analysis reports and logs
- Summary statistics and recommendations

## Usage

### **Command Line Interface**

```bash
# Process all tables by priority
python -m etl_pipeline process-all

# Process specific table
python -m etl_pipeline process-table patient

# Process tables by importance level
python -m etl_pipeline process-priority critical

# Force full refresh
python -m etl_pipeline process-table patient --force-full

# Show pipeline status
python -m etl_pipeline status
```

### **Programmatic Usage**

```python
from etl_pipeline.orchestration import PipelineOrchestrator
from etl_pipeline.core.schema_discovery import SchemaDiscovery

# Initialize SchemaDiscovery (single source of truth)
schema_discovery = SchemaDiscovery(source_engine, source_db)

# Initialize and run pipeline with SchemaDiscovery
with PipelineOrchestrator(schema_discovery=schema_discovery) as orchestrator:
    orchestrator.initialize_connections()
    
    # Process single table
    success = orchestrator.run_pipeline_for_table('patient')
    
    # Process tables by priority
    results = orchestrator.process_tables_by_priority(
        importance_levels=['critical', 'important'],
        max_workers=5
    )
```

### **Configuration Management**

```python
from etl_pipeline.config.settings import Settings

# Access configuration
settings = Settings()
table_config = settings.get_table_config('patient')
critical_tables = settings.get_tables_by_importance('critical')
```

## Monitoring and Observability

### **Logging**
- **Pipeline Logs**: `etl_pipeline.log` - Main pipeline operations
- **Connection Logs**: `connection.log` - Database connection events
- **Schema Analysis Logs**: `schema_analysis_YYYYMMDD_HHMMSS.log` - Schema discovery and configuration generation
- **Error Logs**: Detailed error tracking and debugging information

### **Metrics Collection**
- **Processing Times**: Table-level processing duration
- **Success Rates**: Success/failure tracking by table and priority
- **Resource Usage**: Connection pool and memory utilization
- **Performance Metrics**: Throughput and efficiency measurements

### **Status Tracking**
- **Table Status**: Current processing status for each table
- **Priority Progress**: Progress tracking by importance level
- **Error Reporting**: Detailed error information and recovery status

## Development and Testing

### **Code Structure**
```
etl_pipeline/
├── orchestration/     # Pipeline orchestration and coordination
├── core/             # Core pipeline components and utilities
│   └── schema_discovery.py  # ← Single source of truth for schema analysis
├── loaders/          # Data loading components
├── transformers/     # Data transformation components
├── config/           # Configuration management
├── cli/              # Command line interface
├── monitoring/       # Monitoring and metrics
├── scripts/          # Schema analysis scripts
│   └── analyze_opendental_schema.py  # ← Main schema analysis script
├── tests/            # Comprehensive test suite
└── docs/             # Documentation
```

### **Testing Strategy**
- **Unit Tests**: Individual component testing with SchemaDiscovery mocking
- **Integration Tests**: End-to-end pipeline testing with real SchemaDiscovery
- **Performance Tests**: Large dataset processing validation
- **Error Handling Tests**: Failure scenario validation

### **Adding New Tables**
1. Tables are automatically detected from source database by SchemaDiscovery
2. Configuration in `tables.yml` defines processing behavior
3. Priority levels determine processing order and strategy
4. **No code changes required** - SchemaDiscovery handles everything automatically
5. **dbt Models**: Transformations are handled by dbt models (staging → intermediate → marts)

## Performance Optimization

### **Single Schema Analysis**
- **SchemaDiscovery runs once** per pipeline execution
- **Comprehensive caching** for all schema information
- **Batch analysis** for efficiency
- **No duplicate queries** across components

### **Parallel Processing**
- Critical tables processed in parallel for speed
- Configurable worker count for resource management
- Intelligent load balancing across available resources

### **Chunked Loading**
- Large tables (>1M rows or >100MB) use chunked loading
- Memory-efficient processing for large datasets
- Configurable batch sizes for optimal performance

### **Connection Management**
- Connection pooling for efficient resource usage
- Automatic cleanup and resource management
- Context manager support for safe resource handling

## Troubleshooting

### **Common Issues**

1. **Connection Problems**
   - Verify environment variables
   - Check database permissions
   - Validate network connectivity

2. **Processing Failures**
   - Check logs for specific error messages
   - Verify table configurations in `tables.yml`
   - Review database schema compatibility

3. **Performance Issues**
   - Monitor worker count and parallel processing
   - Check batch sizes for large tables
   - Review database indexes and query optimization

4. **Schema Analysis Issues**
   - Run `analyze_opendental_schema.py` to regenerate configurations
   - Check SchemaDiscovery logs for analysis errors
   - Verify source database connectivity

### **Debugging Tools**
- **Detailed Logging**: Comprehensive log output for debugging
- **Status Commands**: CLI commands for pipeline status
- **Configuration Validation**: Settings validation and error reporting
- **Schema Analysis Reports**: Detailed analysis from SchemaDiscovery

## Contributing

1. **Code Style**: Follow existing patterns and conventions
2. **Testing**: Add tests for new features and changes
3. **Documentation**: Update documentation for any changes
4. **Review Process**: Submit pull requests for review

## License

MIT License - See LICENSE file for details 