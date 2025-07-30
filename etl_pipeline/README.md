# OpenDental ETL Pipeline

A modern, comprehensive ETL pipeline ecosystem for extracting data from OpenDental MySQL, replicating it locally, and loading it into a PostgreSQL analytics database with intelligent orchestration, priority-based processing, and automated management tools.

## Architecture Overview

The pipeline follows a **two-phase architecture** with clear separation between **management/setup** and **nightly ETL execution**:

### Phase 1: Pipeline Management & Setup
```
┌─────────────────────┐    ┌─────────────────────┐    ┌─────────────────────┐
│   Schema Analysis   │    │   Test Environment  │    │   Configuration     │
│   & Discovery       │    │   Setup             │    │   Management        │
│                     │    │                     │    │                     │
│ • analyze_opendental│    │ • setup_test_       │    │ • update_pipeline_  │
│   _schema.py        │    │   databases.py      │    │   config.py         │
│ • Dynamic discovery │    │ • Test database     │    │ • Config validation │
│ • tables.yml gen    │    │   creation          │    │ • Settings mgmt     │
│ • Analysis reports  │    │ • Sample data       │    │ • Version control   │
└─────────────────────┘    └─────────────────────┘    └─────────────────────┘
           │                         │                         │
           └─────────────────────────┼─────────────────────────┘
                                     ▼
                        ┌─────────────────────┐
                        │   Static Config     │
                        │   Outputs           │
                        │                     │
                        │ • tables.yml        │
                        │ • Test databases    │
                        │ • Config backups    │
                        └─────────────────────┘
```

### Phase 2: Nightly ETL Execution
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
                        │ • Static Config     │ ← Modern Configuration
                        └─────────────────────┘
```

## ETL Pipeline Architecture

### SimpleMySQLReplicator

The replicator uses two complementary systems:

#### Copy Methods (Performance-Based)
Determine **HOW** to copy data based on table size:
- `small`: Direct INSERT...SELECT (< 1MB)
- `medium`: Chunked with LIMIT/OFFSET (1-100MB)  
- `large`: Progress-tracked chunks (> 100MB)

#### Extraction Strategies (Business Logic)
Determine **WHAT** to copy based on configuration:
- `full_table`: Complete table replacement
- `incremental`: Copy only new/changed records
- `incremental_chunked`: Small batches for large incremental tables

## Core Components

### **Phase 1: Management & Setup Components** (`scripts/`)
Pipeline management and configuration tools (outside nightly ETL scope):

- **`analyze_opendental_schema.py`**: **Schema Analysis Tool** - Dynamic schema discovery and `tables.yml` generation (4 methods)
- **`setup_test_databases.py`**: **Test Environment Setup** - Automated test database creation and sample data loading (8 functions)
- **`update_pipeline_config.py`**: **Configuration Management** - Settings validation, updates, and version control (13 methods)
- **`analyze_connection_usage.py`**: **Connection Analysis** - Database connection usage analysis and optimization (15 methods)
- **`setup_environments.py`**: **Environment Setup** - Automated environment configuration and validation (5 methods)

### **Phase 2: Nightly ETL Components**

#### 1. **Orchestration Layer** (`orchestration/`)
The brain of the pipeline that coordinates all operations:

- **`PipelineOrchestrator`**: Main coordinator that manages the entire pipeline lifecycle (6 methods)
- **`TableProcessor`**: Handles individual table ETL processing (extract → load → transform) (9 methods)
- **`PriorityProcessor`**: Manages batch processing with intelligent parallelization for critical tables (5 methods)

#### 2. **Core Components** (`core/`)
Essential pipeline building blocks with **modern static configuration**:

- **`simple_mysql_replicator.py`**: **SimpleMySQLReplicator** - Efficient MySQL-to-MySQL replication with clear separation of copy methods and extraction strategies (10 methods)
- **`connections.py`**: **ConnectionFactory** - Explicit environment separation and connection management (25 methods)
- **`postgres_schema.py`**: **PostgresSchema** - Schema conversion and adaptation utilities (10 methods)

#### 3. **Data Loading** (`loaders/`)
PostgreSQL data loading with optimization:

- **`postgres_loader.py`**: **PostgresLoader** - MySQL-to-PostgreSQL loading with chunked processing for large tables (11 methods)

#### 4. **Configuration** (`config/`)
**Modern, static configuration management with provider pattern**:

- **`settings.py`**: **Settings** - Centralized configuration management with environment separation and provider pattern (20 methods)
- **`config_reader.py`**: **ConfigReader** - Static configuration from `tables.yml` (12 methods)
- **`providers.py`**: **ConfigProvider** - Provider pattern for configuration injection and testing (2 providers)
- **`logging.py`**: Advanced logging configuration (15 methods)
- **`pipeline.yml`**: Pipeline configuration with connection overrides and settings

#### 5. **Command Line Interface** (`cli/`)
User-friendly command interface:

- **`commands.py`**: CLI commands for pipeline operations (3 main commands)
- **`main.py`**: CLI entry point with Click-based interface
- **`__main__.py`**: Module entry point for `python -m etl_pipeline`

#### 6. **Monitoring** (`monitoring/`)
Observability and metrics:

- **`unified_metrics.py`**: **UnifiedMetricsCollector** - Comprehensive metrics collection and persistence (15 methods)

#### 7. **Exception Handling** (`exceptions/`)
Structured error handling:

- **`base.py`**: Base exception classes and error handling utilities
- **`configuration.py`**: Configuration and environment-related exceptions
- **`database.py`**: Database connection and transaction exceptions
- **`data.py`**: Data extraction and loading exceptions
- **`schema.py`**: Schema-related exceptions

### **Data Transformation** (External)
Schema and data transformation handled by dbt:

- **dbt Models**: Business logic and analytics transformations
- **Staging Models**: Raw data standardization and cleaning
- **Intermediate Models**: Business logic implementation
- **Mart Models**: Final analytics-ready datasets

## Data Flow

### **Phase 1: Pipeline Management & Setup**
```
Schema Analysis → Configuration Generation → Test Environment Setup
```

**Management Scripts:**
- **`analyze_opendental_schema.py`**: Dynamic schema discovery → `tables.yml` generation
- **`setup_test_databases.py`**: Test environment creation → Sample data loading
- **`update_pipeline_config.py`**: Configuration validation → Settings management
- **`analyze_connection_usage.py`**: Connection analysis → Optimization recommendations
- **`setup_environments.py`**: Environment setup → Configuration validation

**Outputs:**
- `tables.yml` - Static configuration for nightly ETL
- Test databases with sample data for integration testing
- Configuration validation and backup
- Connection usage analysis and optimization

### **Phase 2: Nightly ETL Execution**

#### 1. **Extract Phase**
```
Source MySQL → SimpleMySQLReplicator → Replication MySQL
```
- **Static configuration** from `tables.yml` (no dynamic discovery)
- Efficient MySQL-to-MySQL replication
- Intelligent incremental vs full refresh
- Batch copying for large tables

#### 2. **Load Phase**
```
Replication MySQL → PostgresLoader → PostgreSQL (raw schema)
```
- Optimized loading with chunked processing for large tables
- Automatic type conversion and schema adaptation
- Connection pooling and resource management

#### 3. **Transform Phase**
```
PostgreSQL (raw) → dbt → PostgreSQL (staging/intermediate/marts)
```
- Business logic and analytics transformations
- Data standardization and cleaning via dbt staging models
- Business logic implementation via dbt intermediate models
- Final analytics-ready datasets via dbt mart models

#### 4. **Orchestration**
```
PipelineOrchestrator → PriorityProcessor → TableProcessor → Static Config
```
- **Static configuration** provides ALL table information to all components
- Priority-based table processing (critical → important → audit → reference)
- Parallel processing for critical tables
- Sequential processing for resource management
- Comprehensive error handling and recovery

## Key Features

### **Two-Phase Architecture**
- **Management Phase**: Automated schema analysis, configuration generation, and test environment setup
- **Nightly ETL Phase**: Efficient data extraction, loading, and transformation using static configuration
- **Clear Separation**: Management tools run independently of nightly ETL operations
- **Automated Setup**: One-command schema analysis and configuration generation

### **Modern Provider Pattern Configuration**
- **Provider Pattern**: `FileConfigProvider` and `DictConfigProvider` for flexible configuration injection
- **Environment-Specific Files**: `.env_production` and `.env_test` for environment separation
- **Fail-Fast Validation**: Comprehensive validation with detailed error reporting
- **Testing Support**: `DictConfigProvider` enables easy testing with injected configurations

### **Static Configuration Benefits**
- **Static Configuration**: All table information from `tables.yml` - no dynamic discovery during ETL
- **5-10x Performance**: Faster than dynamic schema discovery approaches
- **Predictable Performance**: Consistent execution times with static configuration
- **No Legacy Code**: All compatibility methods removed for clean architecture

### **Intelligent Processing**
- **Priority-Based**: Tables processed by importance (critical, important, audit, reference)
- **Parallel Processing**: Critical tables processed in parallel for speed
- **Resource Management**: Sequential processing for non-critical tables
- **Incremental Loading**: Smart detection of changes for efficient processing

### **Explicit Environment Separation**
- **Production/Test**: Clear separation between production and test environments
- **Connection Management**: Explicit connection handling for each environment
- **Test Automation**: Automated test database setup and sample data loading
- **Configuration Validation**: Comprehensive validation for both environments

### **Comprehensive Management Tools**
- **Schema Analysis**: Dynamic discovery and static configuration generation
- **Test Environment**: Automated setup with sample data and validation
- **Configuration Management**: Validation, updates, and version control
- **Connection Analysis**: Usage analysis and optimization recommendations
- **Integration Testing**: Complete test environment for validation

### **Robust Error Handling**
- **Structured Exceptions**: Comprehensive exception hierarchy for different error types
- **Comprehensive Logging**: Detailed logs for debugging and monitoring
- **Graceful Degradation**: Failures don't stop the entire pipeline
- **Resource Cleanup**: Automatic cleanup of connections and resources
- **Context Managers**: Proper resource lifecycle management

### **Modern CLI Interface**
- **Click-Based**: Modern CLI framework with command groups
- **Multiple Commands**: `run`, `status`, `test_connections` with comprehensive options
- **Dry Run Mode**: Preview pipeline operations without making changes
- **Real-Time Status**: Monitor pipeline progress and status
- **Connection Testing**: Validate database connections before execution

### **Modern Configuration**
- **Environment Variables**: Secure configuration through environment variables
- **YAML Configuration**: Human-readable table and pipeline configurations
- **Priority Management**: Table importance levels for intelligent processing
- **Flexible Settings**: Support for custom configurations and overrides
- **Provider Pattern**: Dependency injection for testing and flexibility

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

### **Phase 1: Initial Setup**

#### 1. **Environment Configuration**

Copy the template and configure your environment:
```bash
cp .env_production.template .env_production
cp .env_test.template .env_test
```

Required environment variables:
```bash
# Environment Selection
ETL_ENVIRONMENT=production  # or 'test'

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
POSTGRES_ANALYTICS_SCHEMA=raw
POSTGRES_ANALYTICS_USER=analytics_user
POSTGRES_ANALYTICS_PASSWORD=your_password
```

#### 2. **Database Setup**

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

#### 3. **Install Dependencies**

```bash
pip install -r requirements.txt
```

### **Phase 2: Pipeline Management Setup**

#### 4. **Generate Schema Configuration**

**Single command to generate complete schema analysis and configuration**:
```bash
python etl_pipeline/scripts/analyze_opendental_schema.py
```

This generates:
- `etl_pipeline/config/tables.yml` - Complete table configurations
- Detailed analysis reports and logs
- Summary statistics and recommendations

#### 5. **Setup Test Environment**

**Automated test environment setup**:
```bash
python etl_pipeline/scripts/setup_test_databases.py
```

This creates:
- Test databases with sample data
- Integration testing environment
- Validation fixtures for testing

#### 6. **Validate Configuration**

**Configuration validation and management**:
```bash
python etl_pipeline/scripts/update_pipeline_config.py
```

This provides:
- Configuration validation
- Settings management
- Version control and backup

#### 7. **Analyze Connection Usage**

**Connection analysis and optimization**:
```bash
python etl_pipeline/scripts/analyze_connection_usage.py
```

This provides:
- Connection usage analysis
- Performance optimization recommendations
- Resource utilization insights

## Usage

### **Phase 1: Pipeline Management**

#### **Schema Analysis and Configuration**
```bash
# Generate complete schema analysis and configuration
python etl_pipeline/scripts/analyze_opendental_schema.py

# Update and validate configuration
python etl_pipeline/scripts/update_pipeline_config.py

# Analyze connection usage and performance
python etl_pipeline/scripts/analyze_connection_usage.py
```

#### **Test Environment Management**
```bash
# Setup test environment with sample data
python etl_pipeline/scripts/setup_test_databases.py

# Validate test environment
python etl_pipeline/scripts/update_pipeline_config.py --validate-test
```

### **Phase 2: Nightly ETL Execution**

#### **Command Line Interface**

```bash
# Process all tables by priority
python -m etl_pipeline run

# Process specific tables
python -m etl_pipeline run --tables patient appointment

# Process with custom configuration
python -m etl_pipeline run --config custom_config.yml

# Force full refresh
python -m etl_pipeline run --force

# Dry run mode (preview without changes)
python -m etl_pipeline run --dry-run

# Show pipeline status
python -m etl_pipeline status

# Test database connections
python -m etl_pipeline test-connections
```

#### **Programmatic Usage**

```python
from etl_pipeline.orchestration import PipelineOrchestrator

# Initialize and run pipeline with static configuration
with PipelineOrchestrator() as orchestrator:
    orchestrator.initialize_connections()
    
    # Process single table
    success = orchestrator.run_pipeline_for_table('patient')
    
    # Process tables by priority
    results = orchestrator.process_tables_by_priority(
        importance_levels=['critical', 'important'],
        max_workers=5
    )
```

#### **Configuration Management**

```python
from etl_pipeline.config.settings import Settings
from etl_pipeline.config.config_reader import ConfigReader

# Access configuration
settings = Settings()
config_reader = ConfigReader("etl_pipeline/config/tables.yml")

# Get table configuration
table_config = config_reader.get_table_config('patient')
critical_tables = config_reader.get_tables_by_importance('critical')
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
├── loaders/          # Data loading components
├── config/           # Configuration management with provider pattern
├── cli/              # Command line interface (Click-based)
├── monitoring/       # Monitoring and metrics
├── exceptions/       # Structured exception handling
├── scripts/          # Schema analysis and management scripts
├── tests/            # Comprehensive test suite
│   ├── unit/         # Unit tests
│   ├── integration/  # Integration tests
│   ├── e2e/          # End-to-end tests
│   ├── comprehensive/ # Comprehensive test scenarios
│   └── fixtures/     # Test fixtures and utilities
└── docs/             # Documentation
```

### **Testing Strategy**
- **Unit Tests**: Individual component testing with mocked dependencies
- **Integration Tests**: End-to-end pipeline testing with real databases
- **Performance Tests**: Large dataset processing validation
- **Error Handling Tests**: Failure scenario validation
- **Configuration Tests**: Provider pattern and environment testing

### **Adding New Tables**
1. Tables are automatically detected from source database by schema analysis
2. Configuration in `tables.yml` defines processing behavior
3. Priority levels determine processing order and strategy
4. **No code changes required** - Schema analysis handles everything automatically
5. **dbt Models**: Transformations are handled by dbt models (staging → intermediate → marts)

## Performance Optimization

### **Static Configuration Benefits**
- **5-10x faster** than dynamic schema discovery approaches
- **No database queries** during ETL operations for schema information
- **Predictable performance** with consistent execution times
- **Reduced resource usage** with no dynamic discovery overhead

### **Provider Pattern Benefits**
- **Flexible Configuration**: Easy testing with `DictConfigProvider`
- **Environment Separation**: Clear production/test configuration
- **Dependency Injection**: Clean architecture for testing and flexibility
- **Fail-Fast Validation**: Comprehensive validation with detailed errors

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
- Explicit environment separation for production/test

## Troubleshooting

### **Common Issues**

1. **Connection Problems**
   - Verify environment variables in `.env_production` or `.env_test`
   - Check database permissions
   - Validate network connectivity
   - Use `python -m etl_pipeline test-connections`

2. **Processing Failures**
   - Check logs for specific error messages
   - Verify table configurations in `tables.yml`
   - Review database schema compatibility
   - Use dry-run mode: `python -m etl_pipeline run --dry-run`

3. **Performance Issues**
   - Monitor worker count and parallel processing
   - Check batch sizes for large tables
   - Review database indexes and query optimization
   - Run connection analysis: `python etl_pipeline/scripts/analyze_connection_usage.py`

4. **Configuration Issues**
   - Run `analyze_opendental_schema.py` to regenerate configurations
   - Use `update_pipeline_config.py` to validate settings
   - Check `tables.yml` for configuration errors
   - Validate environment files with `setup_environments.py`

5. **Test Environment Issues**
   - Run `setup_test_databases.py` to recreate test environment
   - Validate test configuration with `update_pipeline_config.py --validate-test`
   - Check test database connectivity and permissions

### **Debugging Tools**
- **Detailed Logging**: Comprehensive log output for debugging
- **Status Commands**: `python -m etl_pipeline status` for pipeline status
- **Configuration Validation**: Settings validation and error reporting
- **Schema Analysis Reports**: Detailed analysis from schema analysis script
- **Test Environment Validation**: Automated test environment setup and validation
- **Connection Testing**: `python -m etl_pipeline test-connections`
- **Dry Run Mode**: Preview operations without making changes

## Ecosystem Statistics

### **Complete Component Overview**
- **Total Components**: 18 (13 nightly ETL + 5 management scripts)
- **Total Methods**: 173 (148 nightly ETL + 25 management)
- **Architecture**: Modern, clean, with clear separation of concerns
- **Performance**: Optimized for both setup efficiency and nightly execution speed

### **Component Breakdown**
| Phase | Components | Methods | Purpose |
|-------|------------|---------|---------|
| **Management** | 5 scripts | 25 methods | Schema analysis, test setup, config management, connection analysis |
| **Nightly ETL** | 13 components | 148 methods | Data extraction, loading, orchestration, monitoring, exceptions |
| **Total** | **18** | **173** | **Complete pipeline ecosystem** |

## Contributing

1. **Code Style**: Follow existing patterns and conventions
2. **Testing**: Add tests for new features and changes
3. **Documentation**: Update documentation for any changes
4. **Review Process**: Submit pull requests for review
5. **Architecture**: Maintain clear separation between management and nightly ETL phases
6. **Provider Pattern**: Use dependency injection for testing and flexibility

## License

MIT License - See LICENSE file for details 