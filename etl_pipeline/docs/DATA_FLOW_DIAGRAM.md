# ETL Pipeline Data Flow Diagram

## Complete Pipeline Ecosystem

This diagram shows the complete ETL pipeline ecosystem, including both the **management/setup phase** and the **nightly ETL execution phase**.

## Phase 1: Pipeline Management & Setup (Outside Nightly ETL Scope)

```mermaid
graph TD
    %% Management Scripts
    SCHEMA_ANALYSIS["analyze_opendental_schema.py<br/>✅ ACTIVE - Schema Analysis<br/>4 methods"]
    TEST_SETUP["setup_test_databases.py<br/>✅ ACTIVE - Test Environment<br/>8 functions"]
    CONFIG_MGMT["update_pipeline_config.py<br/>✅ ACTIVE - Config Management<br/>15 methods"]
    
    %% Schema Analysis Flow
    SCHEMA_ANALYSIS --> SCHEMA_DISCOVERY["SchemaDiscovery<br/>Dynamic Analysis"]
    SCHEMA_DISCOVERY --> TABLES_YML["tables.yml<br/>Static Configuration"]
    SCHEMA_DISCOVERY --> ANALYSIS_REPORT["Schema Analysis Report<br/>JSON + Summary"]
    
    %% Test Setup Flow
    TEST_SETUP --> ENV_VALIDATION["Environment Validation<br/>Test Environment Check"]
    ENV_VALIDATION --> DB_CREATION["Database Creation<br/>Test Databases"]
    DB_CREATION --> SAMPLE_DATA["Sample Data Loading<br/>Test Fixtures"]
    
    %% Configuration Management Flow
    CONFIG_MGMT --> CONFIG_VALIDATION["Configuration Validation<br/>Structure Check"]
    CONFIG_MGMT --> CONFIG_UPDATES["Configuration Updates<br/>Settings Management"]
    CONFIG_MGMT --> CONFIG_BACKUP["Configuration Backup<br/>Version Control"]
    
    %% Outputs that feed into nightly ETL
    TABLES_YML --> NIGHTLY_ETL["Nightly ETL Pipeline<br/>Input Configuration"]
    CONFIG_UPDATES --> NIGHTLY_ETL
    SAMPLE_DATA --> TEST_ETL["Test ETL Pipeline<br/>Test Data"]
    
    %% Styling
    classDef management fill:#fff3cd,stroke:#856404,stroke-width:2px
    classDef output fill:#d1ecf1,stroke:#0c5460,stroke-width:2px
    classDef nightly fill:#d4edda,stroke:#155724,stroke-width:2px
    
    class SCHEMA_ANALYSIS,TEST_SETUP,CONFIG_MGMT management
    class TABLES_YML,ANALYSIS_REPORT,SAMPLE_DATA,CONFIG_UPDATES output
    class NIGHTLY_ETL,TEST_ETL nightly
```

## Phase 2: Nightly ETL Execution (Core Pipeline)

```mermaid
graph TD
    %% Entry Points
    CLI["CLI Commands<br/>cli/main.py<br/>✅ ACTIVE"] --> ORCH["PipelineOrchestrator<br/>orchestration/pipeline_orchestrator.py<br/>✅ ACTIVE - 6 methods"]
    
    %% Main Orchestration
    ORCH --> TP["TableProcessor<br/>orchestration/table_processor.py<br/>✅ ACTIVE - 9 methods<br/>CORE ETL ENGINE"]
    ORCH --> PP["PriorityProcessor<br/>orchestration/priority_processor.py<br/>✅ ACTIVE - 5 methods<br/>BATCH PROCESSING"]
    
    %% ETL Phases
    TP --> PHASE1["PHASE 1: EXTRACTION<br/>MySQL → MySQL"]
    TP --> PHASE2["PHASE 2: LOADING<br/>MySQL → PostgreSQL"]
    
    %% Phase 1: Extraction
    PHASE1 --> SMR["SimpleMySQLReplicator<br/>core/simple_mysql_replicator.py<br/>✅ ACTIVE - 10 methods<br/>STATIC CONFIG"]
    SMR --> EXTRACT_CORE["Core Extraction Logic"]
    SMR --> INCREMENTAL["Incremental Loading"]
    SMR --> BATCH_COPY["Batch Copy Operations"]
    SMR --> REPLICATION_DB["opendental_replication"]
    
    %% Phase 2: Loading
    PHASE2 --> PGL["PostgresLoader<br/>loaders/postgres_loader.py<br/>✅ ACTIVE - 11 methods<br/>SIMPLIFIED"]
    PGL --> LOAD_CORE["Core Loading Logic"]
    PGL --> CHUNKED["Chunked Loading"]
    PGL --> VERIFY["Load Verification"]
    PGL --> RAW_SCHEMA["opendental_analytics.raw"]
    
    %% Supporting Components
    ORCH --> CONFIG["Settings<br/>config/settings.py<br/>✅ ACTIVE - 20 methods<br/>MODERN CONFIG"]
    ORCH --> CR["ConfigReader<br/>config/config_reader.py<br/>✅ ACTIVE - 12 methods<br/>STATIC CONFIG"]
    ORCH --> CONN["ConnectionFactory<br/>core/connections.py<br/>✅ ACTIVE - 25 methods<br/>UNIFIED INTERFACE"]
    ORCH --> PGS["PostgresSchema<br/>core/postgres_schema.py<br/>✅ ACTIVE - 10 methods<br/>SCHEMA CONVERSION"]
    ORCH --> UMC["UnifiedMetricsCollector<br/>monitoring/unified_metrics.py<br/>✅ ACTIVE - 15 methods<br/>COMPREHENSIVE"]
    
    %% Configuration Flow (from Phase 1)
    TABLES_YML["tables.yml<br/>From Phase 1"] --> CR
    CONFIG --> CR
    CR --> SMR
    CR --> PGL
    CR --> TP
    CR --> PP
    
    %% Connection Management
    CONN --> SMR
    CONN --> PGL
    CONN --> PGS
    CONN --> UMC
    
    %% Schema Conversion
    PGS --> PGL
    
    %% Metrics Collection
    UMC --> TP
    UMC --> PP
    
    %% Styling
    classDef active fill:#d4edda,stroke:#155724,stroke-width:2px
    classDef core fill:#cce5ff,stroke:#004085,stroke-width:3px
    classDef simplified fill:#d1ecf1,stroke:#0c5460,stroke-width:2px
    classDef modern fill:#d4edda,stroke:#155724,stroke-width:2px
    classDef input fill:#fff3cd,stroke:#856404,stroke-width:2px
    
    class CLI,ORCH,TP,PP,CONFIG,CR,CONN,PGS,UMC,SMR,PGL active
    class TP core
    class PGL simplified
    class CONFIG,CR modern
    class TABLES_YML input
```

## Complete Method Count by Component

### ✅ NIGHTLY ETL COMPONENTS (Core Pipeline)

| Component | File | Methods | Status | Purpose |
|-----------|------|---------|--------|---------|
| **PipelineOrchestrator** | `orchestration/pipeline_orchestrator.py` | 6 | ✅ ACTIVE | Main orchestration |
| **TableProcessor** | `orchestration/table_processor.py` | 9 | ✅ ACTIVE | Core ETL engine |
| **PriorityProcessor** | `orchestration/priority_processor.py` | 5 | ✅ ACTIVE | Batch processing |
| **Settings** | `config/settings.py` | 20 | ✅ ACTIVE | Modern configuration |
| **ConfigReader** | `config/config_reader.py` | 12 | ✅ ACTIVE | Static configuration |
| **ConnectionFactory** | `core/connections.py` | 25 | ✅ ACTIVE | Database connections |
| **PostgresSchema** | `core/postgres_schema.py` | 10 | ✅ ACTIVE | Schema conversion |
| **SimpleMySQLReplicator** | `core/simple_mysql_replicator.py` | 10 | ✅ ACTIVE | MySQL replication |
| **PostgresLoader** | `loaders/postgres_loader.py` | 11 | ✅ ACTIVE | PostgreSQL loading |
| **UnifiedMetricsCollector** | `monitoring/unified_metrics.py` | 15 | ✅ ACTIVE | Metrics collection |

**Total: 123 methods across 10 components**

### 🔧 MANAGEMENT SCRIPTS (Outside Nightly ETL Scope)

| Script | File | Methods/Functions | Status | Purpose |
|--------|------|-------------------|--------|---------|
| **OpenDentalSchemaAnalyzer** | `scripts/analyze_opendental_schema.py` | 4 | ✅ ACTIVE | Schema analysis |
| **Test Database Setup** | `scripts/setup_test_databases.py` | 8 | ✅ ACTIVE | Test environment |
| **PipelineConfigManager** | `scripts/update_pipeline_config.py` | 15 | ✅ ACTIVE | Configuration management |

**Total: 27 methods/functions across 3 scripts**

### 📊 COMPLETE ECOSYSTEM SUMMARY

- **Nightly ETL Components**: 10 components, 123 methods
- **Management Scripts**: 3 scripts, 27 methods/functions
- **Total Ecosystem**: 13 components, 150 methods/functions

## Data Flow by Phase

### Phase 1: Pipeline Management & Setup
```
Schema Analysis → Configuration Generation → Test Environment Setup
```

**Key Management Scripts:**
- `analyze_opendental_schema.py` - Dynamic schema discovery and `tables.yml` generation
- `setup_test_databases.py` - Test environment creation and sample data loading
- `update_pipeline_config.py` - Configuration validation and management

**Outputs:**
- `tables.yml` - Static configuration for nightly ETL
- Test databases with sample data for integration testing
- Configuration validation and backup

### Phase 2: Nightly ETL Execution

#### Phase 2a: Extraction (MySQL → MySQL)
```
OpenDental (Source) → SimpleMySQLReplicator → opendental_replication (Target)
```

**Key Methods:**
- `SimpleMySQLReplicator.copy_table()` - Main extraction method
- `SimpleMySQLReplicator._copy_incremental_table()` - Incremental extraction
- `SimpleMySQLReplicator._copy_new_records()` - Batch copying
- `SimpleMySQLReplicator.get_copy_strategy()` - Size-based strategy selection

**Configuration Integration:**
- Uses `ConfigReader.get_table_config()` for static configuration
- Uses `Settings.get_source_connection_config()` for connections
- Uses `Settings.get_replication_connection_config()` for target

#### Phase 2b: Loading (MySQL → PostgreSQL)
```
opendental_replication → PostgresLoader → opendental_analytics.raw
```

**Key Methods:**
- `PostgresLoader.load_table()` - Core loading logic
- `PostgresLoader.load_table_chunked()` - Large table handling
- `PostgresLoader.verify_load()` - Load validation
- `PostgresSchema.get_table_schema_from_mysql()` - Schema conversion

**Configuration Integration:**
- Uses `ConfigReader.get_table_config()` for table configuration
- Uses `PostgresSchema` for schema conversion
- Uses `ConnectionFactory` for explicit environment connections

## Configuration Architecture

### Complete Configuration Flow (Management + Nightly ETL)
```mermaid
graph LR
    %% Management Phase
    SCHEMA[Schema Analysis] --> YAML[tables.yml]
    CONFIG_MGMT[Config Management] --> YAML
    
    %% Nightly ETL Phase
    ENV[Environment Variables] --> SETTINGS[Settings Class]
    YAML --> CONFIG[ConfigReader]
    SETTINGS --> CONN[ConnectionFactory]
    CONFIG --> ETL[ETL Components]
    CONN --> ETL
    
    %% Test Environment
    TEST_SETUP[Test Setup] --> TEST_CONFIG[Test Configuration]
    TEST_CONFIG --> ETL
```

**Key Configuration Methods:**
- **Management Phase:**
  - `OpenDentalSchemaAnalyzer.analyze_complete_schema()` - Schema analysis and config generation
  - `PipelineConfigManager.validate_configuration()` - Configuration validation
  - `setup_test_databases()` - Test environment setup
- **Nightly ETL Phase:**
  - `Settings.get_database_config()` - Database configuration
  - `Settings.get_tables_by_importance()` - Priority-based table selection
  - `ConfigReader.get_table_config()` - Table-specific configuration
  - `ConfigReader.get_tables_by_importance()` - Importance-based filtering

## Connection Management Architecture

### Unified Interface with Settings Injection
```mermaid
graph TD
    SETTINGS[Settings Class] --> CONN_FACTORY[ConnectionFactory]
    
    CONN_FACTORY --> SOURCE_CONN[get_source_connection(settings)]
    CONN_FACTORY --> REPL_CONN[get_replication_connection(settings)]
    CONN_FACTORY --> ANALYTICS_CONN[get_analytics_connection(settings)]
    
    SOURCE_CONN --> SOURCE_DB[OpenDental Source]
    REPL_CONN --> REPL_DB[MySQL Replication]
    ANALYTICS_CONN --> ANALYTICS_DB[PostgreSQL Analytics]
    
    %% Environment Separation
    PROD_ENV[Production Environment] --> PROD_SETTINGS[Production Settings]
    TEST_ENV[Test Environment] --> TEST_SETTINGS[Test Settings]
    
    PROD_SETTINGS --> CONN_FACTORY
    TEST_SETTINGS --> CONN_FACTORY
```

**Key Connection Methods:**
- `ConnectionFactory.get_source_connection(settings)` - Source database connection
- `ConnectionFactory.get_replication_connection(settings)` - Replication database connection
- `ConnectionFactory.get_analytics_connection(settings)` - Analytics database connection
- `ConnectionFactory.get_analytics_raw_connection(settings)` - Raw schema connection
- `ConnectionFactory.get_analytics_staging_connection(settings)` - Staging schema connection
- `ConnectionFactory.get_analytics_intermediate_connection(settings)` - Intermediate schema connection
- `ConnectionFactory.get_analytics_marts_connection(settings)` - Marts schema connection

## Orchestration Architecture

### Pipeline Flow
```mermaid
graph TD
    CLI[CLI Commands] --> ORCH[PipelineOrchestrator]
    ORCH --> SINGLE[Single Table Processing]
    ORCH --> BATCH[Batch Processing]
    
    SINGLE --> TP[TableProcessor]
    BATCH --> PP[PriorityProcessor]
    
    TP --> SMR[SimpleMySQLReplicator]
    TP --> PGL[PostgresLoader]
    
    PP --> TP_MULTI[Multiple TableProcessors]
    TP_MULTI --> SMR
    TP_MULTI --> PGL
```

**Key Orchestration Methods:**
- `PipelineOrchestrator.run_pipeline_for_table()` - Single table processing
- `PipelineOrchestrator.process_tables_by_priority()` - Batch processing
- `PriorityProcessor.process_by_priority()` - Priority-based batch processing
- `TableProcessor.process_table()` - Individual table ETL

## Metrics Collection Architecture

### Comprehensive Monitoring
```mermaid
graph TD
    UMC[UnifiedMetricsCollector] --> REAL_TIME[Real-time Collection]
    UMC --> PERSISTENCE[Database Persistence]
    UMC --> STATUS[Status Reporting]
    
    REAL_TIME --> PIPELINE_METRICS[Pipeline Metrics]
    REAL_TIME --> TABLE_METRICS[Table Metrics]
    REAL_TIME --> ERROR_TRACKING[Error Tracking]
    
    PERSISTENCE --> HISTORICAL[Historical Analysis]
    STATUS --> CLI_STATUS[CLI Status]
    STATUS --> MONITORING[Monitoring Dashboards]
```

**Key Metrics Methods:**
- `UnifiedMetricsCollector.record_table_processed()` - Table-level metrics
- `UnifiedMetricsCollector.get_pipeline_status()` - Pipeline status
- `UnifiedMetricsCollector.save_metrics()` - Database persistence
- `UnifiedMetricsCollector.cleanup_old_metrics()` - Retention management

## Architecture Summary

This ETL pipeline ecosystem represents a **complete, modern architecture** with two distinct phases:

### Phase 1: Pipeline Management & Setup
1. **Schema Analysis**: Dynamic discovery and static configuration generation
2. **Test Environment**: Automated test database setup and sample data loading
3. **Configuration Management**: Validation, updates, and version control
4. **Output Generation**: `tables.yml` and test fixtures for nightly ETL

### Phase 2: Nightly ETL Execution
1. **Static Configuration**: All configuration from `tables.yml` - no dynamic discovery
2. **Unified Interface**: Settings injection for all database connections
3. **Comprehensive Metrics**: Real-time monitoring with database persistence
5. **Performance Optimized**: 5-10x faster than dynamic approaches
6. **Resource Management**: Proper connection pooling and cleanup
7. **Error Handling**: Comprehensive error handling and recovery
8. **Scalable Design**: Parallel processing for batch operations

### Complete Data Flow
The pipeline successfully moves data through four stages:
1. **Setup**: Schema analysis → Configuration generation → Test environment
2. **Extract**: MySQL source → MySQL replication (SimpleMySQLReplicator)
3. **Load**: MySQL replication → PostgreSQL analytics (PostgresLoader)
4. **Transform**: PostgreSQL analytics → dbt models (external to this pipeline)

### Ecosystem Statistics
- **Total Components**: 13 (10 nightly ETL + 3 management scripts)
- **Total Methods**: 150 (123 nightly ETL + 27 management)
- **Architecture**: Modern, clean, with clear separation of concerns
- **Performance**: Optimized for both setup efficiency and nightly execution speed 