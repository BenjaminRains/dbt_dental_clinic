# ETL Pipeline Data Flow Diagram

## Current Data Movement Architecture

```mermaid
graph TD
    %% Entry Points
    CLI["CLI Commands<br/>cli/main.py<br/>✅ ACTIVE"] --> ORCH["PipelineOrchestrator<br/>orchestration/pipeline_orchestrator.py<br/>✅ ACTIVE"]
    
    %% Main Orchestration
    ORCH --> TP["TableProcessor<br/>orchestration/table_processor.py<br/>✅ ACTIVE - CORE ETL ENGINE"]
    
    %% ETL Phases
    TP --> PHASE1["PHASE 1: EXTRACTION<br/>MySQL → MySQL"]
    TP --> PHASE2["PHASE 2: LOADING<br/>MySQL → PostgreSQL"]
    TP --> PHASE3["PHASE 3: TRANSFORMATION<br/>PostgreSQL → PostgreSQL"]
    
    %% Phase 1: Extraction
    PHASE1 --> MYSQL_COPY["ExactMySQLReplicator<br/>mysql_replicator.py<br/>✅ ACTIVE - Table Copying<br/>⚠️ MISNAMED"]
    MYSQL_COPY --> SCHEMA["Schema Creation<br/>opendental_replication"]
    MYSQL_COPY --> DATA_COPY["Data Copying<br/>OpenDental → opendental_replication"]
    MYSQL_COPY --> VERIFY1["Validation"]
    
    %% Phase 2: Loading
    PHASE2 --> PG_LOADER["PostgresLoader<br/>loaders/postgres_loader.py<br/>✅ ACTIVE - MySQL to PostgreSQL<br/>⚠️ OVER-ENGINEERED"]
    PG_LOADER --> LOAD_CORE["Core Loading Logic"]
    PG_LOADER --> CHUNKED["Large Table Handling"]
    PG_LOADER --> VERIFY2["Load Validation"]
    PG_LOADER --> RAW_SCHEMA["opendental_analytics.raw"]
    
    %% Phase 3: Transformation
    PHASE3 --> TRANSFORMER["RawToPublicTransformer<br/>transformers/raw_to_public.py<br/>✅ ACTIVE - Schema Transformation"]
    TRANSFORMER --> TRANSFORM_CORE["Core Transformation"]
    TRANSFORMER --> READ_RAW["Read from raw schema"]
    TRANSFORMER --> CLEAN["Data Cleaning & Type Conversion"]
    TRANSFORMER --> WRITE_PUBLIC["Write to public schema"]
    TRANSFORMER --> PUBLIC_SCHEMA["opendental_analytics.public"]
    
    %% Supporting Components
    ORCH --> CONFIG["Settings<br/>config/settings.py<br/>✅ ACTIVE - Configuration"]
    ORCH --> CONN["ConnectionFactory<br/>core/connections.py<br/>✅ ACTIVE - Database Connections"]
    ORCH --> SCHEMA_CONV["PostgresSchema<br/>core/postgres_schema.py<br/>✅ ACTIVE - Schema Conversion"]
    ORCH --> METRICS["MetricsCollector<br/>core/metrics.py<br/>✅ ACTIVE - Basic Metrics"]
    
    %% Priority Processing
    ORCH --> PRIORITY["PriorityProcessor<br/>orchestration/priority_processor.py<br/>✅ ACTIVE - Batch Processing"]
    
    %% Styling
    classDef active fill:#d4edda,stroke:#155724,stroke-width:2px
    classDef deprecated fill:#f8d7da,stroke:#721c24,stroke-width:2px
    classDef overEngineered fill:#fff3cd,stroke:#856404,stroke-width:2px
    classDef misnamed fill:#d1ecf1,stroke:#0c5460,stroke-width:2px
    
    class CLI,ORCH,TP,CONFIG,CONN,SCHEMA_CONV,METRICS,PRIORITY active
    class TRANSFORMER,PG_LOADER active
    class MYSQL_COPY misnamed
    class PG_LOADER overEngineered
```

## File Status by Data Movement Responsibility

### ✅ ACTIVE DATA MOVEMENT FILES

| File | Purpose | Status | Lines | Complexity |
|------|---------|--------|-------|------------|
| `mysql_replicator.py` | MySQL table copying | ✅ ACTIVE | 510 | Medium |
| `loaders/postgres_loader.py` | MySQL → PostgreSQL loading | ✅ ACTIVE | 901 | ⚠️ HIGH |
| `transformers/raw_to_public.py` | Raw → Public transformation | ✅ ACTIVE | 643 | Medium |
| `orchestration/table_processor.py` | ETL coordination | ✅ ACTIVE | 421 | ⚠️ HIGH |
| `orchestration/pipeline_orchestrator.py` | Main orchestration | ✅ ACTIVE | 264 | Medium |

### ⚠️ OVER-ENGINEERED FILES

| File | Purpose | Status | Action |
|------|---------|--------|--------|
| `loaders/postgres_loader.py` | PostgreSQL loading | ⚠️ OVER-ENGINEERED | **SIMPLIFY** |
| `loaders/base_loader.py` | Base loader interface | ⚠️ OVER-ENGINEERED | **SIMPLIFY** |
| `transformers/base_transformer.py` | Base transformer interface | ⚠️ OVER-ENGINEERED | **SIMPLIFY** |
| `orchestration/table_processor.py` | Table processing | ⚠️ COMPLEX | **SIMPLIFY** |

## Data Flow by Phase

### Phase 1: Extraction (MySQL → MySQL)
```
OpenDental (Source) → ExactMySQLReplicator → opendental_replication (Target)
```

**Files Involved:**
- `mysql_replicator.py` - **ACTIVE** (rename to `mysql_table_copier.py`)
- `orchestration/table_processor.py` - **ACTIVE** (calls replicator)

**Data Movement Methods:**
- `create_exact_replica()` - Schema creation
- `copy_table_data()` - Data copying with chunking
- `verify_exact_replica()` - Validation

### Phase 2: Loading (MySQL → PostgreSQL)
```
opendental_replication → PostgresLoader → opendental_analytics.raw
```

**Files Involved:**
- `loaders/postgres_loader.py` - **ACTIVE** (needs simplification)
- `core/postgres_schema.py` - **ACTIVE** (schema conversion)
- `orchestration/table_processor.py` - **ACTIVE** (calls loader)

**Data Movement Methods:**
- `load_table()` - Core loading logic
- `load_table_chunked()` - Large table handling
- `verify_load()` - Load validation

### Phase 3: Transformation (PostgreSQL → PostgreSQL)
```
opendental_analytics.raw → RawToPublicTransformer → opendental_analytics.public
```

**Files Involved:**
- `transformers/raw_to_public.py` - **ACTIVE**
- `orchestration/table_processor.py` - **ACTIVE** (calls transformer)

**Data Movement Methods:**
- `transform_table()` - Core transformation
- `_read_from_raw()` - Read from raw schema
- `_apply_transformations()` - Data cleaning
- `_write_to_public()` - Write to public schema

## Configuration and Support Files

### ✅ ACTIVE SUPPORT FILES

| File | Purpose | Status |
|------|---------|--------|
| `config/settings.py` | Modern configuration | ✅ ACTIVE |
| `core/connections.py` | Database connections | ✅ ACTIVE |
| `core/postgres_schema.py` | Schema conversion | ✅ ACTIVE |
| `core/metrics.py` | Basic metrics | ✅ ACTIVE |
| `orchestration/priority_processor.py` | Batch processing | ✅ ACTIVE |



## Entry Points Analysis

### Current Entry Points

```
Multiple Entry Points:
├── cli/main.py (✅ ACTIVE - CLI implementation)

```

## Refactoring Priority by Data Movement Impact

### High Priority (Core Data Movement)
1. **Simplify `loaders/postgres_loader.py`** - 901 lines, over-engineered
2. **Simplify `orchestration/table_processor.py`** - Complex with multiple layers
3. **Rename `mysql_replicator.py`** - Misleading name

### Medium Priority (Support Components)
5. **Simplify base classes** - Reduce over-engineering
6. **Consolidate entry points** - Remove confusion

### Low Priority (Documentation)
7. **Update documentation** - Reflect simplified architecture
8. **Add comprehensive testing** - Ensure reliability

## Data Movement Validation Checklist

### Phase 1: Extraction
- [ ] `mysql_replicator.py` creates exact table replicas
- [ ] Schema validation works correctly
- [ ] Data copying handles large tables
- [ ] Error recovery works properly

### Phase 2: Loading
- [ ] `postgres_loader.py` loads data correctly
- [ ] Schema conversion works properly
- [ ] Chunked loading handles large tables
- [ ] Load verification is accurate

### Phase 3: Transformation
- [ ] `raw_to_public.py` transforms data correctly
- [ ] Data cleaning works properly
- [ ] Type conversions are accurate
- [ ] Transformation tracking works

### Overall Pipeline
- [ ] `table_processor.py` coordinates all phases
- [ ] `pipeline_orchestrator.py` manages overall flow
- [ ] Error handling works across all phases
- [ ] Metrics collection is accurate 