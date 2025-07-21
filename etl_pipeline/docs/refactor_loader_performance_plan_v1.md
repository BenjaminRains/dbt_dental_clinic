# ETL Performance Refactor Strategy

## Revised Implementation Priority

### Phase 1: Database-Level Optimizations (Week 1) - **Immediate 3-5x Gains**

**Why First**: These changes require minimal code changes but deliver massive performance improvements.

#### A. Connection Pool Optimization
```python
# In ConnectionFactory - update these constants
DEFAULT_POOL_SIZE = 20        # 4x increase
DEFAULT_MAX_OVERFLOW = 40     # 4x increase  
DEFAULT_POOL_TIMEOUT = 300    # 10x increase
BULK_INSERT_BUFFER = "256MB"  # 32x increase
```

#### B. Database Engine Settings
```python
def _apply_mysql_performance_settings(self, engine):
    """Apply MySQL bulk operation optimizations."""
    with engine.connect() as conn:
        # Critical for bulk operations
        conn.execute(text("SET SESSION bulk_insert_buffer_size = 268435456"))  # 256MB
        conn.execute(text("SET SESSION innodb_flush_log_at_trx_commit = 2"))
        conn.execute(text("SET SESSION autocommit = 0"))
        conn.execute(text("SET SESSION unique_checks = 0"))
        conn.execute(text("SET SESSION foreign_key_checks = 0"))

def _apply_postgres_performance_settings(self, engine):
    """Apply PostgreSQL bulk loading optimizations."""
    with engine.connect() as conn:
        conn.execute(text("SET work_mem = '256MB'"))
        conn.execute(text("SET maintenance_work_mem = '1GB'"))
        conn.execute(text("SET synchronous_commit = off"))
        conn.execute(text("SET wal_buffers = '64MB'"))
```

#### C. Bulk INSERT Implementation
```python
# Replace individual INSERTs in PostgresLoader.load_table()
def bulk_insert_optimized(self, table_name: str, rows_data: List[Dict], chunk_size: int = 50000):
    """Optimized bulk INSERT using executemany with larger chunks."""
    
    # Process in optimized chunks
    for i in range(0, len(rows_data), chunk_size):
        chunk = rows_data[i:i + chunk_size]
        
        # Build optimized INSERT statement
        columns = ', '.join([f'"{col}"' for col in chunk[0].keys()])
        placeholders = ', '.join([f':{col}' for col in chunk[0].keys()])
        
        insert_sql = f"""
            INSERT INTO {self.analytics_schema}.{table_name} ({columns})
            VALUES ({placeholders})
        """
        
        # Use executemany for bulk operation
        with self.analytics_engine.begin() as conn:
            conn.execute(text(insert_sql), chunk)
```

**Expected Impact**: 3-5x improvement immediately

### Phase 2: Streaming Architecture (Week 2) - **Memory + Speed**

#### A. Generator-Based Processing
```python
def stream_mysql_data(self, table_name: str, query: str, chunk_size: int = 50000):
    """Stream data from MySQL without loading into memory."""
    
    try:
        with self.replication_engine.connect() as conn:
            # Use server-side cursor for streaming
            result = conn.execution_options(stream_results=True).execute(text(query))
            
            while True:
                try:
                    chunk = result.fetchmany(chunk_size)
                    if not chunk:
                        break
                        
                    # Convert to list of dicts
                    column_names = list(result.keys())
                    chunk_dicts = [dict(zip(column_names, row)) for row in chunk]
                    
                    yield chunk_dicts
                    
                except Exception as e:
                    logger.error(f"Error fetching chunk for {table_name}: {e}")
                    raise
                    
    except Exception as e:
        logger.error(f"Error in streaming for {table_name}: {e}")
        raise
    finally:
        # Ensure result is closed
        if 'result' in locals():
            result.close()

def load_table_streaming(self, table_name: str, force_full: bool = False):
    """Streaming version of load_table."""
    
    import psutil
    import time
    
    start_time = time.time()
    initial_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB
    
    try:
        # Get schema and ensure table exists
        mysql_schema = self.schema_adapter.get_table_schema_from_mysql(table_name)
        if not self.schema_adapter.ensure_table_exists(table_name, mysql_schema):
            return False
        
        # Build query once
        query = self._build_load_query(table_name, incremental_columns, force_full)
        
        # Stream and process in chunks
        total_processed = 0
        for chunk in self.stream_mysql_data(table_name, query):
            # Convert types for entire chunk
            converted_chunk = [
                self.schema_adapter.convert_row_data_types(table_name, row) 
                for row in chunk
            ]
            
            # Bulk insert chunk
            self.bulk_insert_optimized(table_name, converted_chunk)
            
            total_processed += len(chunk)
            current_memory = psutil.Process().memory_info().rss / 1024 / 1024
            logger.info(f"Processed {total_processed} rows for {table_name}, Memory: {current_memory:.1f}MB")
        
        end_time = time.time()
        final_memory = psutil.Process().memory_info().rss / 1024 / 1024
        duration = end_time - start_time
        
        logger.info(f"Streaming load completed: {total_processed} rows in {duration:.2f}s, "
                   f"Memory: {initial_memory:.1f}MB → {final_memory:.1f}MB")
        
        return True
        
    except Exception as e:
        logger.error(f"Streaming load failed for {table_name}: {e}")
        return False
```

#### B. Update PostgresLoader Interface
```python
# In PostgresLoader.load_table(), add streaming decision logic
def load_table(self, table_name: str, force_full: bool = False) -> bool:
    """Enhanced load_table with automatic strategy selection."""
    
    table_config = self.get_table_config(table_name)
    estimated_size_mb = table_config.get('estimated_size_mb', 0)
    
    # Use streaming for tables > 50MB
    if estimated_size_mb > 50:
        logger.info(f"Using streaming approach for large table {table_name} ({estimated_size_mb}MB)")
        return self.load_table_streaming(table_name, force_full)
    else:
        logger.info(f"Using standard approach for small table {table_name}")
        return self.load_table_standard(table_name, force_full)  # Renamed existing method
```

**Expected Impact**: Constant memory usage + 2-3x speed improvement

### Phase 3: PostgreSQL COPY Command (Week 3) - **Game Changer**

This is where you'll see the biggest gains for large tables.

#### A. CSV-Based Transfer (Simpler than Parquet initially)
```python
def load_table_copy_csv(self, table_name: str, force_full: bool = False) -> bool:
    """Use PostgreSQL COPY command with CSV for maximum speed."""
    
    import tempfile
    import csv
    
    # Create temporary CSV file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as csvfile:
        # Stream from MySQL directly to CSV
        query = self._build_load_query(table_name, incremental_columns, force_full)
        
        with self.replication_engine.connect() as source_conn:
            result = source_conn.execution_options(stream_results=True).execute(text(query))
            
            writer = csv.writer(csvfile)
            column_names = list(result.keys())
            
            # Write header
            writer.writerow(column_names)
            
            # Stream rows directly to CSV
            chunk_size = 50000
            while True:
                chunk = result.fetchmany(chunk_size)
                if not chunk:
                    break
                writer.writerows(chunk)
        
        csv_path = csvfile.name
    
    try:
        # Use PostgreSQL COPY command
        with self.analytics_engine.begin() as target_conn:
            if force_full:
                target_conn.execute(text(f"TRUNCATE TABLE {self.analytics_schema}.{table_name}"))
            
            copy_sql = f"""
                COPY {self.analytics_schema}.{table_name} ({','.join(column_names)})
                FROM '{csv_path}'
                WITH (FORMAT csv, HEADER true, DELIMITER ',')
            """
            target_conn.execute(text(copy_sql))
            
        logger.info(f"Successfully loaded {table_name} using COPY command")
        return True
        
    finally:
        # Clean up temp file
        os.unlink(csv_path)
```

#### B. Integration with Table Strategy
```python
def load_table(self, table_name: str, force_full: bool = False) -> bool:
    """Enhanced load_table with optimal strategy selection."""
    
    table_config = self.get_table_config(table_name)
    estimated_size_mb = table_config.get('estimated_size_mb', 0)
    
    # Strategy selection based on table size
    if estimated_size_mb > 200:
        logger.info(f"Using COPY command for very large table {table_name}")
        return self.load_table_copy_csv(table_name, force_full)
    elif estimated_size_mb > 50:
        logger.info(f"Using streaming approach for large table {table_name}")
        return self.load_table_streaming(table_name, force_full)
    else:
        logger.info(f"Using standard approach for small table {table_name}")
        return self.load_table_standard(table_name, force_full)
```

**Expected Impact**: 5-10x improvement for large tables

### Phase 4: Parallel Processing (Week 4) - **Scale with CPUs**

```python
def load_table_parallel(self, table_name: str, force_full: bool = False) -> bool:
    """Parallel chunk processing for massive tables."""
    
    from concurrent.futures import ThreadPoolExecutor
    import os
    
    # Get total row count
    count_query = self._build_count_query(table_name, incremental_columns, force_full)
    with self.replication_engine.connect() as conn:
        total_rows = conn.execute(text(count_query)).scalar()
    
    if total_rows < 100000:
        # Not worth parallelizing small tables
        return self.load_table_streaming(table_name, force_full)
    
    # Calculate optimal chunk size and worker count
    cpu_count = os.cpu_count()
    chunk_size = max(50000, total_rows // (cpu_count * 2))
    
    # Create chunks
    chunks = [(i * chunk_size, min((i + 1) * chunk_size, total_rows)) 
              for i in range(0, total_rows, chunk_size)]
    
    # Process chunks in parallel
    with ThreadPoolExecutor(max_workers=cpu_count) as executor:
        futures = [
            executor.submit(self._process_chunk_parallel, table_name, start, end, force_full)
            for start, end in chunks
        ]
        
        # Wait for all chunks to complete
        results = [future.result() for future in futures]
    
    return all(results)

def _process_chunk_parallel(self, table_name: str, start: int, end: int, force_full: bool) -> bool:
    """Process a single chunk in parallel."""
    
    # Each thread gets its own connections
    replication_engine = ConnectionFactory.get_replication_connection(self.settings)
    analytics_engine = ConnectionFactory.get_analytics_raw_connection(self.settings)
    
    try:
        # Build chunk query
        base_query = self._build_load_query(table_name, incremental_columns, force_full)
        chunk_query = f"{base_query} LIMIT {end - start} OFFSET {start}"
        
        # Process chunk
        with replication_engine.connect() as source_conn:
            result = source_conn.execute(text(chunk_query))
            column_names = list(result.keys())
            rows = result.fetchall()
            
            # Convert types
            rows_data = []
            for row in rows:
                row_dict = dict(zip(column_names, row))
                converted_row = self.schema_adapter.convert_row_data_types(table_name, row_dict)
                rows_data.append(converted_row)
        
        # Bulk insert chunk
        with analytics_engine.begin() as target_conn:
            columns = ', '.join([f'"{col}"' for col in column_names])
            placeholders = ', '.join([f':{col}' for col in column_names])
            
            insert_sql = f"""
                INSERT INTO {self.analytics_schema}.{table_name} ({columns})
                VALUES ({placeholders})
            """
            target_conn.execute(text(insert_sql), rows_data)
        
        logger.info(f"Completed chunk {start}-{end} for {table_name}")
        return True
        
    except Exception as e:
        logger.error(f"Failed chunk {start}-{end} for {table_name}: {e}")
        return False
```

**Expected Impact**: Linear scaling with CPU cores

## Configuration Strategy

### Enhanced tables.yml
```yaml
# Add performance settings to existing table configs
periomeasure:
  estimated_size_mb: 500
  batch_size: 100000
  extraction_strategy: incremental
  incremental_columns: ["DateTMeasured"]
  
  # New performance settings
  performance:
    strategy: "copy_csv"  # auto/streaming/copy_csv/parallel
    parallel_chunks: 4
    bulk_insert_buffer: "256MB"
    use_optimized_connections: true

# Strategy auto-selection rules
performance_rules:
  copy_csv_threshold_mb: 200     # Use COPY for tables > 200MB
  streaming_threshold_mb: 50     # Use streaming for tables > 50MB  
  parallel_threshold_rows: 100000 # Use parallel for tables > 100K rows
```

### Configuration Validation
```python
def validate_performance_config(self, table_config: Dict) -> bool:
    """Validate performance configuration settings."""
    
    required_fields = ['estimated_size_mb', 'batch_size']
    for field in required_fields:
        if field not in table_config:
            logger.warning(f"Missing required field '{field}' in table config")
            return False
    
    # Validate performance settings if present
    if 'performance' in table_config:
        perf_config = table_config['performance']
        
        # Validate strategy
        valid_strategies = ['auto', 'streaming', 'copy_csv', 'parallel']
        if 'strategy' in perf_config and perf_config['strategy'] not in valid_strategies:
            logger.error(f"Invalid performance strategy: {perf_config['strategy']}")
            return False
        
        # Validate chunk sizes
        if 'parallel_chunks' in perf_config:
            chunks = perf_config['parallel_chunks']
            if not isinstance(chunks, int) or chunks < 1 or chunks > 16:
                logger.error(f"Invalid parallel_chunks: {chunks} (must be 1-16)")
                return False
    
    return True
```

## Implementation Timeline

### Week 1: Database Optimizations (Immediate 3-5x)
- [ ] Update ConnectionFactory with performance pools
- [ ] Add MySQL/PostgreSQL performance settings
- [ ] Implement bulk INSERT operations
- [ ] Test on large tables

### Week 2: Streaming (Memory + 2-3x Speed)
- [ ] Add generator-based streaming
- [ ] Update PostgresLoader.load_table() with strategy selection
- [ ] Test memory usage improvements

### Week 3: COPY Command (5-10x for Large Tables)
- [ ] Implement CSV-based COPY approach
- [ ] Add automatic strategy selection
- [ ] Test on largest tables (periomeasure)

### Week 4: Parallel Processing (Linear CPU Scaling)
- [ ] Add parallel chunk processing
- [ ] Optimize for your specific hardware
- [ ] Performance validation and tuning

## Risk Mitigation

### 1. **Gradual Rollout**
```python
# Add feature flags to enable/disable optimizations
def load_table(self, table_name: str, force_full: bool = False) -> bool:
    use_optimizations = self.settings.get_pipeline_setting('performance.enable_optimizations', False)
    
    if use_optimizations:
        return self.load_table_optimized(table_name, force_full)
    else:
        return self.load_table_original(table_name, force_full)  # Keep original as fallback
```

### 2. **Comprehensive Testing**
- Start with small test tables
- Validate data integrity after each phase
- Monitor memory usage and performance metrics
- Keep original methods as fallbacks

### 3. **Monitoring Integration**
```python
# Enhance UnifiedMetricsCollector
def track_performance_metrics(self, table_name: str, strategy: str, duration: float, memory_mb: float):
    """Track performance metrics for different strategies."""
    self.performance_metrics[table_name] = {
        'strategy': strategy,
        'duration': duration,
        'memory_mb': memory_mb,
        'timestamp': datetime.now()
    }
```

### Performance Metrics Collection
```python
class PerformanceTracker:
    """Track and analyze performance metrics across different strategies."""
    
    def __init__(self):
        self.metrics = {}
        self.baseline_metrics = {}
    
    def record_baseline(self, table_name: str, duration: float, memory_mb: float):
        """Record baseline performance before optimizations."""
        self.baseline_metrics[table_name] = {
            'duration': duration,
            'memory_mb': memory_mb,
            'timestamp': datetime.now()
        }
    
    def record_performance(self, table_name: str, strategy: str, duration: float, 
                          memory_mb: float, rows_processed: int):
        """Record performance metrics for a specific strategy."""
        if table_name not in self.metrics:
            self.metrics[table_name] = []
        
        self.metrics[table_name].append({
            'strategy': strategy,
            'duration': duration,
            'memory_mb': memory_mb,
            'rows_processed': rows_processed,
            'rows_per_second': rows_processed / duration if duration > 0 else 0,
            'timestamp': datetime.now()
        })
    
    def get_improvement_analysis(self, table_name: str) -> Dict:
        """Analyze performance improvements for a table."""
        if table_name not in self.baseline_metrics or table_name not in self.metrics:
            return {}
        
        baseline = self.baseline_metrics[table_name]
        latest = self.metrics[table_name][-1] if self.metrics[table_name] else None
        
        if not latest:
            return {}
        
        return {
            'table_name': table_name,
            'baseline_duration': baseline['duration'],
            'current_duration': latest['duration'],
            'speed_improvement': baseline['duration'] / latest['duration'],
            'memory_improvement': baseline['memory_mb'] / latest['memory_mb'],
            'strategy_used': latest['strategy'],
            'rows_per_second': latest['rows_per_second']
        }
    
    def generate_performance_report(self) -> str:
        """Generate a comprehensive performance report."""
        report = ["# ETL Performance Report", ""]
        
        for table_name in self.metrics:
            analysis = self.get_improvement_analysis(table_name)
            if analysis:
                report.append(f"## {table_name}")
                report.append(f"- Strategy: {analysis['strategy_used']}")
                report.append(f"- Speed Improvement: {analysis['speed_improvement']:.2f}x")
                report.append(f"- Memory Improvement: {analysis['memory_improvement']:.2f}x")
                report.append(f"- Rows/Second: {analysis['rows_per_second']:.0f}")
                report.append("")
        
        return "\n".join(report)
```

## Expected Results

Based on your current performance (583K rows in 5 minutes), here's what to expect:

| Phase | Current Time | Expected Time | Improvement |
|-------|-------------|---------------|-------------|
| **Before** | 5 minutes | 5 minutes | 1x |
| **Phase 1** | 5 minutes | 1-1.5 minutes | 3-5x |
| **Phase 2** | 1 minute | 30-45 seconds | 2-3x additional |
| **Phase 3** | 45 seconds | 5-10 seconds | 5-10x additional |
| **Phase 4** | 10 seconds | 2-5 seconds | 2-4x additional |

**Total Expected**: **50-100x improvement** for large tables like periomeasure.

The key is that each phase builds on the previous ones, and the COPY command (Phase 3) will be your biggest game-changer for large tables.