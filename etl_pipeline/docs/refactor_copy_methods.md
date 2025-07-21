# Refactor Copy Methods: ConnectionManager Migration Plan

## Overview

This document outlines the plan to refactor all copy methods in `simple_mysql_replicator.py` to
 use `ConnectionManager` instead of direct engine connections. This addresses the systemic
  connection timeout vulnerability that caused the `procnote` table extraction failure.

## Problem Statement

### Current Issue
All copy methods use direct engine connections:
```python
with self.source_engine.connect() as source_conn:
    with self.target_engine.connect() as target_conn:
        # Long-running operations
```

### Problems with Direct Connections
1. **No Retry Logic**: Failures cause immediate termination
2. **No Connection Health Checks**: No validation during long operations
3. **No Rate Limiting**: Can overwhelm source database
4. **Single Long-Running Connection**: Connections held for entire operation
5. **No Exponential Backoff**: No intelligent retry strategy

### Impact
- **`procnote` failure**: 8+ minute timeout on 561K records with 50K batch size
- **Systemic vulnerability**: All large tables at risk
- **Poor reliability**: No graceful handling of connection issues

## Solution: ConnectionManager Integration

### ConnectionManager Benefits
1. **Automatic Retry**: Failed queries retry with exponential backoff
2. **Connection Health**: Fresh connections on retry
3. **Rate Limiting**: Prevents overwhelming source database
4. **Connection Reuse**: Efficient connection management
5. **Automatic Cleanup**: Context manager ensures proper cleanup

## Refactor Plan

### Phase 1: Core Infrastructure (Week 1)

#### 1.1 Import and Setup
```python
# Add to simple_mysql_replicator.py imports
from etl_pipeline.core.connections import create_connection_manager
```

#### 1.2 Create ConnectionManager Factory Method
```python
def _create_connection_managers(self):
    """Create ConnectionManager instances for source and target databases."""
    source_manager = create_connection_manager(
        self.source_engine,
        max_retries=3,
        retry_delay=1.0
    )
    target_manager = create_connection_manager(
        self.target_engine,
        max_retries=3,
        retry_delay=1.0
    )
    return source_manager, target_manager
```

### Phase 2: Method-by-Method Refactor (Week 1-2)

#### 2.1 `_copy_small_table()` - Priority: LOW
**Current Pattern:**
```python
def _copy_small_table(self, table_name: str) -> bool:
    with self.source_engine.connect() as source_conn:
        with self.target_engine.connect() as target_conn:
            # Direct operations
```

**Refactored Pattern:**
```python
def _copy_small_table(self, table_name: str) -> bool:
    try:
        with create_connection_manager(self.source_engine) as source_manager:
            with create_connection_manager(self.target_engine) as target_manager:
                # Use execute_with_retry for all operations
                if source_host == target_host:
                    copy_sql = f"INSERT INTO `{table_name}` SELECT * FROM `{source_db}`.`{table_name}`"
                    result = target_manager.execute_with_retry(copy_sql)
                else:
                    # Handle cross-server operations with retry logic
                    result = source_manager.execute_with_retry(f"SELECT * FROM `{table_name}`")
                    rows = result.fetchall()
                    # Process rows with retry logic
```

#### 2.2 `_copy_medium_table()` - Priority: MEDIUM
**Current Pattern:**
```python
def _copy_medium_table(self, table_name: str, batch_size: int) -> bool:
    with self.source_engine.connect() as source_conn:
        with self.target_engine.connect() as target_conn:
            while True:
                # Batch processing with direct connections
```

**Refactored Pattern:**
```python
def _copy_medium_table(self, table_name: str, batch_size: int) -> bool:
    try:
        with create_connection_manager(self.source_engine) as source_manager:
            with create_connection_manager(self.target_engine) as target_manager:
                offset = 0
                total_copied = 0
                
                while True:
                    # Use execute_with_retry for all batch operations
                    if source_host == target_host:
                        copy_sql = f"""
                            INSERT INTO `{table_name}` 
                            SELECT * FROM `{source_db}`.`{table_name}` 
                            LIMIT {batch_size} OFFSET {offset}
                        """
                        result = target_manager.execute_with_retry(copy_sql)
                    else:
                        # Cross-server with retry logic
                        result = source_manager.execute_with_retry(
                            f"SELECT * FROM `{table_name}` LIMIT {batch_size} OFFSET {offset}"
                        )
                        rows = result.fetchall()
                        # Process with retry logic
```

#### 2.3 `_copy_large_table()` - Priority: HIGH
**Current Pattern:**
```python
def _copy_large_table(self, table_name: str, batch_size: int) -> bool:
    with self.source_engine.connect() as source_conn:
        with self.target_engine.connect() as target_conn:
            while total_copied < total_count:
                # Large batch processing
```

**Refactored Pattern:**
```python
def _copy_large_table(self, table_name: str, batch_size: int) -> bool:
    try:
        with create_connection_manager(self.source_engine) as source_manager:
            with create_connection_manager(self.target_engine) as target_manager:
                # Get total count with retry
                count_result = source_manager.execute_with_retry(
                    f"SELECT COUNT(*) FROM `{source_db}`.`{table_name}`"
                )
                total_count = count_result.scalar() or 0
                
                offset = 0
                total_copied = 0
                
                while total_copied < total_count:
                    # All operations use execute_with_retry
                    copy_sql = f"""
                        INSERT INTO `{table_name}` 
                        SELECT * FROM `{source_db}`.`{table_name}` 
                        LIMIT {batch_size} OFFSET {offset}
                    """
                    result = target_manager.execute_with_retry(copy_sql)
                    # Process with progress tracking
```

#### 2.4 `_copy_new_records()` - Priority: CRITICAL ⚠️
**Current Pattern:**
```python
def _copy_new_records(self, table_name: str, incremental_column: str, last_processed: Any, batch_size: int) -> bool:
    with self.source_engine.connect() as source_conn:
        with self.target_engine.connect() as target_conn:
            while True:
                # Incremental batch processing - CAUSED procnote FAILURE
```

**Refactored Pattern:**
```python
def _copy_new_records(self, table_name: str, incremental_column: str, last_processed: Any, batch_size: int) -> bool:
    try:
        with create_connection_manager(self.source_engine) as source_manager:
            with create_connection_manager(self.target_engine) as target_manager:
                offset = 0
                total_copied = 0
                
                while True:
                    # All operations use execute_with_retry with rate limiting
                    if last_processed is None:
                        copy_sql = f"""
                            INSERT INTO `{table_name}` 
                            SELECT * FROM `{source_db}`.`{table_name}` 
                            LIMIT {batch_size} OFFSET {offset}
                        """
                        result = target_manager.execute_with_retry(copy_sql, rate_limit=True)
                    else:
                        copy_sql = f"""
                            INSERT INTO `{table_name}` 
                            SELECT * FROM `{source_db}`.`{table_name}` 
                            WHERE {incremental_column} > :last_processed
                            LIMIT {batch_size} OFFSET {offset}
                        """
                        result = target_manager.execute_with_retry(
                            copy_sql, 
                            {"last_processed": last_processed}, 
                            rate_limit=True
                        )
                    
                    rows_copied = result.rowcount
                    if rows_copied == 0:
                        break
                    
                    total_copied += rows_copied
                    offset += batch_size
                    
                    logger.info(f"Copied batch: {total_copied} total records for {table_name}")
```

### Phase 3: Supporting Methods Refactor (Week 2)

#### 3.1 `_get_last_processed_value()` - Priority: MEDIUM
```python
def _get_last_processed_value(self, table_name: str, incremental_column: str) -> Any:
    try:
        with create_connection_manager(self.target_engine) as target_manager:
            # Check if table exists with retry
            result = target_manager.execute_with_retry(f"SHOW TABLES LIKE '{table_name}'")
            if not result.fetchone():
                return None
            
            # Get max value with retry
            result = target_manager.execute_with_retry(f"SELECT MAX({incremental_column}) FROM `{table_name}`")
            return result.scalar()
```

#### 3.2 `_get_new_records_count()` - Priority: MEDIUM
```python
def _get_new_records_count(self, table_name: str, incremental_column: str, last_processed: Any) -> int:
    try:
        with create_connection_manager(self.source_engine) as source_manager:
            if last_processed is None:
                result = source_manager.execute_with_retry(f"SELECT COUNT(*) FROM `{table_name}`")
            else:
                result = source_manager.execute_with_retry(
                    f"SELECT COUNT(*) FROM `{table_name}` WHERE {incremental_column} > :last_processed",
                    {"last_processed": last_processed}
                )
            return result.scalar() or 0
```

### Phase 4: Configuration and Optimization (Week 2)

#### 4.1 Batch Size Optimization
```python
def _get_optimized_batch_size(self, table_name: str, config: Dict) -> int:
    """Get optimized batch size based on table size and connection manager."""
    base_batch_size = config.get('batch_size', 5000)
    estimated_size_mb = config.get('estimated_size_mb', 0)
    
    # Reduce batch size for large tables to prevent timeouts
    if estimated_size_mb > 100:
        return min(base_batch_size // 2, 10000)  # Max 10K for large tables
    elif estimated_size_mb > 50:
        return min(base_batch_size, 25000)  # Max 25K for medium tables
    else:
        return base_batch_size
```

#### 4.2 Connection Manager Configuration
```python
def _get_connection_manager_config(self, table_name: str, config: Dict) -> Dict:
    """Get optimized ConnectionManager configuration based on table characteristics."""
    estimated_size_mb = config.get('estimated_size_mb', 0)
    
    if estimated_size_mb > 100:
        # Large tables: more retries, longer delays
        return {
            'max_retries': 5,
            'retry_delay': 2.0
        }
    elif estimated_size_mb > 50:
        # Medium tables: moderate retries
        return {
            'max_retries': 3,
            'retry_delay': 1.0
        }
    else:
        # Small tables: standard configuration
        return {
            'max_retries': 3,
            'retry_delay': 0.5
        }
```

### Phase 5: Testing and Validation (Week 3)

#### 5.1 Unit Tests
```python
def test_copy_methods_with_connection_manager():
    """Test all copy methods use ConnectionManager correctly."""
    # Test each method with mocked ConnectionManager
    # Verify execute_with_retry is called
    # Verify proper error handling
```

#### 5.2 Integration Tests
```python
def test_large_table_copy_with_retry():
    """Test large table copy with simulated connection failures."""
    # Simulate connection timeouts
    # Verify retry logic works
    # Verify progress tracking
```

#### 5.3 Performance Tests
```python
def test_copy_performance_with_connection_manager():
    """Test performance impact of ConnectionManager integration."""
    # Compare performance before/after
    # Verify no significant performance degradation
    # Test with various table sizes
```

## Implementation Timeline

### Week 1: Core Infrastructure
- [ ] Add ConnectionManager imports
- [ ] Create connection manager factory method
- [ ] Refactor `_copy_small_table()`
- [ ] Refactor `_copy_medium_table()`

### Week 2: Critical Methods
- [ ] Refactor `_copy_large_table()` (HIGH PRIORITY)
- [ ] Refactor `_copy_new_records()` (CRITICAL PRIORITY)
- [ ] Refactor supporting methods
- [ ] Add batch size optimization

### Week 3: Testing and Validation
- [ ] Unit tests for all refactored methods
- [ ] Integration tests with simulated failures
- [ ] Performance testing
- [ ] Documentation updates

## Risk Mitigation

### 1. Gradual Rollout
- Start with small tables to validate approach
- Move to medium tables after validation
- Finally tackle large tables

### 2. Monitoring
- Add detailed logging for ConnectionManager operations
- Monitor retry frequency and success rates
- Track performance impact

### 3. Rollback Plan
- Keep original methods as fallback
- Feature flag to switch between old/new implementations
- Quick rollback capability

## Expected Benefits

### 1. Reliability Improvements
- **99% reduction** in connection timeout failures
- **Automatic recovery** from transient network issues
- **Graceful degradation** during database maintenance

### 2. Performance Optimizations
- **Rate limiting** prevents overwhelming source database
- **Connection reuse** reduces connection overhead
- **Optimized batch sizes** for different table sizes

### 3. Operational Benefits
- **Better error messages** with retry context
- **Progress tracking** for long-running operations
- **Consistent behavior** across all copy methods

## Success Metrics

### 1. Reliability Metrics
- **Connection timeout rate**: Target < 1%
- **Retry success rate**: Target > 95%
- **Overall ETL success rate**: Target > 99%

### 2. Performance Metrics
- **No performance degradation** compared to current implementation
- **Improved throughput** for large tables
- **Reduced database load** through rate limiting

### 3. Operational Metrics
- **Mean time to recovery** from connection failures: < 30 seconds
- **Reduced manual intervention** for connection issues
- **Improved monitoring** and alerting capabilities

## Conclusion

This refactor addresses the systemic connection timeout vulnerability that caused the `procnote`
 failure. By migrating all copy methods to use `ConnectionManager`, we'll achieve:

1. **Robust error handling** with automatic retry logic
2. **Better resource management** through connection pooling
3. **Improved reliability** for all table sizes
4. **Consistent behavior** across the entire ETL pipeline

The phased approach ensures minimal risk while delivering maximum benefit, with the critical
 `_copy_new_records()` method being the highest priority to prevent future `procnote`-like failures. 