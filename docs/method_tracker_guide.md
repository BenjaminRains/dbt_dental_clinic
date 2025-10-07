# Method Tracker Integration Guide

## Overview

The Method Tracker is a lightweight, thread-safe utility designed to monitor which `PostgresLoader` methods are actually executed during ETL runs. This helps identify unused code paths for refactoring and removal, providing objective data to guide code cleanup decisions.

## Architecture

### Core Components

1. **MethodTracker Class** (`scripts/method_tracker.py`)
   - Thread-safe tracking of method invocations
   - JSON-based persistence with run merging
   - Minimal performance overhead

2. **@track_method Decorator**
   - Opt-in annotation for methods to track
   - Automatic class-qualified method identification
   - Thread-safe call counting and timestamping

3. **Integration Points**
   - PostgresLoader method annotations
   - CLI orchestrator report generation
   - Automatic report saving on pipeline completion

## How It Works

### 1. Method Tracking

```python
@track_method
def load_table(self, table_name: str, force_full: bool = False):
    # Method implementation
    pass
```

When a decorated method is called:
- Method ID is generated (e.g., `PostgresLoader.load_table`)
- Call count is incremented
- Timestamps are updated (first_seen, last_seen)
- Run ID is recorded for tracking across multiple ETL cycles

### 2. Thread Safety

The tracker uses `threading.Lock()` to ensure data integrity in multi-threaded environments:

```python
with self._lock:
    # Atomic updates to tracked_methods dictionary
    record["call_count"] += 1
    record["last_seen"] = datetime.now().isoformat()
```

### 3. Data Persistence

Tracking data is saved to `etl_pipeline/logs/method_usage.json` with the following structure:

```json
{
  "PostgresLoader.load_table": {
    "call_count": 15,
    "first_seen": "2024-01-15T10:30:00.123456",
    "last_seen": "2024-01-15T14:45:00.789012",
    "run_ids": ["2024-01-15T10:30:00.123456", "2024-01-15T14:45:00.789012"]
  }
}
```

### 4. Report Generation

Reports are automatically generated at the end of each ETL run:

```python
finally:
    orchestrator.cleanup()
    # Save method usage tracking report
    try:
        save_tracking_report()
        print_tracking_report()
    except Exception as e:
        logger.warning(f"Failed to save method tracking report: {e}")
```

## Integration with ETL Pipeline

### 1. PostgresLoader Integration

The method tracker is integrated into the PostgresLoader class:

```python
# Import path setup
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent / "scripts"))
from method_tracker import track_method, save_tracking_report, print_tracking_report

class PostgresLoader:
    @track_method
    def load_table(self, table_name: str, force_full: bool = False):
        # Main table loading method
        
    @track_method
    def load_table_streaming(self, table_name: str, force_full: bool = False):
        # Streaming loading method
        
    @track_method
    def load_table_chunked(self, table_name: str, force_full: bool = False, chunk_size: int = 25000):
        # Chunked loading method
        
    @track_method
    def verify_load(self, table_name: str) -> bool:
        # Load verification method
```

### 2. CLI Integration

The ETL CLI automatically saves tracking reports:

```python
# In etl_pipeline/cli/commands.py
# Add scripts directory to path for method tracking
scripts_path = Path(__file__).parent.parent.parent / "scripts"
sys.path.insert(0, str(scripts_path))
from method_tracker import save_tracking_report, print_tracking_report

@click.command()
def run(...):
    try:
        # ETL pipeline execution
        ...
    finally:
        orchestrator.cleanup()
        # Save method usage tracking report
        try:
            save_tracking_report()
            print_tracking_report()
        except Exception as e:
            logger.warning(f"Failed to save method tracking report: {e}")
```

### 3. Tracked Methods (Phase 1)

Currently tracked methods include:

- `PostgresLoader.load_table` - Main table loading entry point
- `PostgresLoader.load_table_streaming` - Memory-efficient streaming
- `PostgresLoader.load_table_standard` - Standard loading approach
- `PostgresLoader.load_table_chunked` - Chunked processing for large tables
- `PostgresLoader.load_table_copy_csv` - High-performance COPY command
- `PostgresLoader.load_table_parallel` - Parallel processing for massive tables
- `PostgresLoader.verify_load` - Load verification and validation
- `PostgresLoader._build_load_query` - Query building for data extraction

## Usage Examples

### 1. Running ETL with Method Tracking

```bash
# Run full ETL pipeline
cd etl_pipeline
pipenv run etl run --full

# Run specific tables
pipenv run etl run --tables patient appointment

# Dry run to see what would be tracked
pipenv run etl run --dry-run
```

### 2. Viewing Method Usage Reports

After an ETL run, the method usage report is automatically displayed:

```
============================================================
METHOD USAGE REPORT
============================================================

PostgresLoader.load_table
  Calls: 15
  First seen: 2024-01-15T10:30:00.123456
  Last seen:  2024-01-15T14:45:00.789012

PostgresLoader.verify_load
  Calls: 15
  First seen: 2024-01-15T10:30:00.123456
  Last seen:  2024-01-15T14:45:00.789012

PostgresLoader.load_table_chunked
  Calls: 3
  First seen: 2024-01-15T10:30:00.123456
  Last seen:  2024-01-15T14:45:00.789012
```

### 3. Analyzing Method Usage Data

The JSON report can be analyzed programmatically:

```python
import json
from pathlib import Path

# Load method usage data
with open("etl_pipeline/logs/method_usage.json", "r") as f:
    data = json.load(f)

# Find unused methods
all_methods = set(data.keys())
called_methods = {k for k, v in data.items() if v["call_count"] > 0}
unused_methods = all_methods - called_methods

print(f"Unused methods: {unused_methods}")
```

## Configuration

### 1. Output File Location

Default: `etl_pipeline/logs/method_usage.json`

Can be customized by modifying the MethodTracker initialization:

```python
tracker = MethodTracker(output_file="custom/path/method_usage.json")
```

### 2. Logging Level

Method tracking uses debug-level logging:

```python
logger.debug(f"[METHOD_TRACK] {method_id} called")
```

To see tracking logs, set logging level to DEBUG:

```python
import logging
logging.getLogger().setLevel(logging.DEBUG)
```

## Performance Considerations

### 1. Overhead

- **Minimal Impact**: Decorator adds ~0.1ms per method call
- **Memory Usage**: Negligible - only stores method IDs and counts
- **Thread Safety**: Lock contention is minimal due to short critical sections

### 2. Optimization

- **Batched Writes**: All tracking data is written once at the end of the run
- **Efficient Storage**: Uses sets for run_ids with automatic deduplication
- **JSON Serialization**: Only occurs during report generation

## Troubleshooting

### 1. Import Errors

If you see import errors for `method_tracker`:

```python
# Ensure scripts directory is in Python path
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent / "scripts"))
```

### 2. Missing Reports

If method usage reports are not generated:

1. Check that `save_tracking_report()` is called in the finally block
2. Verify the `etl_pipeline/logs/` directory exists and is writable
3. Check for exceptions in the tracking report generation

### 3. Thread Safety Issues

If you encounter data corruption:

1. Ensure all tracked methods use the `@track_method` decorator
2. Verify the MethodTracker uses proper locking
3. Check for any direct access to `tracked_methods` dictionary

## Future Enhancements

### Phase 2 Plans

1. **Expanded Method Coverage**
   - Add tracking to internal helper methods
   - Include query building and strategy selection methods

2. **Enhanced Reporting**
   - Method execution time tracking
   - Parameter signature analysis
   - Call stack depth information

3. **Integration Improvements**
   - CI/CD pipeline integration
   - Automated unused method detection
   - Historical trend analysis

### Phase 3 Plans

1. **Automated Cleanup**
   - Flag unused methods for removal
   - Generate refactoring recommendations
   - Integration with code review tools

2. **Advanced Analytics**
   - Method usage patterns over time
   - Performance correlation analysis
   - Load strategy effectiveness metrics

## Best Practices

### 1. Method Annotation

- Start with public methods that are called by the orchestrator
- Add private methods that influence strategy selection
- Avoid tracking trivial getter/setter methods

### 2. Report Analysis

- Run multiple ETL cycles before making removal decisions
- Consider seasonal or periodic usage patterns
- Corroborate findings with log analysis

### 3. Maintenance

- Archive old method usage reports
- Monitor tracking overhead in production
- Update method annotations as code evolves

## Related Documentation

- [Method Usage Tracking Implementation Guide](method_usage_tracking.md) - Detailed implementation strategy
- [PostgresLoader Refactoring Plan](refactor_postgres_loader_full_refresh_tracking.md) - Context for why tracking is needed
- [ETL Pipeline Architecture](etl/) - Understanding the broader ETL system

## Support

For questions or issues with the method tracker:

1. Check the troubleshooting section above
2. Review the implementation guide for technical details
3. Examine the generated JSON reports for data insights
4. Consider the phased rollout approach for gradual adoption
