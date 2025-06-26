# ETL Pipeline Test Refactoring Plan

## Current State Issues
- Integration tests are heavily over-mocked
- They're actually unit tests in disguise
- No real integration testing happening
- Maintenance overhead is high

## Recommended Structure

### 1. Pure Unit Tests (`test_*_unit.py`)
```
tests/unit/
├── test_pipeline_orchestrator_unit.py
├── test_table_processor_unit.py
├── test_priority_processor_unit.py
└── test_postgres_loader_unit.py
```

**Characteristics:**
- ✅ Mock everything external
- ✅ Test specific logic paths
- ✅ Fast execution (< 1 second)
- ✅ No database dependencies

### 2. Integration Tests (`test_*_integration.py`)
```
tests/integration/
├── test_pipeline_orchestrator_integration.py
├── test_table_processor_integration.py
└── test_etl_flow_integration.py
```

**Characteristics:**
- ✅ Real SQLite databases
- ✅ Real component interactions
- ✅ Minimal mocking (only external connections)
- ✅ Verify actual data flow
- ⏱️ Medium execution (< 10 seconds)

### 3. End-to-End Tests (`test_*_e2e.py`)
```
tests/e2e/
├── test_full_pipeline_e2e.py
└── test_production_scenarios_e2e.py
```

**Characteristics:**
- ✅ Real MySQL/PostgreSQL in Docker
- ✅ No mocking
- ✅ Production-like scenarios
- ⏱️ Slow execution (30+ seconds)
- 🐳 Requires Docker

## Migration Strategy

### Phase 1: Cleanup Current Tests
1. **Keep current tests as unit tests**
   - Move to `tests/unit/`
   - Keep all the mocking
   - These are actually good unit tests

2. **Remove "integration" markers**
   - Current tests are unit tests
   - Remove `@pytest.mark.integration`

### Phase 2: Add Real Integration Tests
1. **Create SQLite-based integration tests**
   - Use real TableProcessor
   - Use real PostgresLoader  
   - Use real SchemaDiscovery
   - Only mock ConnectionFactory

2. **Test actual data flow**
   ```python
   def test_patient_data_flow(self, sqlite_env):
       # Real processing
       processor = TableProcessor()
       result = processor.process_table('patient')
       
       # Verify real data movement
       assert data_in_replication_db()
       assert data_in_analytics_db()
       assert transformations_applied()
   ```

### Phase 3: Add E2E Tests
1. **Docker-based databases**
2. **Production-like scenarios**
3. **Full pipeline testing**

## Implementation Priority

### High Priority (Do First)
- [ ] Move current tests to `tests/unit/`
- [ ] Create 2-3 real integration tests with SQLite
- [ ] Test actual TableProcessor → PostgresLoader flow

### Medium Priority
- [ ] Add Docker-based E2E tests
- [ ] Performance testing with large datasets
- [ ] Error recovery scenarios

### Low Priority
- [ ] Full production scenario testing
- [ ] Load testing
- [ ] Chaos engineering

## Benefits of This Approach

### Unit Tests
- Fast feedback during development
- Test specific logic paths
- Easy to debug
- High code coverage

### Integration Tests  
- Verify components work together
- Catch integration bugs
- Test real data transformations
- Reasonable execution time

### E2E Tests
- Production confidence
- Catch environment issues
- Full system validation
- Realistic performance testing

## Example Test Run Strategy

### Development (Fast Feedback)
```bash
pytest tests/unit/  # < 5 seconds
```

### CI/CD Pipeline
```bash
pytest tests/unit/ tests/integration/  # < 30 seconds
```

### Nightly/Release
```bash
pytest tests/  # All tests including E2E
```

## Key Principles

1. **Test the Right Thing at the Right Level**
   - Unit: Logic and algorithms
   - Integration: Component interactions
   - E2E: Full system behavior

2. **Minimize Mocking at Integration Level**
   - Only mock external dependencies
   - Let real components interact

3. **Use Real Data**
   - SQLite for integration tests
   - Docker databases for E2E
   - Realistic test datasets

4. **Optimize for Feedback Speed**
   - Fast unit tests for development
   - Medium integration tests for CI
   - Slow E2E tests for releases