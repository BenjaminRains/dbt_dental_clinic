# MCP Implementation Guide
## Step-by-Step Implementation of Model Context Protocol Integration

### Quick Start Implementation

This guide provides a **phased approach** to implementing MCP servers with your existing dental clinic analytics platform.

## Phase 1: Foundation Setup (Week 1)

### 1. Install Dependencies
```bash
# Install MCP dependencies
pip install -r requirements-mcp.txt

# Verify installations
python -c "import aiohttp; print('✅ aiohttp installed')"
python -c "import pydantic; print('✅ pydantic installed')"
```

### 2. Test Basic Integration
```bash
# Test enhanced health check (without MCP servers running)
etl status --include-dental-intelligence --format summary

# Expected output:
# 📊 ETL Pipeline Status Summary
# ========================================
# Total Tables: 432
# Status: ready
# 🏥 Dental Intelligence: Not Available (missing_dependencies or server_unavailable)
```

### 3. Test PowerShell Functions
```powershell
# Test enhanced PowerShell functions
Quick-ETLStatus -IncludeDental -Format summary
Test-ComprehensiveHealth -IncludeDentalIntelligence -Format json
```

## Phase 2: MCP Server Development (Week 2-3)

### 1. Implement Dental Quality Server

The **Dental Data Quality Guardian Server** has been created as a starter implementation:

```bash
# File structure created:
etl_pipeline/
├── mcp/
│   ├── __init__.py
│   ├── client.py
│   ├── health_integration.py
│   └── servers/
│       ├── __init__.py
│       └── dental_quality_server.py
```

### 2. Create Placeholder Servers

Create minimal implementations for the other three servers:

```bash
# Create placeholder files (to be implemented later)
touch etl_pipeline/mcp/servers/model_generator_server.py
touch etl_pipeline/mcp/servers/orchestration_server.py  
touch etl_pipeline/mcp/servers/business_intelligence_server.py
```

### 3. Add Placeholder Content

Each placeholder server should have this basic structure:

```python
# Example for model_generator_server.py
class ModelGeneratorServer:
    async def generate_staging_model(self, params):
        return {'success': False, 'message': 'Not implemented yet'}
    
    async def analyze_table_relationships(self, params):
        return {'success': False, 'message': 'Not implemented yet'}
```

## Phase 3: CLI Integration Testing (Week 3)

### 1. Test Enhanced CLI Commands

```bash
# Test without MCP servers (fallback mode)
etl status --include-dental-intelligence
# Should show: "MCP server unavailable - using fallback response"

# Test PowerShell integration
Quick-ETLStatus -IncludeDental
Test-ComprehensiveHealth -IncludeDentalIntelligence -OutputFile health_report.json
```

### 2. Verify Existing Functions Still Work

```bash
# Ensure existing health checks still work
python -m etl_pipeline.monitoring.health_checks
etl status --format summary
etl-quick-status  # PowerShell function
```

## Phase 4: MCP Server Deployment (Week 4)

### 1. Deploy MCP Servers Locally

You can implement MCP servers in several ways:

#### Option A: FastAPI Implementation (Recommended)

Create `scripts/run_mcp_servers.py`:

```python
from fastapi import FastAPI
from uvicorn import run
import asyncio
from etl_pipeline.mcp.servers.dental_quality_server import DentalDataQualityServer

app = FastAPI(title="Dental Quality MCP Server")
dental_server = DentalDataQualityServer()

@app.get("/health")
async def health():
    return {"status": "healthy", "server": "dental_quality"}

@app.post("/tools/validate_patient_record_integrity")
async def validate_patient_records(params: dict):
    return await dental_server.validate_patient_record_integrity(params)

@app.post("/tools/audit_dental_workflow_compliance")  
async def audit_workflows(params: dict):
    return await dental_server.audit_dental_workflow_compliance(params)

if __name__ == "__main__":
    run(app, host="localhost", port=8001)
```

#### Option B: External MCP Server Implementation

If you want to implement servers externally (recommended for production):

```bash
# Create external MCP server projects
mkdir mcp-servers
cd mcp-servers
mkdir dental-quality-server model-generator-server orchestration-server bi-server
```

### 2. Test MCP Server Communication

```bash
# Start the dental quality server
python scripts/run_mcp_servers.py

# In another terminal, test the enhanced health check
etl status --include-dental-intelligence --format summary

# Expected output:
# 📊 ETL Pipeline Status Summary  
# ========================================
# Total Tables: 432
# Status: ready
# 🏥 Dental Practice Health
# ========================================
# Overall Health: GOOD (85.0%)
# Technical Health: 100%
# Business Health: 75%
```

## Phase 5: Integration with dbt Tests (Week 5)

### 1. Connect MCP Intelligence to dbt Testing

Create `etl_pipeline/dbt/mcp_integration.py`:

```python
from etl_pipeline.mcp.client import call_mcp_tool

async def enhance_dbt_test_with_mcp(test_name: str, test_results: dict):
    """Enhance dbt test results with MCP business intelligence."""
    
    if test_name.startswith('patient_'):
        # Add patient-specific intelligence
        mcp_insight = await call_mcp_tool(
            'dental_quality_server',
            'validate_patient_record_integrity', 
            {'test_results': test_results}
        )
        test_results['mcp_insight'] = mcp_insight
    
    return test_results
```

### 2. Enhance Table Validation

Modify your existing validation to include MCP insights:

```python
# In validation logic
if mcp_available:
    validation_results['mcp_insights'] = await get_mcp_insights(table_name)
```

## Phase 6: Production Optimization (Week 6+)

### 1. Performance Optimization

- Implement async connection pooling
- Add MCP response caching  
- Configure health check intervals
- Set up monitoring dashboards

### 2. Full Server Implementation

Replace placeholder implementations with actual business logic:

- Connect to your database connections
- Implement actual validation queries
- Add comprehensive error handling
- Create detailed logging

### 3. Advanced Features

- Real-time streaming health checks
- Predictive analytics for pipeline issues
- Automated remediation suggestions
- Integration with external dental practice management systems

## Expected Usage Patterns

### Daily Operations

```bash
# Morning health check with dental intelligence
Test-ComprehensiveHealth -IncludeDentalIntelligence

# During ETL runs - enhanced monitoring
etl status --include-dental-intelligence --format json | jq '.dental_intelligence.overall_assessment'

# End of day comprehensive report
Test-ComprehensiveHealth -IncludeDentalIntelligence -OutputFile daily_health_$(Get-Date -Format 'yyyy-MM-dd').json
```

### Troubleshooting Mode

```bash
# When issues are detected - detailed analysis
etl status --include-dental-intelligence --format json > troubleshooting_report.json

# Patient-specific validation
# (once MCP servers are fully implemented)
etl validate-patient --patient-id 12345 --include-mcp-insights
```

## Configuration Management

### MCP Server Configuration

Edit `etl_pipeline/config/mcp_config.yml` to:

- Enable/disable specific MCP servers
- Adjust timeout values
- Configure health check intervals
- Set validation thresholds

### Integration Toggles

The implementation includes fallback mechanisms:

- `fallback_behavior.continue_without_mcp: true` - ETL continues if MCP fails
- `fallback_behavior.show_fallback_warnings: true` - Shows when using fallback
- `integration.dbt_test_integration: true` - Enables dbt test enhancement

## Expected Benefits Timeline

### Week 1-2: Foundation
- Enhanced CLI with dental intelligence toggles
- Graceful fallback when MCP servers unavailable
- Improved PowerShell functions

### Week 3-4: Basic Intelligence  
- Patient record integrity validation
- Workflow compliance monitoring
- HIPAA compliance checking

### Week 5-6: Advanced Integration
- dbt test enhancement with business context
- Predictive pipeline health monitoring
- Automated recommendation generation

### Week 7+: Full Intelligence Platform
- Real-time dental practice analytics
- Proactive issue detection and remediation
- Comprehensive business intelligence dashboard

## Troubleshooting Common Issues

### MCP Servers Not Available
```bash
# Check server status
curl http://localhost:8001/health
curl http://localhost:8002/health

# Verify configuration
cat etl_pipeline/config/mcp_config.yml | grep -A5 "dental_quality_server"
```

### Import Errors
```bash
# Install missing dependencies
pip install -r requirements-mcp.txt

# Check Python path
python -c "from etl_pipeline.mcp import MCPClient; print('✅ MCP module accessible')"
```

### PowerShell Function Issues
```powershell
# Re-source the functions
. .\etl_pipeline\cli\etl_functions.ps1

# Test basic function
Quick-ETLStatus
```

This implementation approach allows you to **incrementally enhance** your existing platform while maintaining full backward compatibility with your current operations. 