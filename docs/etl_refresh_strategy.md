# ETL and Data Refresh Strategy

## Overview

This document outlines the strategy for maintaining data synchronization between the source OpenDental MySQL database (MDC clinic) and our local PostgreSQL analytics database. The strategy focuses on reliability, performance, and data consistency while minimizing impact on the source system.

## Current State

- Initial data load completed up to 02-28-2025
- One-time migration using `mariadb_postgre_pipe.py`
- No automated refresh mechanism in place
- Source system: OpenDental MySQL (MDC clinic)
- Target system: Local PostgreSQL analytics database

## Proposed Architecture

### 1. ETL Pipeline Components

#### Source System Connection
- Secure VPN connection to MDC clinic network
- Read-only database user with specific permissions
- Connection pooling for efficient resource usage
- Automatic reconnection handling

#### ETL Orchestration
- Airflow DAG for workflow management
- Configurable schedule based on business needs
- Monitoring and alerting system
- Error handling and retry logic

#### Data Transfer Layer
- Incremental loading strategy
- Change Data Capture (CDC) implementation
- Batch processing for large tables
- Transaction consistency checks

#### Target System Management
- Automated schema evolution
- Index optimization
- Vacuum and maintenance scheduling
- Data quality validation

### 2. Refresh Strategies by Table Type

#### High-Frequency Tables (Daily Updates)
- Appointments
- Procedures
- Payments
- Claims
- Communications
- System Logs

#### Medium-Frequency Tables (Weekly Updates)
- Patient Demographics
- Insurance Information
- Provider Schedules
- Fee Schedules

#### Low-Frequency Tables (Monthly Updates)
- Reference Data
- Historical Records
- Configuration Tables

### 3. Implementation Phases

#### Phase 1: Foundation (Week 1-2)
1. Set up secure connection to source system
2. Implement basic Airflow infrastructure
3. Create initial DAG structure
4. Develop connection management utilities

#### Phase 2: Core ETL (Week 3-4)
1. Implement incremental loading logic
2. Create table-specific ETL modules
3. Develop data quality checks
4. Set up basic monitoring

#### Phase 3: Optimization (Week 5-6)
1. Implement CDC for high-frequency tables
2. Optimize batch processing
3. Add advanced error handling
4. Enhance monitoring and alerting

#### Phase 4: Production (Week 7-8)
1. Complete testing and validation
2. Document procedures and processes
3. Create maintenance schedules
4. Deploy to production environment

### 4. Technical Specifications

#### Source System Requirements
```yaml
Connection:
  Type: MySQL
  User: analytics_readonly
  Permissions:
    - SELECT on all tables
    - SHOW VIEW
    - PROCESS
  Connection Pool:
    Min Size: 5
    Max Size: 20
    Timeout: 30 seconds
```

#### ETL Configuration
```yaml
Scheduling:
  High-Frequency: Every 6 hours
  Medium-Frequency: Weekly (Sunday 2 AM)
  Low-Frequency: Monthly (1st Sunday 3 AM)

Batch Processing:
  Chunk Size: 10,000 rows
  Max Retries: 3
  Retry Delay: 5 minutes

Monitoring:
  Success Rate Threshold: 99.9%
  Data Quality Threshold: 100%
  Alert Channels:
    - Email
    - Slack
    - PagerDuty
```

#### Data Quality Rules
1. Row count validation
2. Primary key integrity
3. Foreign key relationships
4. Data type consistency
5. Business rule validation
6. Historical data preservation

### 5. Maintenance Procedures

#### Daily Tasks
- Monitor ETL job status
- Review error logs
- Check data quality metrics
- Validate high-frequency tables

#### Weekly Tasks
- Review performance metrics
- Optimize indexes
- Clean up temporary tables
- Update statistics

#### Monthly Tasks
- Full data quality audit
- Schema evolution review
- Performance tuning
- Backup verification

### 6. Error Handling and Recovery

#### Error Categories
1. Connection Issues
   - Automatic retry with exponential backoff
   - VPN reconnection
   - Connection pool refresh

2. Data Quality Issues
   - Log detailed error information
   - Notify data team
   - Implement corrective actions

3. System Issues
   - Automatic failover
   - Service restart
   - Resource scaling

#### Recovery Procedures
1. Immediate Actions
   - Stop affected jobs
   - Log error state
   - Notify stakeholders

2. Investigation
   - Review error logs
   - Analyze impact
   - Determine root cause

3. Resolution
   - Apply fixes
   - Verify data integrity
   - Resume normal operations

### 7. Monitoring and Alerting

#### Key Metrics
1. ETL Performance
   - Job duration
   - Row processing rate
   - Resource utilization

2. Data Quality
   - Validation success rate
   - Data completeness
   - Consistency checks

3. System Health
   - Connection status
   - Resource usage
   - Error rates

#### Alert Thresholds
1. Critical
   - Job failure
   - Data corruption
   - System unavailability

2. Warning
   - Performance degradation
   - Quality issues
   - Resource constraints

3. Information
   - Job completion
   - Status updates
   - Maintenance events

### 8. Documentation Requirements

#### Technical Documentation
1. Architecture diagrams
2. Configuration details
3. API specifications
4. Database schemas

#### Operational Documentation
1. Runbooks
2. Troubleshooting guides
3. Recovery procedures
4. Maintenance schedules

#### Business Documentation
1. Data dictionary
2. Business rules
3. Quality standards
4. SLA definitions

## Next Steps

1. Review and approve strategy
2. Set up development environment
3. Begin Phase 1 implementation
4. Create detailed technical specifications
5. Develop test cases
6. Establish monitoring framework

## Success Criteria

1. 99.9% ETL job success rate
2. Zero data loss or corruption
3. Meeting all SLA requirements
4. Successful automated recovery
5. Comprehensive monitoring coverage
6. Complete documentation
7. Efficient resource utilization 