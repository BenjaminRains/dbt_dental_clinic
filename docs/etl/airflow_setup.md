# Airflow Setup and Usage Guide

## Overview

Apache Airflow is used in this project to orchestrate the ETL processes that keep our analytics database in sync with the source OpenDental system. This document outlines the setup process, architecture, and usage guidelines.

## Architecture

### Components
1. **Airflow Webserver**: Web interface for monitoring and managing DAGs
2. **Airflow Scheduler**: Manages DAG execution schedules
3. **Airflow Workers**: Execute the actual tasks
4. **Airflow Metadata Database**: Stores DAG and task metadata
5. **PostgreSQL Database**: Our analytics database (opendental_analytics)
6. **MySQL Database**: Source OpenDental database (MDC clinic server)

### DAG Structure
```
dags/
├── mysql_postgre_dag.py      # Main ETL orchestration
└── dbt_run_dag.py             # dbt model execution (future)
```

## Setup Process

### 1. Prerequisites
- Python 3.8+
- PostgreSQL 12+
- MySQL client
- Virtual environment (recommended)

### 2. Installation

```bash
# Create and activate virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install required packages
pip install apache-airflow==2.7.1
pip install apache-airflow-providers-postgres==5.7.1
pip install apache-airflow-providers-mysql==5.3.1
pip install pandas sqlalchemy pymysql psycopg2-binary python-dotenv
```

### 3. Configuration

1. **Set Environment Variables**
   Create a `.env` file in the project root:
   ```env
   # Airflow
   AIRFLOW_HOME=/path/to/airflow
   AIRFLOW__CORE__LOAD_EXAMPLES=False
   AIRFLOW__CORE__EXECUTOR=LocalExecutor
   AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://user:pass@localhost:5432/airflow

   # Database Connections
   # Source MySQL (MDC clinic server)
   MYSQL_ROOT_USER=your_user
   MYSQL_ROOT_PASSWORD=your_password
   MYSQL_HOST=your_host
   MYSQL_PORT=3306
   MYSQL_DATABASE=your_db

   # Target PostgreSQL (Local analytics)
   POSTGRES_USER=your_user
   POSTGRES_PASSWORD=your_password
   POSTGRES_HOST=localhost
   POSTGRES_PORT=5432
   POSTGRES_DATABASE=opendental_analytics
   ```

2. **Initialize Airflow**
   ```bash
   # Set AIRFLOW_HOME
   export AIRFLOW_HOME=/path/to/airflow

   # Initialize the database
   airflow db init

   # Create admin user
   airflow users create \
       --username admin \
       --firstname Admin \
       --lastname User \
       --role Admin \
       --email admin@example.com \
       --password admin
   ```

3. **Configure Connections**
   - Open Airflow UI (http://localhost:8080)
   - Go to Admin > Connections
   - Add connections for:
     - MySQL (source)
     - PostgreSQL (target)
     - dbt (future)

### 4. Directory Structure
```
airflow/
├── dags/                    # DAG definitions
│   ├── mysql_postgre_incremental_DAG.py  # Main ETL orchestration
│   ├── monitoring_dag.py    # Health monitoring and reporting
│   └── etl_job/            # ETL implementation
│       ├── mysql_postgre_incremental.py  # Core ETL logic
│       ├── connection_factory.py         # Database connections
│       └── monitoring_utils.py           # Monitoring utilities
├── logs/                    # Task execution logs
├── plugins/                 # Custom plugins
└── config/
    └── airflow.cfg         # Airflow configuration
```

## DAGs Overview

### 1. MySQL to PostgreSQL ETL (`mysql_postgre_incremental_DAG.py`)

#### High-Frequency DAG
- Runs every 4 hours
- Tables: appointments, procedures, payments, claims, communications, system logs, recalls
- Purpose: Keep operational data current
- Includes connection testing and validation

#### Medium-Frequency DAG
- Runs daily at 2 AM
- Tables: patient demographics, insurance information, provider schedules, fee schedules, treatment plans, family relationships
- Purpose: Update reference data
- Includes sensor to wait for high-frequency DAG completion

#### Low-Frequency DAG
- Runs weekly (Sunday 3 AM)
- Tables: procedure definitions, system preferences, security logs, user definitions, user groups
- Purpose: Maintain static data

### 2. Monitoring DAG (`monitoring_dag.py`)

#### Health Monitoring DAG
- Runs every 2 hours
- Tasks:
  - Initialize monitoring tables
  - Check ETL pipeline health
- Purpose: Monitor ETL pipeline health and detect issues early

#### Daily Summary DAG
- Runs daily at 8 AM
- Tasks:
  - Generate daily ETL summary reports
- Purpose: Provide daily insights into ETL operations

### 3. dbt Execution DAG (Future)
- Will run after ETL completion
- Purpose: Transform data using dbt models
- Schedule: TBD based on business requirements

## Monitoring and Maintenance

### 1. Logging
- Task logs available in Airflow UI
- Detailed ETL logs in `etl_incremental.log`
- Error notifications via email/Slack
- Health monitoring reports every 2 hours
- Daily summary reports at 8 AM

### 2. Common Tasks

#### Start Airflow
```bash
# Start webserver
airflow webserver -p 8080

# Start scheduler
airflow scheduler
```

#### Check DAG Status
```bash
# List all DAGs
airflow dags list

# Show DAG details
airflow dags show dental_etl_high_frequency
airflow dags show dental_etl_medium_frequency
airflow dags show dental_etl_low_frequency
airflow dags show dental_etl_health_monitoring
airflow dags show dental_etl_daily_summary
```

#### Manual Trigger
```bash
# Trigger DAG run
airflow dags trigger dental_etl_high_frequency
```

### 3. Troubleshooting

#### Common Issues
1. **Connection Errors**
   - Check database credentials
   - Verify network connectivity
   - Check firewall settings
   - Review connection test logs

2. **Task Failures**
   - Review task logs
   - Check data quality issues
   - Verify table configurations
   - Check monitoring reports for patterns

3. **Performance Issues**
   - Monitor resource usage
   - Check batch sizes
   - Review index usage
   - Analyze daily summary reports

## Security Considerations

1. **Database Access**
   - Use read-only user for source MySQL database
   - Implement connection pooling
   - Regular credential rotation

2. **Airflow Security**
   - Enable authentication
   - Use SSL for connections
   - Regular security updates

3. **Data Protection**
   - Encrypt sensitive data
   - Implement data masking
   - Regular security audits

## Best Practices

1. **Development**
   - Use version control for DAGs
   - Test DAGs in development environment
   - Document changes and configurations

2. **Operations**
   - Monitor DAG performance
   - Regular log rotation
   - Backup Airflow metadata

3. **Maintenance**
   - Regular package updates
   - Database optimization
   - Index maintenance

## Next Steps

1. **Immediate**
   - Set up development environment
   - Configure connections
   - Test initial DAGs

2. **Short-term**
   - Implement monitoring
   - Set up alerts
   - Create runbooks

3. **Long-term**
   - Add dbt integration
   - Implement data quality checks
   - Optimize performance

## Resources

1. **Documentation**
   - [Airflow Documentation](https://airflow.apache.org/docs/)
   - [dbt Documentation](https://docs.getdbt.com/)
   - [Project Wiki](link-to-wiki)

2. **Support**
   - Internal Slack channel: #data-engineering
   - Email: data-team@example.com
   - On-call rotation schedule 