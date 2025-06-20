# MySQL Dump Setup Guide
## OpenDental to Local ETL Server

This guide walks through setting up MySQL dumps from your production OpenDental server to a local MySQL database for ETL processing.

## Overview

```
OpenDental Production MySQL → Local MySQL (opendental_replication) → DLT Pipeline → PostgreSQL Analytics
```

## Prerequisites

- MySQL installed on both production and ETL servers
- Network connectivity between servers (for direct dumps)
- Read-only access to production database
- Administrative access to local MySQL instance
- Python 3.7+ installed on ETL server

## Part 1: Environment Setup

### Step 1: Create Environment File

Copy the template and update with your credentials:
```bash
cp .env.template .env
```

Required environment variables:
```bash
# OpenDental Source Database (Production - Read Only)
OPENDENTAL_SOURCE_HOST=client-server
OPENDENTAL_SOURCE_PORT=3306
OPENDENTAL_SOURCE_DB=opendental
OPENDENTAL_SOURCE_USER=readonly_user  # MUST be a read-only user
OPENDENTAL_SOURCE_PASSWORD=your_password

# MySQL Replication Database (Local Copy - Full Access)
MYSQL_REPLICATION_HOST=localhost
MYSQL_REPLICATION_PORT=3306
MYSQL_REPLICATION_DB=opendental_replication
MYSQL_REPLICATION_USER=replication_user
MYSQL_REPLICATION_PASSWORD=your_password
```

### Step 2: Install Dependencies

```bash
pip install -r requirements.txt
```

## Part 2: Database Setup

### Step 1: Create Local Database

```sql
-- Connect to MySQL as root
mysql -u root -p

-- Create database and user
CREATE DATABASE opendental_replication;
CREATE USER 'replication_user'@'localhost' IDENTIFIED BY 'your_password';
GRANT ALL PRIVILEGES ON opendental_replication.* TO 'replication_user'@'localhost';
FLUSH PRIVILEGES;
```

### Step 2: Verify Connections

Test source connection:
```sql
-- In DBeaver, connect to production server
-- IMPORTANT: Always use read-only user for production access
SHOW DATABASES;
USE opendental;
SHOW TABLES;

-- Verify user privileges (should be read-only)
SHOW GRANTS FOR CURRENT_USER();
```

Test local connection:
```sql
-- In DBeaver, connect to local MySQL
SHOW DATABASES;
USE opendental_replication;
SHOW TABLES;
```

## Part 3: Dump Process

### Security Considerations

1. **Always Use Read-Only User**
   - Create a dedicated read-only user on the client server
   - Grant only SELECT privileges
   - Never use root or admin credentials

2. **If Root Access is Required**
   - Document the requirement and get approval
   - Use a temporary password
   - Change password after dump
   - Log all root access
   - Consider using MySQL's audit plugin

3. **Network Security**
   - Use SSL/TLS for connections
   - Restrict access to specific IP addresses
   - Use non-standard port if possible
   - Implement connection timeouts

### Option 1: Direct Dump

If you have network connectivity between servers:

```bash
# Verify user is read-only before proceeding
python scripts/create_mysql_dump.py --direct
```

This will:
1. Connect to production server using read-only credentials
2. Create a dump
3. Stream it directly to local database
4. Verify the import

### Option 2: File-Based Dump

If you need to transfer the file manually:

1. Create dump file:
```bash
python scripts/create_mysql_dump.py
```

2. Transfer the dump file to your local machine
   - Use secure transfer methods (SFTP, SCP)
   - Encrypt the dump file if containing sensitive data
   - Delete the dump file after successful import

3. Import the dump:
```bash
python scripts/setup_replication_db.py dumps/opendental_dump_YYYYMMDD_HHMMSS.sql
```

## Part 4: DLT Integration

### Step 1: Configure DLT Source

The DLT pipeline is already configured to use the local replication database. Verify the configuration in `config/pipeline_config.yml`:

```yaml
databases:
  source:
    type: mysql
    schema: opendental_replication
    read_only: true
```

### Step 2: Test DLT Connection

```bash
python tests/dlt_test_connections.py
```

### Step 3: Run DLT Pipeline

```bash
python scripts/run_pipeline.py
```

## Part 5: Automation

### Option 1: Manual Refresh

1. Create a new dump:
```bash
python scripts/create_mysql_dump.py --direct
```

2. Run DLT pipeline:
```bash
python scripts/run_pipeline.py
```

### Option 2: Scheduled Refresh

Create a shell script (`refresh_data.sh`):
```bash
#!/bin/bash

# Create dump and import
python scripts/create_mysql_dump.py --direct

# Run DLT pipeline
python scripts/run_pipeline.py
```

Add to crontab:
```bash
# Run daily at 2 AM
0 2 * * * /path/to/refresh_data.sh >> /path/to/refresh.log 2>&1
```

## Part 6: Monitoring

### Check Dump Status

```sql
-- In DBeaver, connect to local MySQL
USE opendental_replication;

-- Check table counts
SELECT 
    table_name,
    table_rows
FROM information_schema.tables
WHERE table_schema = 'opendental_replication'
ORDER BY table_rows DESC;
```

### Check DLT Status

```sql
-- In DBeaver, connect to PostgreSQL
SELECT * FROM _dlt_metrics 
WHERE table_name = 'patient'
ORDER BY timestamp DESC
LIMIT 5;
```

## Part 7: Troubleshooting

### Common Issues

1. **Connection Refused**
   - Check firewall settings
   - Verify MySQL is running
   - Check port accessibility
   - Verify SSL/TLS settings

2. **Access Denied**
   - Verify user credentials
   - Check user privileges
   - Ensure correct host access
   - Check for SSL/TLS requirements

3. **Import Errors**
   - Check disk space
   - Verify MySQL version compatibility
   - Check for duplicate key errors
   - Verify file permissions

4. **DLT Pipeline Errors**
   - Check DLT logs
   - Verify table schemas match
   - Check for data type mismatches
   - Verify user permissions

### Recovery Procedures

If the dump fails:

1. **Stop any running processes**
   ```sql
   -- In MySQL
   SHOW PROCESSLIST;
   KILL [process_id];
   ```

2. **Clean up and retry**
   ```sql
   -- Drop and recreate database
   DROP DATABASE opendental_replication;
   CREATE DATABASE opendental_replication;
   ```

3. **Run dump process again**
   ```bash
   python scripts/create_mysql_dump.py --direct
   ```

## Part 8: Best Practices

1. **Security**
   - Use strong passwords
   - Limit user privileges
   - Encrypt sensitive data
   - Regular security audits
   - Use read-only users whenever possible
   - Implement connection encryption
   - Regular password rotation
   - Monitor access logs

2. **Performance**
   - Schedule dumps during off-hours
   - Monitor disk space
   - Regular maintenance
   - Index optimization
   - Use appropriate buffer sizes
   - Monitor connection limits

3. **Data Quality**
   - Regular data validation
   - Schema verification
   - Data consistency checks
   - Error monitoring
   - Verify data integrity
   - Check for data anomalies

4. **Documentation**
   - Keep credentials secure
   - Document any customizations
   - Maintain change log
   - Regular review of procedures
   - Document security measures
   - Track access patterns

## Success Criteria

✅ **Dump is working when:**
- All tables are present in local database
- Row counts match production
- No errors in dump process
- DLT pipeline can connect
- Security measures are in place
- Read-only access is enforced

✅ **Ready for DLT when:**
- Local database is accessible
- Schema matches expectations
- Data is consistent
- DLT pipeline runs successfully
- Security requirements are met
- Access controls are verified

Your MySQL dump setup is now ready to support your DLT pipeline! 