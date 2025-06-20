# MySQL Replication Setup Guide
## Production OpenDental to Local ETL Server

This guide walks through setting up MySQL binary log replication from your production OpenDental server to a local MySQL replica on your ETL server.

## Overview

```
OpenDental Production MySQL (Master) → Local MySQL Replica (Slave) → DLT Pipeline
```

## Prerequisites

- MySQL installed on both production and ETL servers
- Network connectivity between servers
- Administrative access to both MySQL instances
- Backup of production database (safety first!)

## Part 1: Configure Production Server (Master)

### Step 1: Enable Binary Logging

**On the production OpenDental server:**

Edit MySQL configuration file (`/etc/mysql/mysql.conf.d/mysqld.cnf` on Ubuntu/Debian or `/etc/my.cnf` on CentOS/RHEL):

```ini
[mysqld]
# Enable binary logging
log-bin=mysql-bin
server-id=1
binlog-format=ROW

# Optional: Set binlog retention (days)
expire_logs_days=7
max_binlog_size=100M

# For better replication performance
sync_binlog=1
innodb_flush_log_at_trx_commit=1
```

### Step 2: Restart MySQL Service

```bash
# Ubuntu/Debian
sudo systemctl restart mysql

# CentOS/RHEL
sudo systemctl restart mysqld

# Verify binary logging is enabled
mysql -u root -p -e "SHOW VARIABLES LIKE 'log_bin';"
```

Expected output:
```
+---------------+-------+
| Variable_name | Value |
+---------------+-------+
| log_bin       | ON    |
+---------------+-------+
```

### Step 3: Create Replication User

```sql
-- Connect to MySQL as root
mysql -u root -p

-- Create dedicated replication user
CREATE USER 'repl_user'@'%' IDENTIFIED BY 'strong_replication_password';

-- Grant replication privileges
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'repl_user'@'%';

-- Also grant SELECT for initial data dump
GRANT SELECT ON opendental.* TO 'repl_user'@'%';

-- Apply changes
FLUSH PRIVILEGES;

-- Test the user
SELECT User, Host FROM mysql.user WHERE User = 'repl_user';
```

### Step 4: Get Master Status

```sql
-- Lock tables to get consistent position
FLUSH TABLES WITH READ LOCK;

-- Record this information - you'll need it!
SHOW MASTER STATUS;
```

**Important:** Note down the `File` and `Position` values. Example:
```
+------------------+----------+--------------+------------------+-------------------+
| File             | Position | Binlog_Do_DB | Binlog_Ignore_DB | Executed_Gtid_Set |
+------------------+----------+--------------+------------------+-------------------+
| mysql-bin.000001 |      154 |              |                  |                   |
+------------------+----------+--------------+------------------+-------------------+
```

### Step 5: Create Initial Data Dump

**In a new terminal session (keep the lock active):**

```bash
# Create dump of OpenDental database
mysqldump \
  --single-transaction \
  --routines \
  --triggers \
  --master-data=2 \
  -u root -p \
  opendental > opendental_replica_dump.sql

# Transfer dump to ETL server
scp opendental_replica_dump.sql user@etl-server:/tmp/
```

### Step 6: Unlock Tables

**Back in the MySQL session:**
```sql
-- Release the lock
UNLOCK TABLES;
```

## Part 2: Configure ETL Server (Slave)

### Step 7: Install and Configure MySQL

**On your ETL server:**

```bash
# Install MySQL (Ubuntu/Debian)
sudo apt update
sudo apt install mysql-server

# Or CentOS/RHEL
sudo yum install mysql-server
# sudo dnf install mysql-server  # For newer versions

# Start MySQL
sudo systemctl start mysql
sudo systemctl enable mysql

# Secure installation
sudo mysql_secure_installation
```

### Step 8: Configure Slave Settings

Edit MySQL configuration (`/etc/mysql/mysql.conf.d/mysqld.cnf`):

```ini
[mysqld]
# Unique server ID (different from master)
server-id=2

# Enable relay logs
relay-log=relay-bin
relay-log-index=relay-bin.index

# Read-only (prevents accidental writes)
read-only=1

# Optional: Skip slave start on boot (for manual control)
skip-slave-start

# Replication settings
slave-skip-errors=1062  # Skip duplicate key errors
```

### Step 9: Restart MySQL and Import Data

```bash
# Restart MySQL
sudo systemctl restart mysql

# Create database
mysql -u root -p -e "CREATE DATABASE opendental_replica;"

# Import the dump
mysql -u root -p opendental_replica < /tmp/opendental_replica_dump.sql

# Verify import
mysql -u root -p -e "USE opendental_replica; SHOW TABLES; SELECT COUNT(*) FROM patient;"
```

### Step 10: Configure Replication

```sql
-- Connect to MySQL as root
mysql -u root -p

-- Stop slave (in case it's running)
STOP SLAVE;

-- Configure master connection
CHANGE MASTER TO
  MASTER_HOST='production_server_ip',
  MASTER_USER='repl_user',
  MASTER_PASSWORD='strong_replication_password',
  MASTER_LOG_FILE='mysql-bin.000001',  -- From Step 4
  MASTER_LOG_POS=154,                  -- From Step 4
  MASTER_CONNECT_RETRY=60;

-- Start replication
START SLAVE;

-- Check status
SHOW SLAVE STATUS\G
```

## Part 3: Verify Replication

### Step 11: Test Replication

**On production server:**
```sql
-- Create test table
USE opendental;
CREATE TABLE replication_test (id INT PRIMARY KEY, test_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
INSERT INTO replication_test (id) VALUES (1);
```

**On ETL server:**
```sql
-- Check if test table appeared
USE opendental_replica;
SHOW TABLES LIKE 'replication_test';
SELECT * FROM replication_test;

-- Clean up
DROP TABLE replication_test;
```

### Step 12: Monitor Replication Health

```sql
-- Check slave status
SHOW SLAVE STATUS\G

-- Key fields to monitor:
-- Slave_IO_Running: Yes
-- Slave_SQL_Running: Yes
-- Last_Error: (should be empty)
-- Seconds_Behind_Master: (should be low)
```

### Step 13: Create Monitoring Script

```bash
#!/bin/bash
# File: /home/user/monitor_replication.sh

MYSQL_CMD="mysql -u root -p${MYSQL_ROOT_PASSWORD} -e"

echo "=== MySQL Replication Status ==="
echo "Timestamp: $(date)"

# Check slave status
SLAVE_STATUS=$($MYSQL_CMD "SHOW SLAVE STATUS\G" | grep -E "(Slave_IO_Running|Slave_SQL_Running|Last_Error|Seconds_Behind_Master)")

echo "$SLAVE_STATUS"

# Check if replication is healthy
IO_RUNNING=$(echo "$SLAVE_STATUS" | grep "Slave_IO_Running" | awk '{print $2}')
SQL_RUNNING=$(echo "$SLAVE_STATUS" | grep "Slave_SQL_Running" | awk '{print $2}')

if [ "$IO_RUNNING" = "Yes" ] && [ "$SQL_RUNNING" = "Yes" ]; then
    echo "✅ Replication is healthy"
    exit 0
else
    echo "❌ Replication has issues"
    exit 1
fi
```

```bash
# Make executable
chmod +x /home/user/monitor_replication.sh

# Add to crontab for monitoring
crontab -e
# Add: */5 * * * * /home/user/monitor_replication.sh >> /var/log/replication_monitor.log 2>&1
```

## Part 4: Troubleshooting

### Common Issues

**1. Connection Refused**
```bash
# Check firewall
sudo ufw allow from etl_server_ip to any port 3306

# Check MySQL bind address
grep bind-address /etc/mysql/mysql.conf.d/mysqld.cnf
# Should be 0.0.0.0 or specific IP, not 127.0.0.1
```

**2. Access Denied**
```sql
-- Verify user exists and has correct permissions
SELECT User, Host FROM mysql.user WHERE User = 'repl_user';
SHOW GRANTS FOR 'repl_user'@'%';
```

**3. Replication Lag**
```sql
-- Check for long-running queries
SHOW PROCESSLIST;

-- Check binlog events
SHOW BINLOG EVENTS IN 'mysql-bin.000001' LIMIT 10;
```

**4. Duplicate Key Errors**
```sql
-- Skip single error and continue
STOP SLAVE; 
SET GLOBAL SQL_SLAVE_SKIP_COUNTER = 1; 
START SLAVE;
```

### Recovery Procedures

**If replication breaks:**

1. **Stop slave**
   ```sql
   STOP SLAVE;
   ```

2. **Reset slave**
   ```sql
   RESET SLAVE ALL;
   ```

3. **Re-dump and restart** (follow steps 4-10 again)

## Part 5: Security Considerations

### Network Security
```bash
# Restrict MySQL port access
sudo ufw allow from production_server_ip to any port 3306
sudo ufw deny 3306
```

### User Security
```sql
-- Create read-only user for DLT
CREATE USER 'etl_readonly'@'localhost' IDENTIFIED BY 'etl_password';
GRANT SELECT ON opendental_replica.* TO 'etl_readonly'@'localhost';

-- Remove replication user's SELECT permission (if not needed)
REVOKE SELECT ON opendental.* FROM 'repl_user'@'%';
```

### Data Security
```sql
-- Enable SSL for replication (optional but recommended)
CHANGE MASTER TO
  MASTER_HOST='production_server_ip',
  MASTER_USER='repl_user',
  MASTER_PASSWORD='strong_replication_password',
  MASTER_LOG_FILE='mysql-bin.000001',
  MASTER_LOG_POS=154,
  MASTER_SSL=1,
  MASTER_SSL_CA='/path/to/ca.pem',
  MASTER_SSL_CERT='/path/to/client-cert.pem',
  MASTER_SSL_KEY='/path/to/client-key.pem';
```

## Part 6: Maintenance

### Daily Tasks
- Monitor replication lag
- Check error logs
- Verify data consistency (sample checks)

### Weekly Tasks
- Review binlog disk usage
- Test failover procedures
- Update replication monitoring

### Monthly Tasks
- Full data consistency check
- Review security settings
- Update documentation

## Environment Variables for DLT

After replication is set up, update your `.env` file:

```bash
# Source MySQL (now points to replica)
SOURCE_MYSQL_HOST=localhost
SOURCE_MYSQL_PORT=3306
SOURCE_MYSQL_USER=etl_readonly
SOURCE_MYSQL_PASSWORD=etl_password
SOURCE_MYSQL_DB=opendental_replica

# Analytics PostgreSQL (unchanged)
ANALYTICS_POSTGRES_HOST=localhost
ANALYTICS_POSTGRES_PORT=5432
ANALYTICS_POSTGRES_USER=analytics_user
ANALYTICS_POSTGRES_PASSWORD=analytics_password
ANALYTICS_POSTGRES_DB=opendental_analytics
ANALYTICS_POSTGRES_SCHEMA=raw
```

## Success Criteria

✅ **Replication is working when:**
- `SHOW SLAVE STATUS` shows both IO and SQL threads running
- Test data inserted on production appears on replica within seconds
- No errors in MySQL error logs
- Monitoring script reports healthy status

✅ **Ready for DLT when:**
- DLT can connect to replica database
- Sample queries return expected data
- Performance impact on production is eliminated

Your MySQL replication is now ready to support your DLT pipeline!