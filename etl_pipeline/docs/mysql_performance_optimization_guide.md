# MySQL Performance Optimization Guide

## Overview

This guide explains how to enable the MySQL performance optimizations that are currently being
 skipped due to privilege restrictions. When properly configured, these optimizations can
  significantly improve ETL pipeline performance for bulk operations.

## Current Status

The ETL pipeline is currently showing warnings like:
```
innodb_flush_log_at_trx_commit requires GLOBAL privileges, skipping
sync_binlog requires GLOBAL privileges, skipping
```

These warnings indicate that the pipeline is gracefully handling privilege restrictions, but
 performance optimizations are not being applied.

## Performance Settings Being Applied

The ETL pipeline attempts to set the following MySQL session variables for optimal bulk operation
 performance:

### Critical Settings
- `bulk_insert_buffer_size = 268435456` (256MB) - **Currently working**
- `autocommit = 0` - **Currently working**
- `unique_checks = 0` - **Currently working**
- `foreign_key_checks = 0` - **Currently working**
- `sql_mode = 'NO_AUTO_VALUE_ON_ZERO'` - **Currently working**

### Settings Requiring GLOBAL Privileges
- `innodb_flush_log_at_trx_commit = 2` - **Currently skipped**
- `sync_binlog = 0` - **Currently skipped**

## Step-by-Step Implementation Guide

### Option 1: Grant GLOBAL Privileges to ETL User (Recommended)

#### For Source Database (Remote OpenDental Server) - Root Access Available ✅

Since you have root privileges to the source database, this is the most flexible approach:

1. **Connect to MySQL as root:**
   ```sql
   mysql -u root -p
   ```

2. **Grant GLOBAL privileges to the ETL user:**
   ```sql
   -- Replace 'your_etl_user' and 'your_password' with your actual ETL credentials
   GRANT SUPER ON *.* TO 'your_etl_user'@'%' IDENTIFIED BY 'your_password';
   FLUSH PRIVILEGES;
   ```

3. **Verify privileges:**
   ```sql
   SHOW GRANTS FOR 'your_etl_user'@'%';
   ```

4. **Test the ETL user can set session variables:**
   ```sql
   -- Connect as your ETL user
   mysql -u your_etl_user -p
   
   -- Test setting the problematic variables
   SET SESSION innodb_flush_log_at_trx_commit = 2;
   SET SESSION sync_binlog = 0;
   
   -- If no errors, you're all set!
   ```

#### For Target Database (Localhost Replication)

**Do you have root access to your localhost MySQL instance?**

If YES:
1. **Connect to local MySQL as root:**
   ```sql
   mysql -u root -p
   ```

2. **Grant GLOBAL privileges:**
   ```sql
   -- Replace 'your_etl_user' and 'your_password' with your actual ETL credentials
   GRANT SUPER ON *.* TO 'your_etl_user'@'localhost' IDENTIFIED BY 'your_password';
   FLUSH PRIVILEGES;
   ```

3. **Verify privileges:**
   ```sql
   SHOW GRANTS FOR 'your_etl_user'@'localhost';
   ```

If NO (or if you prefer not to grant SUPER privileges):
- Use Option 2 (Global Configuration) or Option 3 (Runtime) below

### Option 2: Set Global Variables in MySQL Configuration

#### For Source Database (Root Access Available ✅)

Since you have root access, you can modify the MySQL configuration directly:

1. **Find the MySQL configuration file:**
   ```bash
   # Common locations (try these in order):
   sudo find /etc -name "my.cnf" -o -name "mysqld.cnf" 2>/dev/null
   sudo find /etc/mysql -name "*.cnf" 2>/dev/null
   
   # Or check these common paths:
   ls -la /etc/mysql/mysql.conf.d/mysqld.cnf
   ls -la /etc/mysql/my.cnf
   ls -la /etc/my.cnf
   ```

2. **Edit the configuration file:**
   ```bash
   # Replace with the actual path found above
   sudo nano /etc/mysql/mysql.conf.d/mysqld.cnf
   ```

3. **Add or modify the [mysqld] section:**
   ```ini
   [mysqld]
   # Existing settings...
   
   # ETL Performance Optimizations
   innodb_flush_log_at_trx_commit = 2
   sync_binlog = 0
   bulk_insert_buffer_size = 268435456
   
   # Additional performance settings
   innodb_buffer_pool_size = 2G
   innodb_log_file_size = 512M
   innodb_flush_method = O_DIRECT
   ```

4. **Restart MySQL service:**
   ```bash
   # Linux/Unix
   sudo systemctl restart mysql
   
   # Windows
   net stop mysql
   net start mysql
   ```

5. **Verify the settings took effect:**
   ```sql
   mysql -u root -p
   SHOW GLOBAL VARIABLES LIKE 'innodb_flush_log_at_trx_commit';
   SHOW GLOBAL VARIABLES LIKE 'sync_binlog';
   ```

#### For Target Database (Localhost)

**Do you have root access to your localhost MySQL instance?**

If YES:
1. **Find and edit the local MySQL configuration file** (same steps as above)
2. **Add the same [mysqld] section**
3. **Restart local MySQL service**

If NO:
- Use Option 3 (Runtime) below, or
- Ask your system administrator to apply these settings

### Option 3: Set Variables at Runtime (Temporary)

#### For Source Database (Root Access Available ✅)

Since you have root access, you can set these variables immediately:

1. **Connect as root:**
   ```sql
   mysql -u root -p
   ```

2. **Set global variables:**
   ```sql
   SET GLOBAL innodb_flush_log_at_trx_commit = 2;
   SET GLOBAL sync_binlog = 0;
   ```

3. **Verify settings:**
   ```sql
   SHOW GLOBAL VARIABLES LIKE 'innodb_flush_log_at_trx_commit';
   SHOW GLOBAL VARIABLES LIKE 'sync_binlog';
   ```

4. **Test immediately:**
   ```bash
   cd etl_pipeline
   etl-run --tables securitylog
   ```

**Note:** Runtime changes are temporary and will reset after MySQL restart. Use this for quick testing, then implement Option 1 or 2 for permanent changes.

#### For Target Database (Localhost)

**Do you have root access to your localhost MySQL instance?**

If YES:
```sql
mysql -u root -p
SET GLOBAL innodb_flush_log_at_trx_commit = 2;
SET GLOBAL sync_binlog = 0;
SHOW GLOBAL VARIABLES LIKE 'innodb_flush_log_at_trx_commit';
```

If NO:
- Ask your system administrator to set these variables, or
- Use Option 2 (Global Configuration) if you can modify the config file

## Verification Steps

### 1. Test ETL User Privileges

```sql
-- Connect as your ETL user
mysql -u your_etl_user -p

-- Test setting session variables
SET SESSION innodb_flush_log_at_trx_commit = 2;
SET SESSION sync_binlog = 0;

-- If no errors, privileges are working
```

### 2. Run ETL Pipeline Test

```bash
cd etl_pipeline
etl-run --tables securitylog
```

**Expected Output (No Warnings):**
```
INFO: Applied MySQL performance optimizations
INFO: Using optimized large table processing for securitylog
INFO: Batch 1: 100,000 rows in 3.45s (28985 rows/sec)
```

### 3. Monitor Performance Improvement

Compare performance metrics:
- **Before**: ~19,146 rows/sec
- **After**: ~28,985+ rows/sec (expected improvement)

## Security Considerations

### Option 1 (GRANT SUPER) - Most Flexible ✅ **Recommended for Your Setup**
- **Pros**: ETL pipeline can dynamically adjust settings, works with your root access
- **Cons**: Gives broad system privileges to ETL user
- **Recommendation**: Use dedicated ETL user with minimal required privileges
- **Your Situation**: Since you have root access to source database, this is the most flexible approach

### Option 2 (Global Configuration) - Most Secure
- **Pros**: No additional user privileges needed, affects all connections
- **Cons**: Settings are static and affect all connections
- **Recommendation**: Good for production environments where you want consistent settings
- **Your Situation**: Works well if you want to set these optimizations for all applications

### Option 3 (Runtime) - Temporary Solution
- **Pros**: Quick to implement, immediate testing
- **Cons**: Requires manual intervention after each restart
- **Recommendation**: Use for quick testing, then implement permanent solution
- **Your Situation**: Perfect for immediate testing since you have root access

## Troubleshooting

### Common Issues

1. **"Access denied" when setting variables:**
   ```sql
   -- Check current privileges
   SHOW GRANTS FOR 'your_etl_user'@'%';
   
   -- Grant specific privileges if needed (since you have root access)
   GRANT SYSTEM_VARIABLES_ADMIN ON *.* TO 'your_etl_user'@'%';
   GRANT SUPER ON *.* TO 'your_etl_user'@'%';
   FLUSH PRIVILEGES;
   ```

2. **Variables not persisting after restart:**
   - Ensure variables are set in MySQL configuration file
   - Check file permissions and syntax

3. **Performance not improving:**
   - Verify variables are actually set: `SHOW GLOBAL VARIABLES LIKE 'innodb_flush_log_at_trx_commit';`
   - Check for other bottlenecks (network, disk I/O, CPU)

### Verification Commands

```sql
-- Check current settings
SHOW GLOBAL VARIABLES LIKE 'innodb_flush_log_at_trx_commit';
SHOW GLOBAL VARIABLES LIKE 'sync_binlog';
SHOW GLOBAL VARIABLES LIKE 'bulk_insert_buffer_size';

-- Check user privileges
SHOW GRANTS FOR 'your_etl_user'@'%';
SHOW GRANTS FOR 'your_etl_user'@'localhost';
```

## Performance Impact

### Expected Improvements

- **Bulk Insert Speed**: 30-50% faster
- **Memory Usage**: More efficient buffer utilization
- **Disk I/O**: Reduced write operations during bulk operations
- **Overall Pipeline**: 20-40% faster completion times

### Monitoring

Monitor these metrics after implementation:
- Rows processed per second
- Total pipeline execution time
- Database server resource usage (CPU, memory, I/O)
- Network transfer rates

## Rollback Plan

If issues arise, you can quickly rollback:

1. **Remove SUPER privileges:**
   ```sql
   REVOKE SUPER ON *.* FROM 'your_etl_user'@'%';
   REVOKE SUPER ON *.* FROM 'your_etl_user'@'localhost';
   FLUSH PRIVILEGES;
   ```

2. **Reset global variables:**
   ```sql
   SET GLOBAL innodb_flush_log_at_trx_commit = 1;
   SET GLOBAL sync_binlog = 1;
   ```

3. **Restore original MySQL configuration**

The ETL pipeline will continue working with graceful degradation (current behavior).

## Quick Start for Your Setup

Since you have root access to the source database, here's the fastest way to test the optimizations:

### Immediate Testing (Option 3 - Runtime)
```sql
-- Connect to source database as root
mysql -u root -p

-- Set the performance variables
SET GLOBAL innodb_flush_log_at_trx_commit = 2;
SET GLOBAL sync_binlog = 0;

-- Verify they're set
SHOW GLOBAL VARIABLES LIKE 'innodb_flush_log_at_trx_commit';
SHOW GLOBAL VARIABLES LIKE 'sync_binlog';
```

Then test your ETL pipeline:
```bash
cd etl_pipeline
etl-run --tables securitylog
```

**Expected Result**: No more warnings about GLOBAL privileges, and improved performance!

### Permanent Solution (Option 1 - Recommended)
After testing, implement the permanent solution by granting SUPER privileges to your ETL user.

## Conclusion

Implementing these optimizations can significantly improve ETL pipeline performance. Since you have root access to the source database, you have maximum flexibility to implement any of these approaches. The pipeline is designed to work correctly regardless of whether these optimizations are enabled, so you can implement them gradually and monitor the results. 