# ETL Schema Update Command Feature

## Overview
This feature document outlines the implementation of a new ETL command that will automatically update the database schema configuration by running the `analyze_opendental_schema.py` script. This command will help resolve schema drift issues and keep the ETL configuration synchronized with the current OpenDental database structure.

## Problem Statement
The current ETL pipeline experiences schema drift issues where:
- Database schemas evolve over time (columns added/removed/modified)
- ETL configuration becomes outdated and references non-existent columns
- Manual schema analysis is required to resolve extraction failures
- No automated way to update schema configuration

## Proposed Solution
Add a new ETL command `etl-update-schema` that:
1. Runs the existing `analyze_opendental_schema.py` script
2. Generates updated `tables.yml` configuration
3. Provides feedback on schema changes detected
4. Integrates with existing ETL CLI infrastructure

## Feature Requirements

### Functional Requirements
1. **Command Interface**: Add `etl-update-schema` command to ETL CLI
2. **Schema Analysis**: Execute `analyze_opendental_schema.py` script
3. **Configuration Update**: Generate new `tables.yml` with current schema
4. **Change Detection**: Report what schema changes were detected
5. **Backup Creation**: Create backup of existing configuration in `logs/schema_analysis/` directory
6. **Validation**: Verify new configuration is valid before applying

### Non-Functional Requirements
1. **Performance**: Complete schema analysis within reasonable time (< 30 minutes)
2. **Reliability**: Handle database connection failures gracefully
3. **Usability**: Provide clear progress indicators and error messages
4. **Integration**: Work with existing ETL environment management
5. **Safety**: Never overwrite configuration without backup

## Implementation Plan

### Phase 1: Command Structure
1. **Add new command to CLI**: Extend `etl_pipeline/etl_pipeline/cli/commands.py`
2. **Command signature**: `etl-update-schema [--backup] [--force] [--dry-run]`
3. **Environment handling**: Use existing environment management
4. **Error handling**: Integrate with existing error handling patterns

### Phase 2: Schema Analysis Integration
1. **Script execution**: Call `analyze_opendental_schema.py` as subprocess
2. **Progress monitoring**: Capture and display analysis progress
3. **Output handling**: Parse analysis results and configuration updates
4. **Error propagation**: Handle and report analysis failures

### Phase 3: Configuration Management
1. **Backup creation**: Save current `tables.yml` with timestamp to `logs/schema_analysis/`
2. **Configuration validation**: Verify new configuration syntax
3. **Safe replacement**: Only replace if new configuration is valid
4. **Rollback capability**: Provide option to restore from backup

### Phase 4: Change Reporting
1. **Schema diff**: Compare old vs new configuration
2. **Change summary**: Report tables added/removed/modified
3. **Column analysis**: Report column changes per table
4. **Impact assessment**: Identify potential ETL impact

## Command Design

### Command Syntax
```bash
etl-update-schema [OPTIONS]

Options:
  --backup          Create backup of current configuration (default: true)
  --force           Force update even if errors detected (default: false)
  --output-dir      Specify output directory for configuration (default: etl_pipeline/config)
  --log-level       Set logging level (default: INFO)
  --help            Show help message
```

### Command Flow
1. **Environment Setup**: Load ETL environment variables
2. **Pre-flight Checks**: Verify database connectivity and permissions
3. **Backup Creation**: Create timestamped backup of current configuration in `logs/schema_analysis/`
4. **Schema Analysis**: Execute `analyze_opendental_schema.py`
5. **Configuration Validation**: Verify new configuration is valid
6. **Change Detection**: Compare old vs new configuration
7. **Update Application**: Replace configuration if valid
8. **Summary Report**: Display changes and next steps

### Example Usage
```bash
# Basic schema update
etl-update-schema

# Force update even with warnings
etl-update-schema --force

# Update with custom output directory
etl-update-schema --output-dir /custom/path
```

## Integration Points

### Existing ETL Infrastructure
1. **CLI Framework**: Extend existing command structure in `commands.py`
2. **Environment Management**: Use existing environment loading
3. **Logging**: Integrate with existing logging infrastructure
4. **Error Handling**: Use existing error handling patterns
5. **Configuration**: Leverage existing configuration management

### Schema Analysis Script
1. **Script Location**: `etl_pipeline/scripts/analyze_opendental_schema.py`
2. **Execution Method**: Run as subprocess with proper environment
3. **Output Parsing**: Parse analysis results and configuration files
4. **Error Handling**: Capture and handle script execution errors

## Expected Output

### Success Case
```
ETL Schema Update
=================

Environment: production
Database: opendental
Configuration: etl_pipeline/config/tables.yml

[INFO] Creating backup: logs/schema_analysis/tables.yml.backup.20251006_143022
[INFO] Running schema analysis...
[INFO] Analyzing 432 tables...
[INFO] Schema analysis complete in 18.3s

Schema Changes Detected:
- Tables modified: 7
- Columns removed: 4
- Columns added: 2
- Tables with strategy changes: 3

Configuration updated successfully!
Backup saved: logs/schema_analysis/tables.yml.backup.20251006_143022

Next steps:
1. Test ETL with updated configuration: etl-run --tables appointment,statement,document
2. Monitor for any remaining issues
3. Run full ETL pipeline when ready
```

### Error Case
```
ETL Schema Update
=================

[ERROR] Database connection failed: Connection timeout
[ERROR] Cannot proceed with schema analysis

Troubleshooting:
1. Check database connectivity
2. Verify environment variables
3. Ensure database permissions

Configuration unchanged.
```

## Benefits

### Immediate Benefits
1. **Automated Schema Sync**: No manual intervention required
2. **Error Prevention**: Catch schema drift before ETL failures
3. **Configuration Safety**: Automatic backup and validation
4. **Change Visibility**: Clear reporting of schema changes

### Long-term Benefits
1. **Reduced Maintenance**: Automated schema management
2. **Improved Reliability**: Fewer ETL failures due to schema issues
3. **Better Monitoring**: Regular schema health checks
4. **Faster Recovery**: Quick resolution of schema drift issues

## Implementation Considerations

### Technical Considerations
1. **Script Execution**: Handle subprocess execution and output capture
2. **File Management**: Safe configuration file replacement with backups in `logs/schema_analysis/`
3. **Error Recovery**: Graceful handling of analysis failures
4. **Performance**: Monitor analysis execution time

### Operational Considerations
1. **Scheduling**: Consider automated schema updates
2. **Monitoring**: Alert on schema changes
3. **Documentation**: Update ETL documentation
4. **Training**: Educate users on new command

## Testing Strategy

### Unit Tests
1. **Command parsing**: Test command line argument handling
2. **Script execution**: Test subprocess execution
3. **Configuration validation**: Test configuration file validation
4. **Error handling**: Test error scenarios

### Integration Tests
1. **End-to-end flow**: Test complete command execution
2. **Database connectivity**: Test with real database
3. **Configuration updates**: Test actual configuration changes
4. **Backup/restore**: Test backup and rollback functionality

### User Acceptance Tests
1. **Command usability**: Test command interface
2. **Output clarity**: Test output readability
3. **Error messages**: Test error message clarity
4. **Documentation**: Test documentation completeness

## Future Enhancements

### Phase 2 Features
1. **Automated Scheduling**: Schedule regular schema updates
2. **Change Notifications**: Email/Slack notifications for schema changes
3. **Schema Validation**: Pre-ETL schema validation
4. **Rollback Commands**: Easy rollback to previous configurations

### Advanced Features
1. **Schema Versioning**: Track schema change history
2. **Impact Analysis**: Analyze ETL impact of schema changes
3. **Migration Scripts**: Generate migration scripts for schema changes
4. **Schema Monitoring**: Continuous schema health monitoring

## Conclusion
The `etl-update-schema` command will provide a critical tool for maintaining ETL configuration synchronization with evolving database schemas. This feature addresses the immediate need for automated schema management while providing a foundation for more advanced schema monitoring and management capabilities.

The implementation leverages existing ETL infrastructure and the proven `analyze_opendental_schema.py` script, ensuring reliability and consistency with current ETL practices.
