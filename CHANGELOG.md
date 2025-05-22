# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
### Added
- ETL orchestration DAGs for MariaDB to PostgreSQL sync
- Incremental ETL script for MariaDB to PostgreSQL sync
- New patient rate metric to appointment metrics
- 'None' appointment type (id 0) to stg_opendental__appointmenttype

### Changed
- Moved DAG file to standard Airflow directory structure
- Removed clinic_id from appointment metrics
- Updated intermediate layer completion status (staging models complete)

### Fixed
- System logging model alignment with actual data structure
- User group ID null handling in system logs
- Date validation tests in int_opendental_system_logs
- Column names and test conditions in int_opendental_system_logs
- Type casting and join issues in int_opendental_system_logs
- Appointment view clinic_id and view_updated_at test expressions
- User preferences model tests and column references
- Task management model uniqueness and timestamp validations
- Provider availability tests and schedule date range handling
- Appointment schedule utilization calculations and tests
- Appointment metrics handling for null values and future dates
- Actual length calculation and time calculations in appointment details
- Duplicate schedule_id and missing clinic_id issues

### Docs
- Added comprehensive ETL and data refresh strategy
- Added mart layer implementation plan
- Added comprehensive Airflow setup and usage guide
- Updated intermediate models documentation
- Updated task management documentation and tests
- Clarified appointment_type_id = 0 meaning in _int_appointment_details.yml

## [2025-05-20]
### Added
- Appointment view staging model and DDL files
- System G scheduling module enhancements
- Appointment metrics model and comprehensive test suite
- Appointment schedule and provider availability models with tests
- User preferences and task management models integrated into System G
- `system_h` logging model
- EOB attachments, employer models, and insurance integration
- Entrylog, tasklist, tasknote, tasksubscription, and taskunread staging models
- Source definitions for EOB, entrylog, employer, task, taskhist, and timeadjust

### Changed
- Centralized pattern length logic in scheduling models
- Improved model relationships across scheduling module
- Reorganized dbt tests in communication templates model
- Updated test format and logic for `communication_datetime`, `is_automated`, and template type handling
- Enhanced communication metrics model structure and documentation

### Fixed
- CASE statement type mismatches in appointment metrics
- Column references and timestamps in appointment details and task management models
- Validation structure in `_int_communication_templates.yml` and related dbt_utils test expressions
- Filtering logic for EOB attachments, task notes, tasklist, and taskhist staging models
- Handling of OpenDental appointment patterns and `user_id` edge cases

### Docs
- Enhanced documentation for all major scheduling models (user preferences, task management, provider availability, appointment schedule, int_appointment_details)
- Clarified grain definitions and model-specific metadata

### Removed
- Removed legacy model (unspecified in log message)

### Added
- 

### Changed
- 

### Fixed
- 

### Deprecated
- 

### Removed
- 

### Security
- 

## [2025-05-19]
### Added
- Advanced trigram similarity logic for communication template matching
- YAML tests for email metrics validation (opens, clicks, bounces)

### Changed
- Refactored communication templates to include PbN and SMS sources
- Improved incremental filtering in communication metrics

### Fixed
- Template subject validation to only require non-null for email
- birth_date and communication_datetime test logic

### Deprecated
- Removed `int_patient_communications` model and YAML
- Migrated logic into `int_patient_communications_base`

## Guidelines for Contributors
- Each change should be documented in the [Unreleased] section
- When releasing a new version, move all changes from [Unreleased] to a new version section
- Use the following categories:
  - Added: for new features
  - Changed: for changes in existing functionality
  - Fixed: for any bug fixes
  - Deprecated: for soon-to-be removed features
  - Removed: for now removed features
  - Security: for vulnerability fixes
- Each entry should be concise and clear
- Include issue/PR numbers when applicable (e.g., "Fixed template validation (#123)")
- Keep the most recent version at the top
