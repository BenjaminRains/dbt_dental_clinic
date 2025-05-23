# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [2025-05-22]
### Added
- Insurance subscriber data quality report
- Test string length validation macro
- Test column greater than validation macro
- Timestamp not future validation test macro
- Custom string not empty validation macro
- Comprehensive test suite for insurance subscriber model
- Required metadata fields to insurance subscriber model
- Metadata columns to insurance plan model
- Metadata columns to insurance bluebook model
- Standardized metadata to historical appointment model
- Metadata columns to fee staging model
- Metadata columns to EOB attachment model
- Metadata columns to entrylog model
- Metadata columns to employer model
- Enhanced employee metadata tracking
- Metadata columns to document staging model
- Metadata columns to disease definition staging model
- Metadata columns to disease staging model
- Required metadata columns to commlog model
- Required metadata columns to codegroup model
- Metadata fields to claimtracking model
- Metadata fields to claimsnapshot model
- Metadata fields to claimproc model
- Comprehensive test suite for claim payment model
- Comprehensive data quality tests for dental claims
- Medication staging model and enhanced allergydef
- Metadata columns to allergy staging model

### Changed
- Converted insurance bluebook log to incremental model
- Converted fee schedule model to view
- Converted EOB attachment model to incremental
- Converted document staging model to incremental table
- Standardized boolean conversions in autocode staging model
- Standardized metadata column naming conventions

### Fixed
- Updated boolean tests in inssub model to use smallint values
- Handled historical records in insurance subscriber tests
- Corrected expression_is_true usage for timestamp validations
- Standardized date validation tests
- Corrected expression_is_true syntax for effective_date
- Standardized timestamp validation across all timestamp columns
- Replaced expression_is_true with test_timestamp_not_future
- Updated user_entry_id relationship test in insurance plan model
- Updated _created_at test in insurance plan model
- Updated insurance plan relationship tests in historical appointments
- Updated carrier relationship test in insurance plan model
- Updated employer relationship test in insurance plan model
- Updated fee schedule relationship tests in insurance plan model
- Updated manual fee schedule relationship test in insurance plan model
- Corrected metadata timestamp handling in insurance bluebook
- Aligned appointment_status tests and docs with valid temporary states
- Updated appointment status codes and test severity
- Updated histappointment_missing_appointments test macro
- Updated appointment_datetime not_null test and documentation
- Updated entry_datetime not_null test and documentation
- Updated assistant_id relationship test and documentation
- Updated user relationship tests for system-generated records
- Updated hygienist_id relationship test and documentation
- Updated insurance_plan_1_id relationship test and documentation
- Updated insurance_plan_2_id relationship test and documentation
- Corrected insurance plan relationship test for no secondary insurance
- Corrected provider relationship test for unassigned providers
- Resolved duplicate test and enhanced missing appointments monitoring
- Enforced program_id referential integrity
- Updated relationship tests for commlog and userod
- Removed non-existent metadata columns from codegroup model
- Resolved syntax error in claimtracking model documentation
- Fixed creation date handling in claimproc
- Adjusted check number uniqueness test and documented patterns
- Corrected data quality test logic for claim statuses
- Corrected claim status data quality tests
- Corrected dbt_utils.expression_is_true test syntax
- Corrected is_employment_related validation based on actual data
- Corrected syntax errors in boolean flag expressions
- Corrected syntax errors in multiple validation tests
- Corrected syntax error in deductible_applied validation test
- Corrected syntax error in insurance_payment_amount validation test
- Corrected syntax error in insurance_payment_estimate validation test
- Corrected syntax error in ortho_remaining_months validation test
- Corrected syntax error in radiographs validation test
- Updated received_date test to handle multiple send dates
- Fixed SQL generation error in date validation tests
- Corrected service_date test syntax in claims model
- Corrected write_off test syntax in claims model
- Updated patient relationship test in claims model
- Updated dbt tests to handle data patterns and edge cases
- Handled legacy carrier records with missing creation dates

### Docs
- Added comprehensive documentation for insurance subscriber data quality
- Enhanced commlog communication_source documentation with detailed patterns
- Enhanced commlog is_sent documentation with detailed distribution
- Enhanced commlog mode documentation with usage patterns
- Clarified date_issued field behavior in claim payment model
- Enhanced payment type documentation with carrier patterns
- Enhanced claim_type documentation with data analysis
- Added comprehensive claim status documentation
- Added medication table DDL files for data lineage
- Added note about adjustment validation failures
- Added note about EOB attachment relationship test warnings

### Removed
- Removed reference to non-existent staging model
- Removed clinic_id field tests (unused by MDC clinic)
- Removed dbt_utils tests from claim payment model

## [2025-05-21]
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
