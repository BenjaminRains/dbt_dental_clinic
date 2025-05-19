# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
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
