# DBT Dental Practice Project

## Overview

The DBT Dental Practice project is an analytics engineering initiative that transforms OpenDental operational data into validated, standardized datasets for analytics and machine learning. This project serves as the foundation for improving dental clinic operations, financial performance, patient care, and data-driven decision making.

## Project Purpose

We are working with dental clinics to:

- **Validate database integrity** through comprehensive testing and documentation
- **Standardize data models** for consistent reporting and analysis
- **Improve data entry practices** by identifying patterns and inconsistencies
- **Enable advanced analytics** for business intelligence and process optimization
- **Support machine learning initiatives** for:
  - Patient behavior prediction (appointment adherence, treatment acceptance)
  - Treatment outcome forecasting
  - Scheduling optimization
  - Insurance processing efficiency
  - Fee structure design and optimization
  - Patient targeting and segmentation

## Project Architecture

The project follows the dbt (data build tool) methodology with a three-layer architecture:

1. **Staging Layer**: Initial data validation and standardization
   - Enforces data quality standards
   - Standardizes naming conventions
   - Documents data patterns and anomalies
   - Implements basic business rules validation

2. **Intermediate Layer**: Business process modeling
   - Aligns with the seven core business systems (see Process Flow section)
   - Implements complex business logic
   - Creates cross-system data connections
   - Enables end-to-end process analysis

3. **Marts Layer**: Business-specific analytical views
   - Provider and production analytics
   - Financial performance metrics
   - Patient journey analytics
   - Operational efficiency KPIs

## Business Process Flow

Our data models are structured around seven interconnected business systems that represent the complete patient journey:

1. **System A: Fee Processing & Verification**
   - Setting and validating procedure fees
   - Managing fee schedules and contracted rates

2. **System B: Insurance Processing**
   - Claims creation and submission
   - Insurance payment estimation
   - Claim tracking and resolution

3. **System C: Payment Allocation & Reconciliation**
   - Payment processing and allocation
   - Managing payment splits across procedures
   - Transaction validation and reconciliation

4. **System D: AR Analysis**
   - Accounts receivable aging categorization
   - AR metric monitoring and alerting

5. **System E: Collection Process**
   - Managing outstanding balance collection
   - Payment plan creation and monitoring
   - Collection escalation workflows

6. **System F: Patient–Clinic Communications**
   - Multi-channel patient communication
   - Response tracking and follow-up
   - Communication effectiveness analysis

7. **System G: Scheduling & Referrals**
   - Appointment management
   - Referral tracking and conversion
   - Schedule optimization

These systems and their interconnections are visually represented in the `mdc_process_flow_diagram.md` document.

## Current Status

The project is currently focused on the **staging layer** with a systematic approach to validating all OpenDental source tables:

- **Completed**: Payment module validation with comprehensive testing
- **In Progress**: Core data entity validation (patients, procedures, appointments)
- **Upcoming**: Insurance, claims, and provider data validation

The intermediate and marts layers are in the planning stage, with detailed specifications available in `dbt_int_models_plan.md`.

## Technical Implementation

### Key Components

- **MariaDB v11.6**: Database platform for development and testing
- **dbt Core**: Data transformation framework
- **DBeaver**: SQL development environment for exploratory analysis
- **Git**: Version control for all models and documentation

### Directory Structure

```
dbt_dental_practice/
├── dbeaver_validation/       # DBeaver SQL scripts for initial exploration
├── docs/                     # Documentation and validation logs
├── models/                   # DBT models organized by layer
│   ├── staging/              # Initial validation models
│   ├── intermediate/         # Business process models (planned)
│   └── marts/                # Business-specific analytical views (planned)
├── tests/                    # Data quality tests and validations
├── macros/                   # Reusable SQL templates
└── seeds/                    # Static reference data
```

### Validation Workflow

We follow a structured validation workflow for each source table:

1. **Exploratory Analysis**: Initial data profiling in DBeaver
2. **Pattern Documentation**: Identifying and documenting data patterns
3. **Model Implementation**: Creating standardized dbt models
4. **Test Development**: Implementing data quality tests
5. **Documentation**: Comprehensive validation documentation

For detailed workflow steps, refer to `dbt_validation_workflow.md`.

## Getting Started

### Prerequisites

- MariaDB v11.6 or compatible database
- dbt Core installed
- Access to OpenDental database backup
- Python 3.8+ (for future ML components)

### Initial Setup

1. Clone the repository:
```bash
git clone https://github.com/your-org/dbt_dental_practice.git
cd dbt_dental_practice
```

2. Install dependencies:
```bash
# Install dbt packages
dbt deps
```

3. Configure database connection in `profiles.yml`

4. Run the models:
```bash
# Run all staging models
dbt run --models staging

# Run a specific model
dbt run --select stg_opendental__payment
```

5. Run tests:
```bash
# Test all models
dbt test

# Test a specific model
dbt test --select stg_opendental__payment
```

### Key Documentation

To understand the project better, review these key documents:

- `dbt_validation_workflow.md`: Detailed validation process
- `dbt_stg_models_plan.md`: Staging models plan and standards
- `dbt_int_models_plan.md`: Intermediate models plan (in development)
- `mdc_process_flow_diagram.md`: Business process flow documentation
- `sql_naming_conventions.md`: SQL coding standards

## Collaboration and Contribution

This project is a collaborative effort between data engineers and dental practice domain experts. When contributing:

1. Follow the SQL naming conventions in `sql_naming_conventions.md`
2. Document all validation findings in the appropriate logs
3. Add comprehensive tests for all new models
4. Update documentation when changing business logic

## Contact

For questions or contributions, contact the project maintainer at [rains.bp@gmail.com].