# DBT Dental Practice Analytics Platform

## Overview

The DBT Dental Practice project is an analytics engineering initiative that transforms OpenDental operational data into validated, standardized datasets for analytics and machine learning applications. This project transforms OpenDental's operational (OLTP) database into an analytics-ready (OLAP) data platform using DBT. By restructuring the data model, we enable advanced analytics, machine learning, and business intelligence capabilities while maintaining data integrity and standardization. This project serves as the foundation for improving dental clinic operations, financial performance, patient care, and data-driven decision making.

## Why OLTP to OLAP Transformation?

### 1. Operational Benefits
- **Performance Optimization**: Separate analytical queries from transactional systems
- **Data Quality Improvement**: Standardized validation catches data entry issues early
- **Historical Analysis**: Preserve full history of changes for trend analysis
- **Cross-System Integration**: Enable unified view across multiple dental practices
- **Validate database integrity** through comprehensive testing and documentation using dbt-core

### 2. Business Process Enhancement
- **Process Mining**: Identify bottlenecks and optimization opportunities
- **Workflow Analysis**: Track end-to-end patient journeys
- **Resource Utilization**: Optimize staff scheduling and equipment usage
- **Revenue Cycle Management**: Improve financial forecasting and AR management

### 3. Analytics Capabilities
- **Predictive Analytics**:
  - Patient no-show prediction
  - Treatment plan acceptance likelihood
  - Revenue forecasting
  - Insurance claim processing time estimation
- **Operational Analytics**:
  - Chair time utilization
  - Provider productivity metrics
  - Procedure profitability analysis
  - Patient acquisition cost analysis

### 4. Data Quality & Standardization
- **Consistent Data Entry**: Enforce standardized naming and coding conventions
- **Validation Rules**: Automated checks for business rule compliance
- **Error Detection**: Early identification of data quality issues
- **Documentation**: Comprehensive data dictionary and lineage tracking

### 5. Machine Learning Applications
- **Patient Segmentation**: Personalized treatment and communication strategies
- **Treatment Outcome Prediction**: Evidence-based treatment planning
- **Dynamic Pricing**: Optimize fee schedules based on market factors
- **Automated Coding**: Assist in procedure code assignment
- **Risk Stratification**: Identify high-risk patients for preventive care

## Project Architecture

### 1. Development Workflow
The project follows the dbt (data build tool) methodology with a three-layer architecture:
Our three-phase approach ensures data quality and business rule compliance:

#### Phase 1: Analysis
- Start with table DDL as ground truth
- Iterative exploratory analysis in dedicated directories
- Stakeholder collaboration for pattern identification
- Documentation of business rules and data patterns

#### Phase 2: Validation
- Convert analysis findings to automated tests
- Implement comprehensive validation rules
- Verify data integrity and relationships
- Document validation results

#### Phase 3: Implementation
- Create standardized staging models
- Apply business rules and transformations
- Implement testing framework
- Deploy to production

Major Business Systems: 
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
- **Python"" v3.8+ (for future ML components)

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

### Initial Setup

1. Clone the repository:
```bash
git clone https://github.com/your-org/dbt_dental_practice.git
cd dbt_dental_practice
```

2. Set up Python virtual environment and install dependencies:
```bash
# Install pipenv if you haven't already
pip install pipenv

# Create virtual environment and install dependencies
pipenv install

# Activate the virtual environment
pipenv shell
```

3. Create and run the environment setup script:
```powershell
# Create set_env.ps1
@"
# Read the .env file and set environment variables
Get-Content .env | ForEach-Object {
    if ($_ -match '^([^#][^=]+)=(.*)$') {
        $name = $matches[1].Trim()
        $value = $matches[2].Trim()
        [Environment]::SetEnvironmentVariable($name, $value, 'Process')
    }
}
"@ > set_env.ps1

# Run the script to load environment variables
. .\set_env.ps1
```

4. Install DBT packages:
```bash
# Install dbt packages
dbt deps
```

5. Configure database connection in `profiles.yml`

6. Run the models:
```bash
# Run all staging models
dbt run --models staging

# Run a specific model
dbt run --select stg_opendental__payment
```

7. Run tests:
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

This project is a collaborative effort between data engineers and dental practice domain experts. 
When contributing:

1. Follow the SQL naming conventions in `sql_naming_conventions.md`
2. Document all validation findings in the appropriate logs
3. Add comprehensive tests for all new models
4. Update documentation when changing business logic

## Contact

For questions or contributions, contact the project maintainer at [rains.bp@gmail.com].