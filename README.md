# DBT Dental Practice Analytics Platform

## Overview

The DBT Dental Practice project is an analytics engineering initiative that transforms OpenDental 
operational data into validated, standardized datasets for analytics and machine learning 
applications. This project transforms OpenDental's operational (OLTP) database into an 
analytics-ready (OLAP) data platform using DBT. By restructuring the data model, we enable advanced 
analytics, machine learning, and business intelligence capabilities while maintaining data integrity
 and standardization. This project serves as the foundation for improving dental clinic operations, 
 financial performance, patient care, and data-driven decision making.

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

These systems and their interconnections are visually represented in the 
`mdc_process_flow_diagram.md` document.

## Current Status

The project has made significant progress in both staging and intermediate layers, with mart layer development now underway:

### Completed Components ✅

1. **Staging Layer**
   - All OpenDental source tables validated and tested
   - Comprehensive data quality checks implemented
   - Historical data validation complete
   - Full documentation and lineage tracking

2. **Intermediate Layer - All Systems Complete**
   - **System A: Fee Processing & Verification**
     - `int_procedure_complete`: Comprehensive procedure model
     - `int_adjustments`: Fee adjustment tracking
     - `int_fee_model`: Fee processing and verification
   
   - **System B: Insurance Processing**
     - `int_claim_details`: Core insurance claim information
     - `int_claim_payments`: Detailed payment tracking
     - `int_claim_tracking`: Complete claim history
   
   - **System C: Payment Allocation & Reconciliation**
     - `int_payment_allocated`: Payment allocation model
     - Comprehensive validation rules implemented
     - Test coverage for edge cases

   - **System D: AR Analysis**
     - `int_ar_aging`: Accounts receivable aging model
     - AR metrics and aging analysis
     - Comprehensive validation rules

   - **System E: Collection Process**
     - `int_collection_activity`: Collection tracking model
     - Payment plan monitoring
     - Collection status tracking

   - **System F: Patient-Clinic Communications**
     - `int_communication_log`: Communication tracking model
     - Multi-channel interaction history
     - Response rate analysis

   - **System G: Scheduling & Referrals**
     - `int_appointment_metrics`: Appointment tracking model
     - Scheduling efficiency metrics
     - No-show analysis

   - **System H: System Logging**
     - `int_opendental_system_logs`: System activity tracking
     - User action audit trail
     - Log source analysis

3. **ETL Infrastructure**
   - Automated ETL pipeline for MySQL to PostgreSQL sync
   - Incremental processing with change tracking
   - Comprehensive error handling and logging
   - Data quality validation checks

### In Progress 🚧

1. **Mart Layer Development**
   - Initial implementation of core analytics models
   - Business requirements gathering and validation
   - Performance optimization strategies
   - Dashboard development planning
   - Key metrics definition and implementation

2. **Infrastructure Setup**
   - Docker containerization for development and production
   - Airflow DAG implementation for ETL orchestration
   - Monitoring and alerting system setup
   - Environment configuration and documentation

### Upcoming Work 📅

1. **Mart Layer Completion**
   - Complete implementation of core analytics models
   - Develop reporting views
   - Create data marts for specific business areas
   - Implement dashboard data models
   - Performance optimization and testing

2. **Frontend Development**
   - Begin implementation of web application
   - Develop interactive dashboards
   - Create user interfaces for data exploration
   - Implement role-based access control
   - Real-time analytics capabilities

3. **Documentation**
   - Update technical documentation
   - Create user guides for new models
   - Document data lineage and dependencies
   - Create dashboard documentation
   - API documentation for frontend integration

For detailed information about the intermediate models, see `dbt_int_models_plan.md`.

## Technical Implementation

### Key Components

- **MariaDB v11.6**: Source database platform for operational (OLTP) data
- **PostgreSQL**: Target database platform for analytics-ready (OLAP) data
- **ETL Pipeline**: Custom Python-based data transformation pipeline that:
  - Extracts data from MariaDB source tables
  - Performs type conversion and data validation
  - Handles schema evolution and index creation
  - Manages incremental syncs with tracking
  - Ensures data quality through comprehensive checks
- **dbt Core**: Data transformation framework for analytics models
- **DBeaver**: SQL development environment for exploratory analysis
- **Git**: Version control for all models and documentation
- **Python v3.8+**: For ETL pipeline and future ML components

### ETL Pipeline Features

The ETL pipeline in the `etl_pipeline` directory provides robust data transformation capabilities:

1. **Connection Management**
   - Centralized connection factory (`connection_factory.py`)
   - Secure environment variable handling
   - Read-only access enforcement for source database
   - SSL/TLS configuration support
   - Connection validation and testing utilities

2. **Data Type Handling**
   - Automatic conversion between MariaDB and PostgreSQL types
   - Special handling for date/time fields and NULL values
   - Boolean type detection and conversion
   - Numeric precision preservation

3. **Schema Management**
   - Automatic table creation in PostgreSQL
   - Schema validation and evolution
   - Index creation and optimization
   - Primary key preservation

4. **Data Quality**
   - Row count validation with configurable tolerance
   - NULL value handling and validation
   - Data type consistency checks
   - Comprehensive error logging

5. **Incremental Processing**
   - Sync status tracking
   - Incremental updates based on last modified timestamps
   - Chunked processing for large tables
   - Transaction management

6. **Error Handling**
   - Comprehensive error logging
   - Retry mechanisms
   - Quality issue tracking
   - Detailed execution summaries

### Directory Structure

```
dbt_dental_clinic/
├── analysis/                  # Ad-hoc analysis SQL queries
├── analysis_intermediate/     # Intermediate analysis workspace
├── api/                      # API integration components
├── config/                   # Configuration files
├── dbt_docs/                 # Generated dbt documentation
├── dbt_packages/             # Installed dbt packages
├── docs/                     # Project documentation
├── etl_pipeline/            # ETL pipeline components
│   ├── elt_pipeline.py      # Main ETL orchestration
│   ├── connection_factory.py # Database connection management
│   ├── test_connections.py  # Connection testing utilities
│   ├── debug_source_connection.py # Source connection debugging
│   ├── set_connection.sh    # Connection setup script (Unix)
│   ├── set_connection.ps1   # Connection setup script (Windows)
│   ├── set_env.ps1          # Environment setup script
│   └── .env.template        # Environment variables template
├── frontend/                 # Frontend components
├── logs/                     # Log files
├── macros/                   # Reusable SQL templates
├── models/                   # DBT models
│   ├── intermediate/         # Business process models
│   │   ├── cross_system/    # Cross-system integrations
│   │   ├── foundation/      # Core entity models
│   │   ├── system_a_fee_processing/
│   │   ├── system_b_insurance/
│   │   ├── system_c_payment/
│   │   ├── system_d_ar_analysis/
│   │   ├── system_e_collection/
│   │   ├── system_f_communications/
│   │   └── system_g_scheduling/
│   ├── marts/               # Business-specific analytical views
│   └── staging/             # Initial staging models
├── scripts/                 # Utility scripts
├── stakeholders/           # Stakeholder documentation
├── target/                 # DBT compilation output
├── tests/                  # Data quality tests
├── .gitignore
├── .user.yml
├── dbt_project.yml        # DBT project configuration
├── dependency_graph.txt   # Project dependencies
├── package-lock.yml
├── packages.yml          # Package dependencies
├── Pipfile              # Python dependencies
├── Pipfile.lock
├── profiles.yml         # Connection profiles
└── README.md           # Project documentation
```

### Frontend Development Plans

The project's long-term goal includes developing a modern web application to visualize and interact
with the analytics data warehouse. The frontend implementation will utilize:

- **React**: For building the user interface components
- **Vite**: For fast development and optimized production builds
- **FastAPI**: For building the backend API services
- **Modern UI/UX**: Following best practices for data visualization and user experience
- **Real-time Analytics**: Interactive dashboards and reports
- **Role-based Access**: Secure access control for different user types

The frontend will provide:
- Interactive dashboards for key performance indicators
- Drill-down capabilities for detailed analysis
- Custom report generation
- Data export functionality
- Real-time monitoring of dental practice metrics
- User-friendly interfaces for non-technical staff

Development of the frontend will begin after the completion of the core data warehouse and 
analytics models.

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

3. Set up environment variables:
```powershell
# Windows
Copy-Item etl_pipeline\.env.template etl_pipeline\.env
# Edit etl_pipeline\.env with your database credentials

# Unix/Linux
cp etl_pipeline/.env.template etl_pipeline/.env
# Edit etl_pipeline/.env with your database credentials
```

4. Test database connections:
```bash
# Windows
.\etl_pipeline\set_connection.ps1

# Unix/Linux
./etl_pipeline/set_connection.sh
```

5. Install DBT packages:
```bash
# Install dbt packages
dbt deps
```

6. Configure database connection in `profiles.yml`

7. Run the models:
```bash
# Run all staging models
dbt run --models staging

# Run a specific model
dbt run --select stg_opendental__payment
```

8. Run tests:
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