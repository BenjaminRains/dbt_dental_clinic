# dbt Dental Models

A comprehensive dbt project transforming OpenDental practice management data into an analytics-ready data warehouse with 157+ models across staging, intermediate, and mart layers.

## 📊 Overview

This dbt project implements a modern analytics engineering approach for dental practice data, transforming raw OpenDental source tables into business-ready analytical models. The project follows a three-layer architecture pattern with standardized data transformations, comprehensive testing, and complete documentation.

**Status**: ✅ **157 models - All Passing** (PASS=157, WARN=0, ERROR=0, SKIP=0)

## 🏗️ Architecture

### Data Flow

```
OpenDental (MySQL) → ETL Pipeline → PostgreSQL Raw → dbt Transformations → Analytics Warehouse
    432 Tables         Replication      (Raw Layer)    (Staging/Int/Marts)   (Business Ready)
```

### Three-Layer Architecture

#### 1. **Staging Layer** (`models/staging/`)
**Purpose**: Standardize and clean raw source data

- **88+ Models** (views)
- Converts OpenDental conventions to PostgreSQL standards
- Handles data type conversions (boolean, dates, strings)
- Standardizes naming conventions (snake_case)
- Adds metadata tracking columns (`_loaded_at`, `_transformed_at`)
- Located in `staging.opendental` schema

**Key Features**:
- Boolean conversion from TINYINT (0/1) to PostgreSQL booleans
- Date cleaning (removes sentinel values like `0001-01-01`)
- String normalization (trims whitespace, converts empty strings to NULL)
- Metadata column standardization

#### 2. **Intermediate Layer** (`models/intermediate/`)
**Purpose**: Implement business logic and cross-system integrations

- **50+ Models** (tables)
- Organized by business system:
  - **Foundation**: Core patient and provider foundations
  - **System A**: Fee processing and pricing
  - **System B**: Insurance claims and coverage
  - **System C**: Payments and payment splits
  - **System D**: Accounts receivable (AR) analysis
  - **System E**: Collections and billing statements
  - **System F**: Communications and messaging
  - **System G**: Scheduling and appointments
  - **System H**: Audit logging and tracking
  - **System I**: Patient management
- Cross-system models for patient financial journeys
- Located in `intermediate` schema

**Key Features**:
- Business logic implementation
- Data enrichment and calculations
- Cross-table joins and aggregations
- Incremental materialization for performance

#### 3. **Mart Layer** (`models/marts/`)
**Purpose**: Denormalized models optimized for reporting and analytics

- **21 Models** (tables)
- **Dimension Tables**:
  - `dim_patient` - Patient demographics and attributes
  - `dim_provider` - Provider information
  - `dim_procedure` - Procedure codes and details
  - `dim_insurance` - Insurance plan information
  - `dim_fee_schedule` - Fee schedule definitions
  - `dim_clinic` - Clinic location data
  - `dim_date` - Date dimension for time-based analysis

- **Fact Tables**:
  - `fact_procedure` - Procedure transactions
  - `fact_appointment` - Appointment records
  - `fact_payment` - Payment transactions
  - `fact_claim` - Insurance claim records
  - `fact_communication` - Communication logs

- **Summary Marts**:
  - `mart_production_summary` - Production metrics
  - `mart_provider_performance` - Provider KPIs
  - `mart_ar_summary` - Accounts receivable analysis
  - `mart_patient_retention` - Patient retention metrics
  - `mart_new_patient` - New patient analytics
  - `mart_appointment_summary` - Appointment analytics
  - `mart_hygiene_retention` - Hygiene appointment retention
  - `mart_procedure_acceptance_summary` - Treatment acceptance
  - `mart_revenue_lost` - Revenue opportunity analysis
  - `mart_treatment_plan_summary` - Treatment plan tracking

- Located in `marts` schema

**Key Features**:
- Star schema design
- Pre-aggregated metrics
- Business-friendly column names
- Optimized for dashboard queries

## 🚀 Quick Start

### Prerequisites

- Python 3.8+
- PostgreSQL database with raw OpenDental data
- dbt-core installed

### Installation

1. **Install dependencies**:
```bash
pip install dbt-postgres
```

2. **Install dbt packages**:
```bash
dbt deps
```

3. **Configure database connection**:
Update `profiles.yml` with your PostgreSQL credentials:
```yaml
dbt_dental_models:
  outputs:
    local:
      type: postgres
      host: localhost
      user: your_user
      password: your_password
      port: 5432
      dbname: your_database
      schema: analytics
  target: local
```

### Running Models

**Run all models**:
```bash
dbt run
```

**Run specific layer**:
```bash
# Staging only
dbt run --select staging.*

# Intermediate only
dbt run --select intermediate.*

# Marts only
dbt run --select marts.*
```

**Run specific model**:
```bash
dbt run --select dim_patient
```

**Full refresh (rebuild tables)**:
```bash
dbt run --full-refresh
```

### Testing

**Run all tests**:
```bash
dbt test
```

**Test specific layer**:
```bash
dbt test --select staging.*
dbt test --select intermediate.*
dbt test --select marts.*
```

**Test with severity levels**:
```bash
dbt test --severity warn  # Continue on test failures
dbt test --severity error # Fail build on test failures
```

### Documentation

**Generate and serve documentation**:
```bash
dbt docs generate
dbt docs serve
```

This will open interactive documentation in your browser showing:
- Model lineage graphs
- Column-level documentation
- Data source definitions
- Test results

## 📦 Package Dependencies

The project uses the following dbt packages:

- **dbt-labs/dbt_utils** (v1.3.0) - Utility macros for common transformations
- **metaplane/dbt_expectations** (v0.10.8) - Advanced data quality testing
- **godatadriven/dbt_date** (v0.13.0) - Date dimension and date utilities
- **dbt-labs/audit_helper** (v0.12.1) - Data auditing and comparison utilities

Install with: `dbt deps`

## 🧪 Testing Strategy

### Data Quality Tests

All models include comprehensive data quality tests:

- **Uniqueness**: Primary keys are unique
- **Not null**: Required fields contain values
- **Referential integrity**: Foreign keys reference valid records
- **Accepted values**: Categorical fields contain valid enum values
- **Relationships**: Related records exist in dependent tables
- **Custom tests**: Domain-specific validation rules

### Test Organization

- **Macros/tests/core/**: Core data quality macros
- **Macros/tests/data_quality/**: Advanced data quality checks
- **Macros/tests/domain/**: Dental domain-specific validations
- **Macros/tests/fee/**: Fee calculation validations

### Running Tests

Tests are automatically executed during CI/CD and can be run manually:

```bash
# Run all tests
dbt test

# Run tests for a specific model
dbt test --select dim_patient

# Run tests and continue on failures (warnings)
dbt test --severity warn
```

## 📚 Documentation

### Inline Documentation

All models include comprehensive YAML documentation:

- **Model descriptions**: Purpose and usage
- **Column descriptions**: Business definitions
- **Source definitions**: Origin table mappings
- **Test definitions**: Data quality expectations

### Key Documentation Files

- **`models/docs.md`**: Domain concepts and conventions
- **`dbt_docs/`**: Generated documentation site
- **`macros/README.md`**: Macro documentation

### Accessing Documentation

1. Generate docs: `dbt docs generate`
2. Serve locally: `dbt docs serve`
3. Deploy to web: Upload `target/` directory to web server

## 🔧 Key Macros

### Data Transformation Macros

- **`convert_opendental_boolean()`**: Converts TINYINT to boolean
- **`clean_opendental_dates()`**: Removes sentinel date values
- **`clean_opendental_string()`**: Normalizes string values
- **`transform_id_columns()`**: Standardizes ID column naming

### Metadata Macros

- **`standardize_metadata_columns()`**: Adds tracking columns
- **`standardize_staging_metadata()`**: Staging layer metadata
- **`standardize_intermediate_metadata()`**: Intermediate layer metadata
- **`standardize_mart_metadata()`**: Mart layer metadata

### Utility Macros

- **`quote_column()`**: Handles case-sensitive column quoting
- **`calculate_pattern_length()`**: Pattern matching utilities
- **`dbt_tracking()`**: ETL tracking integration
- **`etl_tracking_helpers()`**: Pipeline status updates

### Security Macros

- **`hipaa_compliance()`**: HIPAA compliance checks

## 📈 Model Statistics

### Current Status

- **Total Models**: 157
- **Staging Models**: 88+ views
- **Intermediate Models**: 50+ tables
- **Mart Models**: 21 tables
- **Test Coverage**: 100% (all models tested)
- **Success Rate**: 100% (all tests passing)

### Performance

- **Average Run Time**: ~15-20 minutes (full refresh)
- **Incremental Runs**: ~5-10 minutes (incremental only)
- **Test Execution**: ~2-3 minutes

## 🏥 Healthcare Domain Knowledge

This project implements dental practice analytics with deep understanding of:

- **Clinical Workflows**: Appointments → Procedures → Claims → Payments
- **Financial Cycles**: Production → Insurance Claims → AR → Collections
- **Patient Management**: Family structures, guarantors, insurance coverage
- **Provider Operations**: Schedules, production tracking, performance metrics
- **Compliance**: HIPAA considerations in data transformations

## 🔗 Integration Points

### ETL Pipeline Integration

The dbt project integrates with the upstream ETL pipeline:

- **Tracking**: Reads ETL run status from `raw.etl_tracking_summary`
- **Hooks**: Updates transform status in `on-run-start` and `on-run-end`
- **Incremental Logic**: Aligns with ETL incremental loading strategy

### API Integration

Mart models serve as the data source for the FastAPI backend:

- REST endpoints query mart tables
- Pre-aggregated metrics enable fast dashboard responses
- Dimension tables provide lookup data for filters

### Dashboard Integration

React dashboard consumes mart layer data:

- Real-time KPIs from summary marts
- Interactive charts from fact tables
- Dimension tables for filtering and grouping

## 🛠️ Development Workflow

### Adding a New Model

1. **Create SQL file** in appropriate layer directory
2. **Create YAML file** with documentation and tests
3. **Run and test**: `dbt run --select <model_name>`
4. **Test**: `dbt test --select <model_name>`
5. **Document**: Generate and review docs

### Best Practices

- **Follow naming conventions**: `stg_*`, `int_*`, `dim_*`, `fact_*`, `mart_*`
- **Add documentation**: Every model and column should be documented
- **Include tests**: At minimum, test uniqueness and not null
- **Use incremental**: For large tables, use incremental materialization
- **Version control**: Commit model changes with descriptive messages

## 📝 Configuration

### Project Configuration

Key settings in `dbt_project.yml`:

```yaml
# Schema naming
staging: +schema: staging
intermediate: +schema: intermediate
marts: +schema: marts

# Materializations
staging: +materialized: view
intermediate: +materialized: table
marts: +materialized: table
```

### Variables

Project variables in `dbt_project.yml`:

- `max_valid_date`: Maximum date for date filtering
- `schedule_window_days`: Appointment scheduling window (default: 30)

## 🚨 Troubleshooting

### Common Issues

**Case Sensitivity Errors**:
- Ensure `profiles.yml` uses correct case for schema/database names
- Check that quoting is enabled in `dbt_project.yml`

**Test Failures**:
- Review test results: `dbt test --select <model>`
- Check source data quality
- Adjust test thresholds if needed

**Performance Issues**:
- Use incremental materialization for large tables
- Add indexes on join keys
- Review query plans in PostgreSQL

### Getting Help

- Check model documentation: `dbt docs serve`
- Review test output for specific error messages
- Examine source data quality

## 📊 Business Impact

### Analytics Enabled

- **Revenue Analytics**: Production, collections, AR analysis
- **Provider Performance**: Productivity, efficiency metrics
- **Patient Analytics**: Retention, acquisition, demographics
- **Operational Metrics**: Scheduling, utilization, capacity
- **Financial Intelligence**: Payment trends, insurance claims

### Key Metrics Available

- Production by provider, procedure, time period
- Accounts receivable aging and collection rates
- Patient retention and hygiene compliance
- Treatment acceptance rates
- Appointment utilization and no-show rates

## 🔗 Related Projects

- **ETL Pipeline**: [`../etl_pipeline`](../etl_pipeline) - Source data replication
- **API Backend**: [`../api`](../api) - REST API serving mart data
- **Frontend Dashboard**: [`../frontend`](../frontend) - React analytics dashboard
- **Consult Audio Pipeline**: [`../consult_audio_pipe`](../consult_audio_pipe) - ML transcription pipeline

## 📄 License

Part of the dbt Dental Clinic Analytics Platform. See main repository README for license information.

## 👥 Contributing

This is a production analytics project. All changes should:
1. Include comprehensive tests
2. Update documentation
3. Follow naming conventions
4. Be reviewed before merging

---

**Last Updated**: 2025-01-27  
**dbt Version**: Compatible with dbt-core 1.3.0+  
**PostgreSQL Version**: 12+  
**Status**: ✅ Production Ready

