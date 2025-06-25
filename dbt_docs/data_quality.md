{% docs data_quality %}

# Data Quality Framework

## Quality Standards Overview

The Dental Clinic Analytics Platform maintains rigorous data quality standards across all layers of 
the data pipeline. Our goal is to achieve 95%+ test pass rates and ensure data reliability for 
business-critical decisions.

## 📊 Quality Dimensions

### 1. Completeness
**Definition**: All required fields are populated with valid data.

**Standards**:
- **Critical Fields**: 100% completion required
- **Important Fields**: 95%+ completion required
- **Optional Fields**: 80%+ completion expected

**Examples**:
```sql
-- Patient ID must always be present
patient_id IS NOT NULL

-- Email should be present for 95%+ of patients
SUM(CASE WHEN email IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*) >= 0.95
```

### 2. Accuracy
**Definition**: Data correctly represents the real-world entities and events.

**Standards**:
- **Business Rules**: 100% compliance required
- **Data Relationships**: Referential integrity maintained
- **Calculations**: Mathematical accuracy verified

**Examples**:
```sql
-- Payment amounts must be positive
payment_amount > 0

-- Appointment dates must be in the past or present
appointment_date <= CURRENT_DATE

-- Patient age must be reasonable (0-120 years)
patient_age BETWEEN 0 AND 120
```

### 3. Consistency
**Definition**: Data is consistent across different sources and time periods.

**Standards**:
- **Cross-Table Consistency**: Referential integrity maintained
- **Temporal Consistency**: Historical data remains stable
- **Format Consistency**: Standardized data formats

**Examples**:
```sql
-- Patient IDs in appointments must exist in patient table
EXISTS (SELECT 1 FROM dim_patient WHERE patient_id = appointment.patient_id)

-- Procedure codes must be valid
procedure_code IN (SELECT procedure_code FROM dim_procedure)
```

### 4. Timeliness
**Definition**: Data is updated within acceptable timeframes.

**Standards**:
- **Real-time Data**: <5 minutes delay
- **Daily Updates**: <24 hours delay
- **Batch Processing**: <2 hours delay

**Examples**:
```sql
-- Data should be updated within last 24 hours
MAX(updated_at) >= CURRENT_DATE - INTERVAL '1 day'

-- ETL completion time tracking
etl_completion_time <= scheduled_time + INTERVAL '2 hours'
```

### 5. Validity
**Definition**: Data conforms to expected formats and ranges.

**Standards**:
- **Data Types**: Correct type validation
- **Format Validation**: Email, phone, date formats
- **Range Validation**: Reasonable value ranges

**Examples**:
```sql
-- Email format validation
email ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'

-- Phone number format (US)
phone ~* '^\+?1?\d{9,15}$'

-- Date range validation
appointment_date BETWEEN '2020-01-01' AND CURRENT_DATE + INTERVAL '1 year'
```

## 🧪 Testing Framework

### Test Categories

#### 1. Generic Tests (DBT Built-in)
**Purpose**: Standard data quality checks across all models.

**Test Types**:
- **not_null**: Required fields are populated
- **unique**: Unique constraint validation
- **relationships**: Referential integrity
- **accepted_values**: Valid value validation

**Example Configuration**:
```yaml
version: 2

models:
  - name: dim_patient
    columns:
      - name: patient_id
        tests:
          - not_null
          - unique
      - name: email
        tests:
          - not_null
          - unique
      - name: provider_id
        tests:
          - relationships:
              to: ref('dim_provider')
              field: provider_id
```

#### 2. Custom Business Logic Tests
**Purpose**: Dental practice-specific business rules validation.

**Test Examples**:
```sql
-- Test: Payment amounts must match procedure fees
SELECT payment_id
FROM fact_payment fp
JOIN fact_procedure fproc ON fp.procedure_id = fproc.procedure_id
WHERE ABS(fp.payment_amount - fproc.procedure_fee) > 0.01

-- Test: Patient age calculation accuracy
SELECT patient_id
FROM dim_patient
WHERE patient_age != EXTRACT(YEAR FROM AGE(birth_date))
```

#### 3. Data Quality Tests (dbt_expectations)
**Purpose**: Advanced statistical and pattern-based validation.

**Test Examples**:
```sql
-- Test: Column value distribution
{{ config(tags=['data_quality']) }}
{{ dbt_expectations.expect_column_values_to_be_between(
    "payment_amount",
    min_value=0,
    max_value=10000,
    row_condition="payment_type = 'cash'"
) }}

-- Test: Column uniqueness percentage
{{ dbt_expectations.expect_column_proportion_of_unique_values_to_be_between(
    "email",
    min_value=0.95,
    max_value=1.0
) }}
```

### Test Execution Strategy

#### 1. Development Testing
- **Unit Tests**: Run on individual model changes
- **Integration Tests**: Run on dependent model chains
- **Performance Tests**: Query execution time validation

#### 2. CI/CD Testing
- **Pre-commit**: Basic validation before code commit
- **Pull Request**: Full test suite on proposed changes
- **Deployment**: Production validation before release

#### 3. Production Monitoring
- **Scheduled Tests**: Daily quality validation
- **Alerting**: Immediate notification of quality issues
- **Reporting**: Quality metrics dashboard

## 📈 Quality Metrics & Monitoring

### Key Performance Indicators

#### 1. Test Coverage
- **Target**: 95%+ of models have comprehensive tests
- **Measurement**: Number of tested models / Total models
- **Reporting**: Weekly coverage reports

#### 2. Test Pass Rate
- **Target**: 95%+ test success rate
- **Measurement**: Passed tests / Total tests executed
- **Alerting**: Immediate notification if <90%

#### 3. Data Freshness
- **Target**: <24 hours for all data
- **Measurement**: Time since last successful update
- **Monitoring**: Real-time freshness tracking

#### 4. Data Volume Validation
- **Target**: <5% variance from expected volumes
- **Measurement**: Actual vs. expected row counts
- **Alerting**: Volume anomaly detection

### Quality Dashboard Metrics

```sql
-- Quality Metrics Summary
SELECT 
    model_name,
    test_coverage_percentage,
    test_pass_rate,
    data_freshness_hours,
    last_test_run,
    quality_score
FROM quality_metrics
WHERE quality_score < 0.95
ORDER BY quality_score ASC;
```

## 🔍 Data Quality Monitoring

### Automated Monitoring

#### 1. Real-time Quality Checks
- **Streaming Validation**: Real-time data quality assessment
- **Anomaly Detection**: Statistical outlier identification
- **Trend Analysis**: Quality metric trending over time

#### 2. Scheduled Quality Reports
- **Daily Reports**: Comprehensive quality assessment
- **Weekly Summaries**: Quality trend analysis
- **Monthly Reviews**: Quality improvement recommendations

#### 3. Alerting System
- **Critical Alerts**: Immediate notification for quality failures
- **Warning Alerts**: Quality degradation warnings
- **Info Alerts**: Quality improvement notifications

### Quality Improvement Process

#### 1. Issue Identification
- **Automated Detection**: System identifies quality issues
- **Manual Review**: Business team validates issues
- **Root Cause Analysis**: Technical investigation

#### 2. Issue Resolution
- **Data Correction**: Fix data quality issues
- **Process Improvement**: Prevent future issues
- **Documentation Update**: Update quality standards

#### 3. Continuous Improvement
- **Quality Metrics Review**: Regular assessment of standards
- **Process Optimization**: Streamline quality processes
- **Tool Enhancement**: Improve quality monitoring tools

## 📋 Quality Standards by Model Layer

### Staging Layer Quality Standards
- **Completeness**: 90%+ for all fields
- **Accuracy**: 100% for critical business fields
- **Consistency**: Standardized naming and formats
- **Timeliness**: <1 hour from source update

### Intermediate Layer Quality Standards
- **Completeness**: 95%+ for all fields
- **Accuracy**: 100% for calculated fields
- **Consistency**: Referential integrity maintained
- **Timeliness**: <2 hours from staging update

### Mart Layer Quality Standards
- **Completeness**: 98%+ for all fields
- **Accuracy**: 100% for business metrics
- **Consistency**: Cross-model consistency verified
- **Timeliness**: <4 hours from intermediate update

## 🛠️ Quality Tools & Automation

### DBT Testing Tools
- **dbt_utils**: Generic testing utilities
- **dbt_expectations**: Advanced quality testing
- **dbt_audit_helper**: Data quality auditing
- **Custom Macros**: Dental practice-specific tests

### Quality Automation
- **Automated Testing**: CI/CD pipeline integration
- **Quality Scoring**: Automated quality assessment
- **Issue Tracking**: Integrated with project management
- **Reporting**: Automated quality reporting

### Quality Documentation
- **Data Dictionary**: Comprehensive field documentation
- **Business Rules**: Documented quality standards
- **Quality Reports**: Regular quality assessment reports
- **Improvement Plans**: Quality enhancement roadmaps

---

*This data quality framework ensures reliable, accurate, and timely data for business
 decision-making. Regular reviews and updates maintain high quality standards across the platform.*

{% enddocs %} 