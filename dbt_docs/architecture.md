{% docs architecture %}

# Technical Architecture

## System Overview

The Dental Clinic Analytics Platform is built using modern data engineering practices with a focus
 on scalability, reliability, and business value. The architecture follows a layered approach with
  clear separation of concerns.

## 🏗️ High-Level Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   OpenDental    │    │   ETL Pipeline  │    │   PostgreSQL    │    │   DBT Models    │
│   (MariaDB)     │───▶│   (Python)      │───▶│   Analytics     │───▶│   (Transform)   │
│                 │    │                 │    │   Database      │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘    └─────────────────┘
                                │                                              │
                                ▼                                              ▼
                       ┌─────────────────┐                        ┌─────────────────┐
                       │   FastAPI       │                        │   React         │
                       │   (API Layer)   │                        │   (Frontend)    │
                       └─────────────────┘                        └─────────────────┘
```

## 📊 Data Flow Architecture

### 1. Source Layer (OpenDental)
- **Database**: MariaDB/MySQL
- **Tables**: 432+ tables covering all dental practice operations
- **Data Volume**: ~3.7GB, 17.8M+ rows
- **Update Frequency**: Real-time operational data

### 2. ETL Layer (Intelligent Pipeline)
- **Technology**: Python with intelligent schema discovery
- **Key Features**:
  - Auto-discovery of all 432 tables
  - Intelligent batching (1K-5K rows per batch)
  - Parallel processing for critical tables
  - Incremental sync using 60+ timestamp columns
  - Quality validation and monitoring
- **Processing Time**: 5-70 minutes depending on data volume
- **Error Handling**: Comprehensive logging and alerting

### 3. Data Warehouse (PostgreSQL)
- **Database**: PostgreSQL optimized for analytics
- **Schemas**:
  - `raw`: Raw ingested data
  - `raw_staging`: Standardized source data
  - `raw_intermediate`: Business logic layer with complex       transformations and system-specific models
  - `raw_marts`: Analytics-ready models
- **Performance**: Optimized indexes and partitioning

### 4. Transformation Layer (DBT)
- **Framework**: DBT Core with comprehensive testing
- **Model Layers**:
  - **Staging**: Data standardization and validation
  - **Intermediate**: Business logic and dimensional modeling
  - **Marts**: Business intelligence and analytics
- **Testing**: 95%+ test pass rate target
- **Documentation**: Auto-generated lineage and documentation

### 5. API Layer (FastAPI)
- **Framework**: FastAPI with RESTful endpoints
- **Features**:
  - Patient data access
  - Appointment analytics
  - Financial reporting
  - Real-time metrics
- **Performance**: Sub-200ms response times
- **Security**: CORS configuration and authentication ready

### 6. Frontend Layer (React)
- **Framework**: React with TypeScript
- **UI Library**: Material-UI for professional design
- **Charts**: Recharts for interactive visualizations
- **State Management**: Zustand for application state
- **Features**: Responsive design, real-time updates

## 🔧 Technical Components

### ETL Pipeline Components

#### Intelligent Schema Discovery
```python
# Auto-discovers all tables and their characteristics
- Table classification by business importance
- Column type detection and mapping
- Relationship identification
- Performance optimization recommendations
```

#### Parallel Processing Engine
```python
# Optimizes processing based on table characteristics
- Critical tables processed simultaneously
- Batch size optimization (1K-5K rows)
- Memory management and resource allocation
- Progress tracking and monitoring
```

#### Quality Validation System
```python
# Comprehensive data quality checks
- Schema validation
- Data type verification
- Business rule validation
- Performance monitoring
```

### DBT Model Architecture

#### Staging Layer
```sql
-- Standardizes source data
- Consistent naming conventions
- Data type standardization
- Basic quality checks
- Source system abstraction
```

#### Intermediate Layer
```sql
-- Implements business logic
- Dimensional modeling
- Business process models
- Reusable intermediate tables
- Performance optimization
```

#### Mart Layer
```sql
-- Business-ready analytics
- Revenue cycle analytics
- Patient retention metrics
- Provider performance dashboards
- Operational efficiency metrics
```

## 📈 Performance Characteristics

### ETL Performance
- **Full Refresh**: 5-70 minutes depending on data volume
- **Incremental Updates**: 2-10 minutes for daily changes
- **Parallel Processing**: Up to 10x speed improvement
- **Memory Usage**: Optimized for 8GB+ systems

### Query Performance
- **Dashboard Load**: Sub-second response times
- **Complex Analytics**: <5 seconds for multi-table joins
- **Real-time Metrics**: <200ms for API responses
- **Batch Processing**: Optimized for overnight runs

### Scalability
- **Data Volume**: Handles 10M+ rows efficiently
- **Concurrent Users**: Supports 50+ simultaneous users
- **Storage**: Efficient compression and partitioning
- **Growth**: Designed for 3-5x data volume growth

## 🔒 Security & Compliance

### Data Security
- **Encryption**: Data encrypted in transit and at rest
- **Access Control**: Role-based permissions
- **Audit Logging**: Complete audit trails
- **HIPAA Compliance**: Healthcare data protection

### System Security
- **Authentication**: JWT-based authentication
- **Authorization**: Fine-grained access control
- **Network Security**: HTTPS/TLS encryption
- **Monitoring**: Security event logging

## 🚀 Deployment Architecture

### Development Environment
```
Local Development → Docker Compose → Local PostgreSQL
```

### Production Environment
```
Load Balancer → Application Servers → Database Cluster → Backup Storage
```

### Monitoring & Alerting
- **Application Monitoring**: Performance and error tracking
- **Data Quality Monitoring**: Automated validation alerts
- **Infrastructure Monitoring**: System health and capacity
- **Business Metrics**: KPI tracking and alerting

## 📋 Configuration Management

### Environment Variables
```bash
# Database Connections
DB_HOST=localhost
DB_PORT=5432
DB_NAME=opendental_analytics
DB_USER=analytics_user

# ETL Configuration
ETL_BATCH_SIZE=5000
ETL_PARALLEL_WORKERS=4
ETL_TIMEOUT_MINUTES=120

# DBT Configuration
DBT_TARGET=prod
DBT_PROFILE=dbt_dental_clinic
```

### Configuration Files
- **dbt_project.yml**: DBT project configuration
- **profiles.yml**: Database connection profiles
- **packages.yml**: DBT package dependencies
- **docker-compose.yml**: Container orchestration

## 🔄 Data Pipeline Orchestration

### Scheduling
- **ETL Pipeline**: Daily incremental updates
- **DBT Models**: Automated after ETL completion
- **Quality Checks**: Continuous monitoring
- **Documentation**: Auto-generated on model changes

### Error Handling
- **Retry Logic**: Automatic retry for transient failures
- **Alerting**: Immediate notification for critical errors
- **Rollback**: Automatic rollback for failed deployments
- **Recovery**: Self-healing for common issues

## 📊 Data Quality Framework

### Quality Dimensions
- **Completeness**: Required fields populated
- **Accuracy**: Data matches business rules
- **Consistency**: Cross-table relationships valid
- **Timeliness**: Data updated within SLA
- **Validity**: Data format and range checks

### Quality Metrics
- **Test Coverage**: 95%+ of models tested
- **Pass Rate**: 95%+ test success rate
- **Performance**: Query execution time monitoring
- **Freshness**: Data update frequency tracking

---

*This architecture is designed for scalability, maintainability, and business value. Regular
 reviews and updates ensure the system continues to meet evolving requirements.*

{% enddocs %} 