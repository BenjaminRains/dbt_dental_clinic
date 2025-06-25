{% docs index %}

# Dental Clinic Analytics Platform Documentation

Welcome to the comprehensive documentation for the Dental Clinic Analytics Platform. This platform 
transforms OpenDental practice management data into actionable business intelligence using modern
 data engineering practices.

## 🏥 Platform Overview

This analytics platform processes data from OpenDental (a dental practice management system) and
 transforms it into business-ready analytics models using DBT (Data Build Tool). The platform
  handles 432+ tables with intelligent ETL processing and provides comprehensive business insights.

### Key Features
- **Intelligent ETL Pipeline**: Auto-discovers and processes 432+ tables
- **Comprehensive Data Models**: 25+ mart models covering all business processes
- **Data Quality Assurance**: 95%+ test pass rates across all layers
- **Business Intelligence**: Revenue optimization, patient retention, operational efficiency
- **Modern Architecture**: PostgreSQL, DBT, FastAPI, React

## 📊 Business Processes Covered

Our data models are organized around seven core dental practice business processes:

1. **Fee Processing & Verification** - Procedure pricing and validation
2. **Insurance Claims Management** - End-to-end claims processing
3. **Payment Allocation & Reconciliation** - Revenue cycle optimization
4. **AR Analysis** - Accounts receivable intelligence
5. **Collection Management** - Outstanding balance workflows
6. **Patient Communications** - Multi-channel engagement tracking
7. **Scheduling & Referrals** - Appointment optimization

## 🏗️ Data Architecture

### Data Flow
```
OpenDental (MariaDB) → ETL Pipeline → PostgreSQL → DBT → Analytics API → Frontend
```

### Model Layers
- **Staging**: Raw data standardization and basic quality checks
- **Intermediate**: Business logic implementation and dimensional modeling
- **Marts**: Business-ready analytics models for reporting and dashboards

## 📈 Key Metrics & KPIs

### Financial Metrics
- Revenue per provider
- AR aging analysis
- Payment collection rates
- Insurance claim processing efficiency

### Operational Metrics
- Patient retention rates
- Appointment utilization
- Provider productivity
- Chair utilization

### Patient Metrics
- New patient acquisition
- Treatment acceptance rates
- Communication effectiveness
- Patient satisfaction correlation

## 🔧 Technical Stack

- **ETL Engine**: Python with intelligent schema discovery
- **Data Warehouse**: PostgreSQL with optimized analytics schema
- **Transformation**: DBT Core with comprehensive testing
- **API Layer**: FastAPI with RESTful endpoints
- **Frontend**: React/TypeScript with Material-UI
- **Visualization**: Recharts for interactive dashboards

## 📋 Documentation Structure

### Model Documentation
- **Staging Models**: Source data standardization and validation
- **Intermediate Models**: Business logic and dimensional modeling
- **Mart Models**: Business intelligence and analytics

### Business Documentation
- **Process Overview**: Dental practice workflow documentation
- **Data Dictionary**: Field definitions and business rules
- **Quality Metrics**: Data quality standards and validation

### Technical Documentation
- **Architecture**: System design and data flow
- **Deployment**: Setup and configuration guides
- **API Reference**: Endpoint documentation

## 🎯 Getting Started

### For Data Analysts
1. Review the [Business Process Overview](dental_process_overview)
2. Explore [Mart Models](marts) for business-ready analytics
3. Check [Data Quality Reports](quality) for data reliability

### For Data Engineers
1. Review [Staging Models](staging) for data standardization
2. Explore [Intermediate Models](intermediate) for business logic
3. Check [Test Coverage](tests) for data quality validation

### For Business Users
1. Review [Key Metrics](marts) for business insights
2. Explore [Financial Analytics](marts) for revenue optimization
3. Check [Patient Analytics](marts) for retention insights

## 📊 Data Quality Standards

Our platform maintains high data quality standards:
- **95%+ Test Pass Rate**: Across all model layers
- **Automated Validation**: Comprehensive data quality checks
- **Business Rule Testing**: Custom logic validation
- **Performance Monitoring**: Query optimization and SLA tracking

## 🔍 Model Lineage

All models include comprehensive lineage tracking:
- **Source Dependencies**: Clear mapping to source systems
- **Business Logic**: Documented transformation rules
- **Data Flow**: Visual representation of data movement
- **Impact Analysis**: Understanding of downstream effects

---

*This documentation is automatically generated and updated with each DBT run. For the latest
 information, ensure you're viewing the most recent documentation build.*

{% enddocs %} 