# BI Tools Development Plan
## Professional Development & Data Exposure Strategy

**Purpose**: Explore and implement various BI tool integrations to expose dbt models and datasets for professional development, skill building, and comprehensive business intelligence capabilities.

**Current State**: 
- 17 marts, 50+ intermediate models, 88+ staging models
- React frontend with Recharts (custom dashboards)
- FastAPI backend serving mart data
- PostgreSQL analytics warehouse ready for BI connections

**Target State**: 
- Multiple BI tool integrations (Tableau, Power BI, Looker, Metabase)
- Direct database connections for advanced analytics
- API-based connections for controlled access
- Embedded analytics for external stakeholders
- Comprehensive exposure documentation

**Strategic Value**: 
- Professional development in modern BI tools
- Portfolio demonstration of enterprise BI capabilities
- Flexible data access patterns for different use cases
- Industry-standard analytics workflows

**Related Documentation**:
- Exposures Development Plan: `EXPOSURES_DEVELOPMENT_PLAN.md`
- Technical Architecture: `../../etl_pipeline/docs/PIPELINE_ARCHITECTURE.md`
- dbt Grants Guide: `dbt_grants_guide.md`

---

## Table of Contents

1. [BI Tool Landscape](#bi-tool-landscape)
2. [Connection Architecture Patterns](#connection-architecture-patterns)
3. [Tool-Specific Integration Plans](#tool-specific-integration-plans)
4. [Professional Development Benefits](#professional-development-benefits)
5. [Implementation Roadmap](#implementation-roadmap)
6. [Security & Governance](#security--governance)
7. [Documentation & Exposure Management](#documentation--exposure-management)
8. [Best Practices & Patterns](#best-practices--patterns)

---

## BI Tool Landscape

### Tool Categories

#### 1. Enterprise BI Platforms
**Purpose**: Full-featured, enterprise-grade business intelligence

**Tools**:
- **Tableau** (Desktop/Server/Online)
  - Industry leader, strong visualization capabilities
  - Excellent for portfolio demonstration
  - Direct PostgreSQL support
  
- **Power BI** (Desktop/Service)
  - Microsoft ecosystem integration
  - Strong data modeling capabilities
  - Free tier available
  
- **Looker** (Google Cloud)
  - Modern LookML modeling language
  - Strong semantic layer
  - API-first architecture

**Use Cases**:
- Executive dashboards
- Advanced analytics and forecasting
- Portfolio demonstrations
- Professional skill development

#### 2. Open-Source BI Tools
**Purpose**: Self-hosted, customizable analytics platforms

**Tools**:
- **Metabase**
  - Easy setup, user-friendly
  - Great for ad-hoc analysis
  - Open-source with paid features
  
- **Superset** (Apache)
  - Advanced visualization options
  - Strong SQL editor
  - Enterprise features available

**Use Cases**:
- Internal team analytics
- Cost-effective BI solution
- Customization and control
- Learning open-source BI stack

#### 3. Embedded Analytics
**Purpose**: Integrate analytics into applications

**Tools**:
- **Tableau Embedded**
- **Power BI Embedded**
- **Looker Embedded**
- **Custom API-based dashboards**

**Use Cases**:
- Client-facing analytics
- White-label solutions
- API-driven analytics

#### 4. Lightweight Analytics
**Purpose**: Quick insights and prototyping

**Tools**:
- **Grafana** (time-series focused)
- **Redash** (SQL-focused)
- **Retool** (internal tools)

**Use Cases**:
- Operational dashboards
- Real-time monitoring
- Quick prototyping

---

## Connection Architecture Patterns

### Pattern 1: Direct Database Connection (Recommended for Most Tools)

**Architecture**:
```
BI Tool (Tableau/Power BI/Looker)
    ↓ (Native Database Driver)
PostgreSQL Analytics Database
    ├── marts schema (primary - 17 marts)
    ├── intermediate schema (50+ models)
    └── staging schema (88+ models)
```

**Benefits**:
- ✅ Full SQL capabilities
- ✅ No API overhead
- ✅ Direct access to all dbt models
- ✅ Standard BI tool pattern
- ✅ Best performance
- ✅ Leverages existing marts

**Drawbacks**:
- ⚠️ Requires database access management
- ⚠️ Network security considerations
- ⚠️ Direct database load

**Best For**:
- Tableau Desktop/Server
- Power BI Desktop/Service
- Looker
- Metabase
- Superset

**Implementation**:
1. Create dedicated read-only database user
2. Grant schema and table access
3. Configure dbt grants for auto-permissions
4. Document connection details in exposures

---

### Pattern 2: API-Based Connection

**Architecture**:
```
BI Tool (Web Data Connector / REST API)
    ↓ (HTTP/HTTPS)
FastAPI Backend
    ↓ (SQLAlchemy)
PostgreSQL Analytics Database
```

**Benefits**:
- ✅ Centralized access control
- ✅ Caching and rate limiting
- ✅ Consistent with frontend approach
- ✅ API key authentication
- ✅ Request logging and monitoring

**Drawbacks**:
- ⚠️ More complex setup
- ⚠️ Performance overhead
- ⚠️ Limited SQL flexibility
- ⚠️ Requires API endpoint development

**Best For**:
- Tableau Web Data Connector
- Power BI REST API connections
- Embedded analytics
- External stakeholder access

**Implementation**:
1. Extend FastAPI with BI-specific endpoints
2. Implement pagination and filtering
3. Add caching layer (Redis)
4. Document API endpoints in exposures

---

### Pattern 3: Hybrid Approach

**Architecture**:
```
Internal Users → Direct DB Connection
External Users → API Connection
Embedded Analytics → API Connection
```

**Benefits**:
- ✅ Best of both worlds
- ✅ Security by use case
- ✅ Flexible access patterns

**Best For**:
- Mixed user base
- Different security requirements
- Portfolio demonstrations

---

## Tool-Specific Integration Plans

### 1. Tableau Integration

#### Overview
Tableau is the industry standard for enterprise BI. Integration provides professional development value and portfolio demonstration capabilities.

#### Connection Method: Direct PostgreSQL

**Connection Details**:
- **Server**: `POSTGRES_ANALYTICS_HOST`
- **Port**: `5432` (default)
- **Database**: `opendental_analytics`
- **Username**: `tableau_user` (read-only)
- **Authentication**: Password
- **SSL Mode**: Prefer (production)

**Schema Access**:
- **Primary**: `marts` (17 marts for dashboards)
- **Secondary**: `intermediate` (50+ models for deep analysis)
- **Optional**: `staging` (raw data exploration)

#### Implementation Steps

**Step 1: Database User Setup**
```sql
-- Create dedicated Tableau user
CREATE USER tableau_user WITH PASSWORD 'secure_password_here';

-- Grant schema access
GRANT USAGE ON SCHEMA marts TO tableau_user;
GRANT USAGE ON SCHEMA intermediate TO tableau_user;
GRANT USAGE ON SCHEMA staging TO tableau_user;

-- Grant SELECT on all existing tables
GRANT SELECT ON ALL TABLES IN SCHEMA marts TO tableau_user;
GRANT SELECT ON ALL TABLES IN SCHEMA intermediate TO tableau_user;
GRANT SELECT ON ALL TABLES IN SCHEMA staging TO tableau_user;

-- Grant SELECT on future tables (for dbt auto-grants)
ALTER DEFAULT PRIVILEGES IN SCHEMA marts GRANT SELECT ON TABLES TO tableau_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA intermediate GRANT SELECT ON TABLES TO tableau_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA staging GRANT SELECT ON TABLES TO tableau_user;
```

**Step 2: dbt Grants Configuration**
```yaml
# In dbt_project.yml
models:
  dbt_dental_models:
    marts:
      +grants:
        select: ['tableau_user', 'analytics_user']
    intermediate:
      +grants:
        select: ['tableau_user', 'analytics_user']
```

**Step 3: Tableau Workbook Development**

**Recommended Data Sources**:
1. **Executive Dashboard**
   - `marts.mart_revenue_lost`
   - `marts.mart_provider_performance`
   - `marts.mart_production_summary`

2. **Financial Analytics**
   - `marts.mart_ar_summary`
   - `marts.mart_production_summary`
   - `fact_payment`
   - `fact_claim`

3. **Operational Analytics**
   - `marts.mart_appointment_summary`
   - `marts.mart_hygiene_retention`
   - `fact_appointment`

4. **Patient Analytics**
   - `marts.mart_new_patient`
   - `marts.mart_patient_retention`
   - `dim_patient`

**Step 4: Exposure Documentation**
Add to `exposures.yml`:
```yaml
exposures:
  - name: tableau_analytics_connection
    label: "Tableau Analytics Connection"
    type: dashboard
    maturity: high
    owner:
      name: Analytics Team
      email: analytics@dentalclinic.com
    
    depends_on:
      - ref('mart_revenue_lost')
      - ref('mart_provider_performance')
      - ref('mart_ar_summary')
      # ... all marts
    
    url: "postgresql://tableau_user@host:5432/opendental_analytics"
    
    description: >
      **Tableau Direct Database Connection**
      
      Direct PostgreSQL connection for advanced BI analysis. Provides access to all
      17 marts and 50+ intermediate models for comprehensive business intelligence.
      
      ## Connection Details:
      - Server: PostgreSQL Analytics Database
      - Schemas: marts (primary), intermediate (advanced), staging (raw)
      - User: tableau_user (read-only)
      
      ## Recommended Data Sources:
      - marts.mart_revenue_lost: Revenue recovery opportunities
      - marts.mart_provider_performance: Provider productivity metrics
      - marts.mart_ar_summary: AR aging and collection analysis
      - marts.mart_appointment_summary: Scheduling efficiency
      
      ## Data Refresh:
      - ETL Pipeline: Daily at 3:00 AM
      - dbt Transform: Daily at 6:00 AM
      - Tableau Refresh: User-initiated or scheduled
```

#### Professional Development Value

**Skills Developed**:
- Tableau Desktop proficiency
- Data source connection management
- Calculated fields and LOD expressions
- Dashboard design and interactivity
- Tableau Server administration (if using Server)

**Portfolio Projects**:
- Executive KPI dashboard
- Financial analytics workbook
- Operational efficiency dashboard
- Patient analytics workbook

---

### 2. Power BI Integration

#### Overview
Power BI provides Microsoft ecosystem integration and strong data modeling capabilities. **Canonical runbooks** (private **Aurora PostgreSQL**, **`bi_user`**, selective **`marts`** grants, **data gateway**, pilot tables, costs) live in **`docs/analytics/`** — start with [power_bi_postgresql_access.md](power_bi_postgresql_access.md) and [power_bi_pilot_dataset.md](power_bi_pilot_dataset.md). Career-oriented summary: [career/platform_projects/POWER_BI.md](../../career/platform_projects/POWER_BI.md).

> **Status (2026-06): local pilot in progress.** Implementation artifacts live in
> [`powerbi/`](../../powerbi/README.md) at the repo root: connection guide (localhost,
> no gateway), model guide with verified grains/relationships, starter DAX measures,
> and report checklist. The dbt exposure `power_bi_executive_pilot` tracks lineage.

#### Connection Method: PostgreSQL over a private network path

The analytics warehouse is **not public**; use the **Aurora cluster (or reader) endpoint** inside the VPC. **Power BI Service** scheduled refresh requires an **on-premises or VNet data gateway** on a host that can reach Aurora on **5432**. **Power BI Desktop** must run from a workstation or jump box with the same reachability (VPN, Session Manager, RDP to VPC EC2).

**Connection details (typical)**:
- **Data Source**: PostgreSQL (Aurora-compatible)
- **Server**: Aurora endpoint hostname (not a public IP in the standard design)
- **Database**: `opendental_analytics` (confirm in deployment)
- **Data Connectivity Mode**: **Import** (recommended for pilot); DirectQuery only if justified

#### Implementation Steps

**Step 1: Database user setup**

Use SQL template **`bi_user`** with **`SELECT` on named mart tables only** (not `ALL TABLES IN SCHEMA`). See [sql/bi_user_marts_grants.sql](sql/bi_user_marts_grants.sql) and keep grants in sync with [power_bi_pilot_dataset.md](power_bi_pilot_dataset.md).

**Step 2: Power BI Data Model**

**Recommended Approach**:
1. **Import Mode** (default)
   - Full data import into Power BI
   - Best for small-medium datasets
   - Enables Power BI calculated columns
   - Faster dashboard performance

2. **DirectQuery Mode** (for large datasets)
   - Live connection to PostgreSQL
   - Real-time data
   - Limited Power BI features
   - Better for large fact tables

**Data Model Design**:
- **Star Schema**: Use existing dbt star schema
- **Dimensions**: `dim_patient`, `dim_provider`, `dim_procedure`, `dim_date`
- **Facts**: `fact_appointment`, `fact_payment`, `fact_claim`, `fact_procedure`
- **Marts**: Pre-aggregated metrics for fast dashboards

**Step 3: Power BI Reports**

**Recommended Reports**:
1. **Executive Summary**
   - Revenue trends
   - Provider performance
   - Key metrics cards

2. **Financial Dashboard**
   - AR aging analysis
   - Collection rates
   - Revenue by provider

3. **Operational Dashboard**
   - Appointment utilization
   - Schedule efficiency
   - Hygiene retention

**Step 4: Power BI Service**

- Publish reports to a workspace; configure **scheduled refresh** on the **gateway** that reaches **private Aurora** (required for this architecture — the Microsoft cloud does not connect to the cluster without a gateway path).
- Share with stakeholders; mobile app access per tenant policy.

#### Professional Development Value

**Skills Developed**:
- Power BI Desktop proficiency
- DAX formula language
- Power Query M language
- Data modeling best practices
- Power BI Service administration

**Portfolio Projects**:
- Power BI report suite
- DAX calculations showcase
- Data model documentation

---

### 3. Looker Integration

#### Overview
Looker provides modern BI with LookML semantic layer. Strong API-first architecture and embedded analytics capabilities.

#### Connection Method: Direct PostgreSQL

**Connection Details**:
- **Database**: PostgreSQL
- **Host**: `POSTGRES_ANALYTICS_HOST`
- **Port**: `5432`
- **Database Name**: `opendental_analytics`
- **Username**: `looker_user`
- **Schema**: `marts` (primary)

#### Implementation Steps

**Step 1: Database User Setup**
```sql
CREATE USER looker_user WITH PASSWORD 'secure_password_here';
GRANT USAGE ON SCHEMA marts TO looker_user;
GRANT SELECT ON ALL TABLES IN SCHEMA marts TO looker_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA marts GRANT SELECT ON TABLES TO looker_user;
```

**Step 2: LookML Model Development**

**LookML Structure**:
```lookml
connection: postgres_analytics {
  host: "POSTGRES_ANALYTICS_HOST"
  port: 5432
  database: "opendental_analytics"
  user: "looker_user"
  password: "secure_password"
  schema: "marts"
}

model: dental_analytics {
  connection: postgres_analytics
  
  explore: mart_revenue_lost {
    label: "Revenue Lost Analysis"
    description: "Revenue recovery opportunities and lost revenue tracking"
  }
  
  explore: mart_provider_performance {
    label: "Provider Performance"
    description: "Provider productivity and efficiency metrics"
  }
  
  # ... additional explores
}
```

**Step 3: Looker Dashboards**

**Recommended Dashboards**:
- Executive KPI Dashboard
- Financial Analytics Dashboard
- Operational Efficiency Dashboard
- Patient Analytics Dashboard

#### Professional Development Value

**Skills Developed**:
- LookML modeling language
- Semantic layer design
- Looker API integration
- Embedded analytics
- Modern BI architecture

**Portfolio Projects**:
- LookML model documentation
- Looker dashboard suite
- API integration examples

---

### 4. Metabase Integration

#### Overview
Metabase is an open-source BI tool with easy setup and user-friendly interface. Great for ad-hoc analysis and team collaboration.

#### Connection Method: Direct PostgreSQL

**Connection Details**:
- **Database Type**: PostgreSQL
- **Name**: Dental Analytics
- **Host**: `POSTGRES_ANALYTICS_HOST`
- **Port**: `5432`
- **Database Name**: `opendental_analytics`
- **Username**: `metabase_user`
- **Password**: (stored in Metabase)

#### Implementation Steps

**Step 1: Database User Setup**
```sql
CREATE USER metabase_user WITH PASSWORD 'secure_password_here';
GRANT USAGE ON SCHEMA marts TO metabase_user;
GRANT SELECT ON ALL TABLES IN SCHEMA marts TO metabase_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA marts GRANT SELECT ON TABLES TO metabase_user;
```

**Step 2: Metabase Setup**

**Installation Options**:
1. **Docker** (recommended for development)
   ```bash
   docker run -d -p 3000:3000 \
     -e "MB_DB_TYPE=postgres" \
     -e "MB_DB_DBNAME=metabase" \
     -e "MB_DB_PORT=5432" \
     -e "MB_DB_USER=metabase" \
     -e "MB_DB_PASS=password" \
     metabase/metabase
   ```

2. **Local Installation** (Java required)
   ```bash
   java -jar metabase.jar
   ```

**Step 3: Metabase Dashboards**

**Recommended Dashboards**:
- Quick Metrics Dashboard
- Ad-hoc Analysis Workspace
- Team Collaboration Dashboard

#### Professional Development Value

**Skills Developed**:
- Open-source BI tool administration
- Self-hosted analytics platform
- SQL query building
- Dashboard design

**Portfolio Projects**:
- Metabase deployment documentation
- Dashboard collection
- SQL query library

---

### 5. API-Based Connections

#### Overview
For tools that don't support direct database connections or for controlled access scenarios.

#### Implementation: FastAPI Extensions

**New Endpoints**:
```python
# api/routers/bi_tools.py

@router.get("/bi/marts/{mart_name}")
async def get_mart_data(
    mart_name: str,
    limit: int = 1000,
    offset: int = 0,
    filters: Optional[Dict] = None
):
    """BI tool endpoint for mart data access"""
    # Implementation
    pass

@router.get("/bi/marts/{mart_name}/schema")
async def get_mart_schema(mart_name: str):
    """Return schema information for BI tools"""
    pass
```

**Use Cases**:
- Tableau Web Data Connector
- Power BI REST API
- Custom embedded analytics
- External stakeholder access

---

## Professional Development Benefits

### Technical Skills Development

#### 1. BI Tool Proficiency
- **Tableau**: Industry-standard visualization tool
- **Power BI**: Microsoft ecosystem integration
- **Looker**: Modern semantic layer architecture
- **Metabase**: Open-source BI platform

#### 2. Data Modeling
- Star schema design patterns
- Dimension and fact table relationships
- Pre-aggregation strategies
- Performance optimization

#### 3. SQL Expertise
- Complex analytical queries
- Window functions and CTEs
- Performance tuning
- Database-specific optimizations

#### 4. API Development
- REST API design for BI tools
- Pagination and filtering
- Authentication and authorization
- Rate limiting and caching

### Portfolio Development

#### 1. Dashboard Showcase
- Executive dashboards
- Financial analytics
- Operational metrics
- Patient analytics

#### 2. Technical Documentation
- Connection guides
- Data model documentation
- API documentation
- Architecture diagrams

#### 3. Code Examples
- dbt model examples
- BI tool connection scripts
- API endpoint implementations
- Database setup scripts

### Career Development

#### 1. Analytics Engineering
- dbt model development
- Data warehouse design
- ETL/ELT pipeline understanding
- Data quality and testing

#### 2. Business Intelligence
- Dashboard design
- KPI development
- Data storytelling
- Stakeholder communication

#### 3. Full-Stack Analytics
- Backend API development
- Frontend dashboard development
- Database administration
- DevOps for analytics

---

## Implementation Roadmap

### Phase 1: Foundation (Weeks 1-2)

**Goal**: Set up database access and basic connections

**Deliverables**:
1. ✅ Database user creation scripts
2. ✅ dbt grants configuration
3. ✅ Connection documentation
4. ✅ Basic Tableau connection test

**Effort**: 8-10 hours

**Tasks**:
- [ ] Create `tableau_user` with proper grants
- [x] Create `bi_user` with selective grants (Power BI; [sql/bi_user_marts_grants.sql](sql/bi_user_marts_grants.sql))
- [ ] Update dbt_project.yml with grants
- [ ] Test Tableau Desktop connection
- [ ] Test Power BI Desktop connection
- [x] Document connection details (Power BI: [powerbi/README.md](../../powerbi/README.md))

---

### Phase 2: Power BI Integration (Weeks 3-4) — IN PROGRESS

**Goal**: Power BI integration with data model

**Why first**: Power BI is the most frequently required BI tool in data analyst /
analytics engineer / data engineer job postings; prioritized ahead of Tableau.

**Deliverables**:
1. ✅ Power BI connection setup (local pilot: [powerbi/README.md](../../powerbi/README.md))
2. ✅ Power BI data model design ([powerbi/model_guide.md](../../powerbi/model_guide.md))
3. ✅ DAX calculations starter set ([powerbi/measures.dax](../../powerbi/measures.dax))
4. ⬜ Sample Power BI report (PBIP committed under `powerbi/`)

**Effort**: 10-14 hours

**Tasks**:
- [x] Create `bi_user` grant workflow (local; [sql/bi_user_marts_grants.sql](sql/bi_user_marts_grants.sql))
- [x] Design Power BI data model (4-table pilot star: facts `mart_revenue_lost`, `mart_provider_performance`; dims `dim_provider`, `dim_date`)
- [x] Draft DAX measures aligned with `executive_dashboard` exposure
- [x] Document Power BI setup (local + deployed gateway story)
- [x] Add exposure to exposures.yml (`power_bi_executive_pilot`)
- [ ] Build Power BI report in Desktop ([powerbi/report_checklist.md](../../powerbi/report_checklist.md))
- [ ] Save as PBIP and commit; export screenshots

---

### Phase 3: Tableau Integration (Weeks 5-6)

**Goal**: Full Tableau integration with sample workbooks

**Deliverables**:
1. ✅ Tableau connection documentation
2. ✅ Sample Tableau workbook (Executive Dashboard)
3. ✅ Tableau exposure in dbt
4. ✅ Connection guide document

**Effort**: 12-16 hours

**Tasks**:
- [ ] Create Tableau workbook using marts
- [ ] Build Executive KPI dashboard
- [ ] Add calculated fields and LOD expressions
- [ ] Document Tableau best practices
- [ ] Add exposure to exposures.yml
- [ ] Create connection guide

---

### Phase 4: Open-Source Tools (Weeks 7-8)

**Goal**: Metabase integration for team analytics

**Deliverables**:
1. ✅ Metabase installation and setup
2. ✅ Metabase dashboards
3. ✅ SQL query library
4. ✅ Team access documentation

**Effort**: 8-12 hours

**Tasks**:
- [ ] Set up Metabase (Docker or local)
- [ ] Configure PostgreSQL connection
- [ ] Create sample dashboards
- [ ] Document Metabase setup
- [ ] Add exposure to exposures.yml

---

### Phase 5: API Extensions (Weeks 9-10)

**Goal**: API-based BI tool connections

**Deliverables**:
1. ✅ FastAPI BI endpoints
2. ✅ API documentation
3. ✅ Web Data Connector examples
4. ✅ Authentication and rate limiting

**Effort**: 12-16 hours

**Tasks**:
- [ ] Extend FastAPI with BI endpoints
- [ ] Implement pagination and filtering
- [ ] Add caching layer
- [ ] Create API documentation
- [ ] Build Web Data Connector example

---

### Phase 6: Advanced Features (Weeks 11-12)

**Goal**: Advanced BI features and optimization

**Deliverables**:
1. ✅ Performance optimization
2. ✅ Advanced dashboards
3. ✅ Embedded analytics examples
4. ✅ Comprehensive documentation

**Effort**: 16-20 hours

**Tasks**:
- [ ] Optimize database queries
- [ ] Create advanced visualizations
- [ ] Implement embedded analytics
- [ ] Complete documentation
- [ ] Portfolio showcase preparation

---

## Security & Governance

### Database Access Control

#### User Management
```sql
-- Principle of least privilege
CREATE USER bi_tool_user WITH PASSWORD 'secure_password';

-- Schema-level access
GRANT USAGE ON SCHEMA marts TO bi_tool_user;

-- Table-level access (read-only)
GRANT SELECT ON ALL TABLES IN SCHEMA marts TO bi_tool_user;

-- Future table access (for dbt auto-grants)
ALTER DEFAULT PRIVILEGES IN SCHEMA marts 
  GRANT SELECT ON TABLES TO bi_tool_user;
```

#### Schema Isolation
- **marts**: Primary access for BI tools (recommended)
- **intermediate**: Advanced analysis (optional)
- **staging**: Raw data exploration (rarely needed)
- **raw**: No access (ETL pipeline only)

### Network Security

#### Connection Security
- **SSL/TLS**: Require encrypted connections
- **IP Whitelisting**: Restrict by IP address (if possible)
- **VPN**: Use VPN for remote access
- **Firewall Rules**: Limit database port exposure

#### Credential Management
- **Environment Variables**: Store credentials securely
- **Secrets Management**: Use tools like HashiCorp Vault
- **Rotation Policy**: Regular password rotation
- **No Hardcoding**: Never commit credentials

### Data Privacy

#### PII Handling
- **HIPAA Compliance**: Ensure patient data protection
- **Data Masking**: Consider masking sensitive fields
- **Access Logging**: Log all data access
- **Audit Trail**: Track who accessed what data

#### Row-Level Security (Optional)
```sql
-- Example: Provider-specific data access
CREATE POLICY provider_data_policy ON marts.mart_provider_performance
  FOR SELECT
  USING (provider_id = current_setting('app.current_provider_id')::int);
```

---

## Documentation & Exposure Management

### dbt Exposures

#### Exposure Structure
```yaml
exposures:
  - name: tableau_connection
    label: "Tableau Analytics Connection"
    type: dashboard
    maturity: high
    owner:
      name: Analytics Team
      email: analytics@dentalclinic.com
    
    depends_on:
      - ref('mart_revenue_lost')
      - ref('mart_provider_performance')
    
    url: "postgresql://tableau_user@host:5432/opendental_analytics"
    
    description: >
      Direct PostgreSQL connection for Tableau analytics.
      Access to all marts and intermediate models.
```

#### Exposure Categories
1. **Direct Database Connections**
   - Tableau
   - Power BI
   - Looker
   - Metabase

2. **API Connections**
   - Tableau Web Data Connector
   - Power BI REST API
   - Custom API endpoints

3. **Embedded Analytics**
   - Tableau Embedded
   - Power BI Embedded
   - Looker Embedded

### Documentation Standards

#### Connection Guides
Each BI tool should have:
- Connection setup instructions
- Credential requirements
- Schema access details
- Sample queries
- Troubleshooting guide

#### Data Model Documentation
- Mart descriptions
- Column definitions
- Relationship diagrams
- Sample data
- Refresh schedules

#### Dashboard Documentation
- Dashboard purpose
- Data sources
- Calculation logic
- Update frequency
- User guide

---

## Best Practices & Patterns

### Database Connection Best Practices

#### 1. User Naming Convention
```
{tableau|powerbi|looker|metabase}_user
```

#### 2. Grant Strategy
```sql
-- Always use schema-level grants first
GRANT USAGE ON SCHEMA marts TO bi_user;

-- Then table-level grants
GRANT SELECT ON ALL TABLES IN SCHEMA marts TO bi_user;

-- Future-proof with default privileges
ALTER DEFAULT PRIVILEGES IN SCHEMA marts 
  GRANT SELECT ON TABLES TO bi_user;
```

#### 3. Connection Pooling
- Use connection pooling for BI tools
- Configure appropriate pool size
- Monitor connection usage

### dbt Model Best Practices

#### 1. Mart Design for BI Tools
- **Star Schema**: Dimensions and facts
- **Pre-aggregation**: Summary marts for performance
- **Clear Naming**: Business-friendly column names
- **Documentation**: Comprehensive descriptions

#### 2. Performance Optimization
- **Indexes**: On join keys and filter columns
- **Materialization**: Tables for marts (not views)
- **Partitioning**: For large fact tables (if needed)
- **Query Optimization**: Test query performance

#### 3. Data Quality
- **Tests**: Comprehensive dbt tests
- **Null Handling**: Explicit null handling
- **Data Types**: Appropriate data types
- **Constraints**: Foreign key relationships

### BI Tool Best Practices

#### 1. Dashboard Design
- **KPI Cards**: Key metrics at top
- **Visual Hierarchy**: Most important data prominent
- **Interactivity**: Filters and drill-downs
- **Performance**: Fast load times

#### 2. Data Refresh
- **Scheduled Refresh**: Automated data updates
- **Incremental Updates**: For large datasets
- **Refresh Notifications**: Alert on failures
- **Version Control**: Track dashboard changes

#### 3. User Experience
- **Documentation**: In-dashboard help
- **Training**: User guides and tutorials
- **Support**: Clear contact information
- **Feedback**: Collect user feedback

---

## Success Metrics

### Technical Metrics

#### Connection Success
- ✅ All BI tools successfully connected
- ✅ Database users created and configured
- ✅ dbt grants working correctly
- ✅ Sample dashboards functional

#### Performance Metrics
- Dashboard load time < 3 seconds
- Query response time < 1 second (for marts)
- Connection pool utilization < 80%
- Database CPU usage < 70%

### Professional Development Metrics

#### Skills Acquired
- ✅ Tableau Desktop proficiency
- ✅ Power BI Desktop proficiency
- ✅ Looker/LookML experience
- ✅ Metabase administration
- ✅ API development for BI

#### Portfolio Deliverables
- ✅ 4+ BI tool integrations
- ✅ 10+ sample dashboards
- ✅ Comprehensive documentation
- ✅ Connection guides
- ✅ Architecture diagrams

### Business Value Metrics

#### Data Access
- Multiple access patterns available
- Flexible tool selection
- Reduced dependency on single tool
- Improved analytics capabilities

#### User Adoption
- BI tool usage tracking
- Dashboard view counts
- User feedback collection
- Training completion rates

---

## Next Steps

### Immediate Actions (This Week)

1. **Review and Approve Plan**
   - Validate tool selection
   - Confirm resource availability
   - Set timeline expectations

2. **Set Up Development Environment**
   - Install BI tools (Tableau Desktop, Power BI Desktop)
   - Set up Metabase (Docker or local)
   - Prepare database access

3. **Create Database Users**
   - Execute user creation scripts
   - Configure grants
   - Test connections

### Week 1-2: Foundation

1. **Database Setup**
   - Create all BI tool users
   - Configure dbt grants
   - Test basic connections

2. **Documentation**
   - Connection guides
   - User setup instructions
   - Troubleshooting guides

### Week 3-4: Power BI Integration (current focus)

1. **Power BI Development**
   - Build the pilot report in Desktop per [powerbi/report_checklist.md](../../powerbi/report_checklist.md)
   - Save as PBIP and commit; export screenshots
   - Document best practices learned

2. **Exposure Management**
   - Power BI exposure added to dbt (`power_bi_executive_pilot`)
   - Keep `bi_user` grants in sync as tables are added

### Ongoing: Iterative Development

1. **Tool Integration**
   - Add new BI tools incrementally
   - Build sample dashboards
   - Document each integration

2. **Portfolio Development**
   - Create showcase materials
   - Document architecture
   - Build example projects

---

## Conclusion

This BI Tools Development Plan provides a comprehensive roadmap for integrating multiple BI tools with your dbt models and PostgreSQL analytics warehouse. The plan emphasizes:

1. **Professional Development**: Learning industry-standard BI tools
2. **Portfolio Building**: Creating showcase materials
3. **Flexible Access**: Multiple connection patterns for different needs
4. **Best Practices**: Security, governance, and documentation

**Key Benefits**:
- ✅ Multiple BI tool proficiency
- ✅ Portfolio demonstration capabilities
- ✅ Flexible data access patterns
- ✅ Industry-standard workflows
- ✅ Professional skill development

**Success Criteria**:
- All major BI tools connected and functional
- Sample dashboards created for each tool
- Comprehensive documentation complete
- Portfolio materials ready for demonstration

---

**Document Version**: 1.0  
**Created**: 2025-01-XX  
**Last Updated**: 2025-01-XX  
**Next Review**: After Phase 1 completion  
**Owner**: Data Engineering & Analytics  
**Status**: Planning Phase - Ready for Implementation

