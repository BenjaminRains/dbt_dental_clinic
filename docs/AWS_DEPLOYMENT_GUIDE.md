# AWS Deployment Guide

## Overview

Complete guide for deploying the OpenDental analytics platform on AWS. This creates a secure, scalable demonstration environment using synthetic data.

**Architecture**: Multi-tier web application with private database  
**Budget**: $35-50/month  
**Setup Time**: 20-30 hours over 2-3 weeks  
**Maintenance**: <2 hours/month

**Security Model**: Complete data separation - no connection to production OpenDental database. Uses synthetic data only.

---

## Architecture Overview

### Infrastructure Diagram

```
┌──────────────────────────────────────────────────────────┐
│              AWS Demo Infrastructure                      │
├──────────────────────────────────────────────────────────┤
│                                                           │
│  Internet → Route53 (DNS)                                │
│            ↓                                              │
│         CloudFront (CDN) + SSL                           │
│            ↓                                              │
│         S3 Bucket (Frontend)                             │
│            │                                              │
│            ├─ index.html (Landing page)                  │
│            ├─ /dashboards (React app)                    │
│            └─ /docs (Technical documentation)            │
│                                                           │
│  API calls → Application Load Balancer OR Direct EC2     │
│            ↓                                              │
│         EC2 t3.small (FastAPI + Nginx)                   │
│            ↓ (Private VPC only)                          │
│         RDS PostgreSQL t4g.micro                         │
│            │                                              │
│            ├─ Schema: raw (synthetic data)               │
│            ├─ Schema: staging (dbt staging models)       │
│            └─ Schema: marts (dbt marts)                  │
│                                                           │
└──────────────────────────────────────────────────────────┘

Monthly Cost: $35-45
```

### Security Architecture

```
┌─────────────────────────────────────────────────────┐
│  PRODUCTION ENVIRONMENT (Separate/On-Premises)      │
│  ├─ OpenDental MySQL (Real PHI data)                │
│  ├─ ETL Pipeline (Production operations)            │
│  └─ NEVER CONNECTED to demo environment            │
└─────────────────────────────────────────────────────┘
        
        ⛔ NO CONNECTION ⛔
        
┌─────────────────────────────────────────────────────┐
│  AWS DEMO ENVIRONMENT (Public)                      │
│  ├─ Synthetic Data Generator output only            │
│  ├─ No PHI, no production credentials               │
│  └─ Safe for public demonstration                   │
└─────────────────────────────────────────────────────┘
```

---

## Deployment Tiers

### Tier 1: Essential Demo ($35-45/month)

**Components**:
- S3 + CloudFront (frontend): $3-5/month
- EC2 t3.small (API): $15-17/month
- RDS t4g.micro (database): $15-18/month
- Route53 + data transfer: $2-3/month

**Suitable For**: Professional demonstration with good performance

### Tier 2: Minimal ($20-25/month)

**Components**:
- S3 + CloudFront: $3-5/month
- EC2 t3.micro: $7-10/month
- RDS t4g.micro: $10-12/month

**Suitable For**: Budget-conscious deployment, basic demo

### Tier 3: Enhanced ($55-70/month)

**Components**:
- Tier 1 stack: $35-45/month
- EC2 t3.small for Airflow: $15-20/month
- CloudWatch enhanced monitoring: $5/month

**Suitable For**: Demonstrating orchestration capabilities, production-like environment

**Recommendation**: Start with Tier 1, add Tier 3 features if demonstrating Airflow expertise

---

## Prerequisites

### Required Before Starting

- [ ] AWS account with admin access
- [ ] AWS CLI installed and configured (`aws configure`)
- [ ] Domain name (optional but recommended)
- [ ] SSH key pair created (`aws ec2 create-key-pair`)
- [ ] Synthetic data generator functional
- [ ] Frontend builds successfully (`npm run build`)
- [ ] Backend runs locally (`python main.py`)
- [ ] dbt models tested (`dbt build`)

### Credentials to Generate

```bash
# AWS credentials (from IAM user)
AWS_ACCESS_KEY_ID=your-key
AWS_SECRET_ACCESS_KEY=your-secret
AWS_DEFAULT_REGION=us-east-1

# Database passwords (save in password manager)
RDS_ADMIN_PASSWORD=$(openssl rand -base64 32)
DBT_USER_PASSWORD=$(openssl rand -base64 32)
DEMO_READONLY_PASSWORD=$(openssl rand -base64 32)
```

---

## Step 1: VPC and Networking

### 1.1 Create VPC

```bash
# Create VPC
aws ec2 create-vpc \
    --cidr-block 10.0.0.0/16 \
    --tag-specifications 'ResourceType=vpc,Tags=[{Key=Name,Value=analytics-demo-vpc}]'

# Enable DNS hostnames
aws ec2 modify-vpc-attribute \
    --vpc-id vpc-xxxxx \
    --enable-dns-hostnames
```

### 1.2 Create Subnets

```bash
# Public subnet (for EC2 web/API)
aws ec2 create-subnet \
    --vpc-id vpc-xxxxx \
    --cidr-block 10.0.1.0/24 \
    --availability-zone us-east-1a \
    --tag-specifications 'ResourceType=subnet,Tags=[{Key=Name,Value=public-subnet-a}]'

# Private subnet (for RDS)
aws ec2 create-subnet \
    --vpc-id vpc-xxxxx \
    --cidr-block 10.0.11.0/24 \
    --availability-zone us-east-1a \
    --tag-specifications 'ResourceType=subnet,Tags=[{Key=Name,Value=private-subnet-a}]'

# Private subnet B (RDS requires 2 AZs for subnet group)
aws ec2 create-subnet \
    --vpc-id vpc-xxxxx \
    --cidr-block 10.0.12.0/24 \
    --availability-zone us-east-1b \
    --tag-specifications 'ResourceType=subnet,Tags=[{Key=Name,Value=private-subnet-b}]'
```

### 1.3 Internet Gateway and Route Tables

```bash
# Create Internet Gateway
aws ec2 create-internet-gateway \
    --tag-specifications 'ResourceType=internet-gateway,Tags=[{Key=Name,Value=analytics-igw}]'

# Attach to VPC
aws ec2 attach-internet-gateway \
    --vpc-id vpc-xxxxx \
    --internet-gateway-id igw-xxxxx

# Create public route table
aws ec2 create-route-table \
    --vpc-id vpc-xxxxx \
    --tag-specifications 'ResourceType=route-table,Tags=[{Key=Name,Value=public-rt}]'

# Add route to internet
aws ec2 create-route \
    --route-table-id rtb-xxxxx \
    --destination-cidr-block 0.0.0.0/0 \
    --gateway-id igw-xxxxx

# Associate with public subnet
aws ec2 associate-route-table \
    --subnet-id subnet-public-xxxxx \
    --route-table-id rtb-xxxxx

# Enable auto-assign public IP for public subnet
aws ec2 modify-subnet-attribute \
    --subnet-id subnet-public-xxxxx \
    --map-public-ip-on-launch
```

### 1.4 Create Security Groups

**Security Group for EC2 (sg-ec2-web)**:

```bash
# Create SG for EC2
aws ec2 create-security-group \
    --group-name sg-ec2-web \
    --description "Security group for web/API EC2 instance" \
    --vpc-id vpc-xxxxx

# Allow SSH from your IP only
aws ec2 authorize-security-group-ingress \
    --group-id sg-xxxxx \
    --protocol tcp \
    --port 22 \
    --cidr YOUR_IP/32

# Allow HTTP from anywhere
aws ec2 authorize-security-group-ingress \
    --group-id sg-xxxxx \
    --protocol tcp \
    --port 80 \
    --cidr 0.0.0.0/0

# Allow HTTPS from anywhere
aws ec2 authorize-security-group-ingress \
    --group-id sg-xxxxx \
    --protocol tcp \
    --port 443 \
    --cidr 0.0.0.0/0
```

**Security Group for RDS (sg-rds-private)**:

```bash
# Create SG for RDS
aws ec2 create-security-group \
    --group-name sg-rds-private \
    --description "Security group for private RDS instance" \
    --vpc-id vpc-xxxxx

# Allow PostgreSQL from EC2 security group only
aws ec2 authorize-security-group-ingress \
    --group-id sg-rds-xxxxx \
    --protocol tcp \
    --port 5432 \
    --source-group sg-ec2-xxxxx
```

**Time for Step 1**: 1-2 hours

---

## Step 2: RDS PostgreSQL Database

### 2.1 Create DB Subnet Group

```bash
# Create subnet group for RDS (requires 2+ subnets in different AZs)
aws rds create-db-subnet-group \
    --db-subnet-group-name analytics-db-subnet-group \
    --db-subnet-group-description "Subnet group for analytics demo DB" \
    --subnet-ids subnet-private-a-xxxxx subnet-private-b-xxxxx \
    --tags Key=Project,Value=AnalyticsDemo
```

### 2.2 Create RDS Instance

```bash
# Create RDS PostgreSQL instance
aws rds create-db-instance \
    --db-instance-identifier analytics-demo-db \
    --db-instance-class db.t4g.micro \
    --engine postgres \
    --engine-version 15.4 \
    --master-username dbadmin \
    --master-user-password "$RDS_ADMIN_PASSWORD" \
    --allocated-storage 20 \
    --storage-type gp3 \
    --backup-retention-period 1 \
    --no-publicly-accessible \
    --vpc-security-group-ids sg-rds-xxxxx \
    --db-subnet-group-name analytics-db-subnet-group \
    --tags Key=Project,Value=AnalyticsDemo Key=Environment,Value=Demo

# Wait for database (10-15 minutes)
aws rds wait db-instance-available \
    --db-instance-identifier analytics-demo-db

# Get endpoint
RDS_ENDPOINT=$(aws rds describe-db-instances \
    --db-instance-identifier analytics-demo-db \
    --query 'DBInstances[0].Endpoint.Address' \
    --output text)

echo "RDS Endpoint: $RDS_ENDPOINT"
```

### 2.3 Initialize Database and Users

```bash
# Temporarily allow your IP to connect for setup
aws ec2 authorize-security-group-ingress \
    --group-id sg-rds-xxxxx \
    --protocol tcp \
    --port 5432 \
    --cidr YOUR_IP/32

# Connect to RDS
psql -h $RDS_ENDPOINT -U dbadmin -d postgres

# Create database and schemas
CREATE DATABASE dental_analytics;
\c dental_analytics

CREATE SCHEMA raw;
CREATE SCHEMA staging;
CREATE SCHEMA marts;

-- Create dbt user for running models
CREATE USER dbt_user WITH PASSWORD 'your_dbt_password';
GRANT ALL PRIVILEGES ON DATABASE dental_analytics TO dbt_user;
GRANT ALL PRIVILEGES ON SCHEMA raw TO dbt_user;
GRANT ALL PRIVILEGES ON SCHEMA staging TO dbt_user;
GRANT ALL PRIVILEGES ON SCHEMA marts TO dbt_user;

-- Create read-only demo user for API
CREATE USER demo_readonly WITH PASSWORD 'your_readonly_password';
GRANT CONNECT ON DATABASE dental_analytics TO demo_readonly;
GRANT USAGE ON SCHEMA marts TO demo_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA marts TO demo_readonly;
ALTER DEFAULT PRIVILEGES IN SCHEMA marts GRANT SELECT ON TABLES TO demo_readonly;

\q

# Revoke your IP after setup complete
aws ec2 revoke-security-group-ingress \
    --group-id sg-rds-xxxxx \
    --protocol tcp \
    --port 5432 \
    --cidr YOUR_IP/32
```

**Time for Step 2**: 1-2 hours

---

## Step 3: Generate and Load Synthetic Data

### 3.1 Configure Synthetic Data Generator

**Update generator configuration** (local workstation):

```python
# etl_pipeline/synthetic_data_generator/config.py

DB_CONFIG = {
    'host': 'analytics-demo-db.xxxxx.us-east-1.rds.amazonaws.com',
    'port': 5432,
    'database': 'dental_analytics',
    'user': 'dbadmin',
    'password': os.getenv('RDS_ADMIN_PASSWORD'),  # Use environment variable
    'schema': 'raw'
}

GENERATION_CONFIG = {
    'num_patients': 100,        # Manageable dataset size
    'num_providers': 4,         # Realistic small practice
    'num_days': 90,             # 3 months of data
    'start_date': '2025-01-01',
    'completion_rate': 0.87,    # Based on validation data
    'no_show_rate': 0.05,
    'avg_production': 550,      # Based on real patterns
}
```

### 3.2 Generate Synthetic Data

**Option A: Generate Locally, Upload**

```bash
# On local workstation
cd etl_pipeline/synthetic_data_generator
export RDS_ADMIN_PASSWORD='your-password'

# Generate data
python run_generator.py --output-dir output_demo

# Result: CSV files in output_demo/
# Upload to EC2 (we'll load from there)
```

**Option B: Pre-generate Python Code**

```python
# etl_pipeline/synthetic_data_generator/demo_data_loader.py

from faker import Faker
import random
from datetime import datetime, timedelta
import psycopg2

fake = Faker()

def generate_patients(n=100):
    """Generate synthetic patient records."""
    patients = []
    for i in range(n):
        patients.append({
            'patient_id': i + 1,
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'birth_date': fake.date_of_birth(minimum_age=10, maximum_age=90),
            'email': fake.email(),
            'balance': round(random.triangular(0, 5000, 200), 2),
            'status': random.choices(['Active', 'Inactive'], weights=[90, 10])[0]
        })
    return patients

def generate_appointments(patients, providers, days=90):
    """Generate realistic appointment patterns."""
    appointments = []
    start_date = datetime.now() - timedelta(days=days)
    
    for day in range(days):
        current_date = start_date + timedelta(days=day)
        
        # Skip weekends
        if current_date.weekday() >= 5:
            continue
        
        # 8-12 appointments per day
        for _ in range(random.randint(8, 12)):
            provider = random.choice(providers)
            patient = random.choice(patients)
            
            # Realistic appointment characteristics
            hour = random.randint(8, 16)
            minute = random.choice([0, 15, 30, 45])
            
            status = random.choices(
                ['Completed', 'No Show', 'Cancelled', 'Scheduled'],
                weights=[85, 5, 5, 5]  # 85% completion rate
            )[0]
            
            production = round(random.triangular(50, 2000, 300), 2) if status == 'Completed' else 0
            
            appointments.append({
                'appointment_id': len(appointments) + 1,
                'patient_id': patient['patient_id'],
                'provider_id': provider['provider_id'],
                'appointment_date': current_date.date(),
                'status': status,
                'production_amount': production,
            })
    
    return appointments

# Load to database...
```

### 3.3 Load Data to RDS

```bash
# Connect to RDS (from EC2 or local with temporary access)
psql -h $RDS_ENDPOINT -U dbadmin -d dental_analytics

# Create raw tables and load data
\copy raw.patient FROM 'output_demo/patient.csv' CSV HEADER;
\copy raw.provider FROM 'output_demo/provider.csv' CSV HEADER;
\copy raw.appointment FROM 'output_demo/appointment.csv' CSV HEADER;
\copy raw.procedurelog FROM 'output_demo/procedurelog.csv' CSV HEADER;
\copy raw.payment FROM 'output_demo/payment.csv' CSV HEADER;
-- ... load all generated tables
```

### 3.4 Run dbt Models

```bash
# Configure dbt profiles for RDS
cd dbt_dental_models

# Add demo target to profiles.yml
```

```yaml
dbt_dental_models:
  target: demo
  outputs:
    demo:
      type: postgres
      host: analytics-demo-db.xxxxx.us-east-1.rds.amazonaws.com
      port: 5432
      user: dbt_user
      password: "{{ env_var('DBT_DEMO_PASSWORD') }}"
      dbname: dental_analytics
      schema: marts
      threads: 4
      keepalives_idle: 0
```

```bash
# Run dbt models
export DBT_DEMO_PASSWORD='your_dbt_password'

dbt deps
dbt run --target demo
dbt test --target demo
dbt docs generate --target demo
```

**Result**: All 17 marts materialized in RDS, ready for API queries

**Time for Step 3**: 4-6 hours

---

## Step 4: Deploy Backend API (EC2)

### 4.1 Launch EC2 Instance

```bash
# Launch instance in public subnet
aws ec2 run-instances \
    --image-id ami-0c55b159cbfafe1f0 \  # Amazon Linux 2023
    --instance-type t3.small \
    --key-name your-key-pair \
    --security-group-ids sg-ec2-xxxxx \
    --subnet-id subnet-public-xxxxx \
    --associate-public-ip-address \
    --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=analytics-api}]'

# Allocate and associate Elastic IP
aws ec2 allocate-address
aws ec2 associate-address \
    --instance-id i-xxxxx \
    --allocation-id eipalloc-xxxxx
```

### 4.2 Setup Application on EC2

```bash
# SSH into instance
ssh -i your-key.pem ec2-user@YOUR_EC2_IP

# Update system
sudo yum update -y

# Install dependencies
sudo yum install python3.11 python3.11-pip git nginx postgresql15 -y

# Clone repository
cd /opt
sudo git clone https://github.com/your-username/dbt_dental_clinic.git
sudo chown -R ec2-user:ec2-user dbt_dental_clinic

# Setup Python environment
cd dbt_dental_clinic/api
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 4.3 Configure Environment

```bash
# Create .env file
cat > /opt/dbt_dental_clinic/api/.env << EOF
DATABASE_URL=postgresql://demo_readonly:$DEMO_READONLY_PASSWORD@$RDS_ENDPOINT:5432/dental_analytics
ENVIRONMENT=demo
ALLOWED_ORIGINS=https://analytics-demo.your-domain.com,http://localhost:3000
EOF

# Test API
python3.11 main.py
# Should start on port 8000
# Test: curl http://localhost:8000/api/docs
```

### 4.4 Configure as System Service

```bash
# Create systemd service
sudo tee /etc/systemd/system/analytics-api.service > /dev/null <<'UNIT'
[Unit]
Description=Analytics Platform Demo API
After=network.target

[Service]
Type=simple
User=ec2-user
WorkingDirectory=/opt/dbt_dental_clinic/api
Environment="PATH=/opt/dbt_dental_clinic/api/.venv/bin:/usr/local/bin:/usr/bin"
ExecStart=/opt/dbt_dental_clinic/api/.venv/bin/uvicorn main:app --host 0.0.0.0 --port 8000
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
UNIT

# Start service
sudo systemctl daemon-reload
sudo systemctl enable --now analytics-api

# Check status
sudo systemctl status analytics-api
```

### 4.5 Configure Nginx Reverse Proxy

```bash
# Create nginx configuration
sudo tee /etc/nginx/conf.d/analytics-api.conf > /dev/null <<'NGINX'
server {
    listen 80;
    server_name _;

    # API endpoints
    location /api/ {
        proxy_pass http://127.0.0.1:8000/api/;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }

    # Health check
    location /health {
        proxy_pass http://127.0.0.1:8000/health;
        access_log off;
    }

    # Optional: serve dbt docs as static site
    location /dbt-docs/ {
        alias /var/www/dbt_docs/;
        try_files $uri $uri/ /dbt-docs/index.html;
    }
}
NGINX

# Enable nginx
sudo systemctl enable --now nginx

# Test configuration
sudo nginx -t
sudo systemctl reload nginx

# Test API through nginx
curl http://localhost/api/docs
curl http://YOUR_EC2_IP/health
```

**Time for Step 4**: 3-4 hours

---

## Step 5: Deploy Frontend

### 5.1 Build Production Frontend

```bash
# On local workstation
cd frontend

# Configure production environment
cat > .env.production << EOF
VITE_API_URL=https://api.analytics-demo.your-domain.com
VITE_ENVIRONMENT=demo
EOF

# Build production bundle
npm install
npm run build

# Verify build
ls -lh dist/
```

### 5.2 Create S3 Bucket and Upload

```bash
# Create S3 bucket (must be globally unique)
aws s3 mb s3://analytics-demo-yourname \
    --region us-east-1

# Enable static website hosting
aws s3 website s3://analytics-demo-yourname \
    --index-document index.html \
    --error-document index.html

# Upload frontend
aws s3 sync dist/ s3://analytics-demo-yourname \
    --delete \
    --cache-control "public, max-age=3600"

# Set bucket policy for public read
aws s3api put-bucket-policy \
    --bucket analytics-demo-yourname \
    --policy '{
      "Version": "2012-10-17",
      "Statement": [{
        "Sid": "PublicReadGetObject",
        "Effect": "Allow",
        "Principal": "*",
        "Action": "s3:GetObject",
        "Resource": "arn:aws:s3:::analytics-demo-yourname/*"
      }]
    }'
```

### 5.3 Setup CloudFront Distribution

```bash
# Create CloudFront distribution
aws cloudfront create-distribution \
    --origin-domain-name analytics-demo-yourname.s3.amazonaws.com \
    --default-root-object index.html \
    --comment "Analytics Platform Demo" \
    --default-cache-behavior '{
      "TargetOriginId": "S3-analytics-demo",
      "ViewerProtocolPolicy": "redirect-to-https",
      "Compress": true,
      "AllowedMethods": {
        "Quantity": 2,
        "Items": ["GET", "HEAD"]
      }
    }'

# Get distribution ID and domain
aws cloudfront list-distributions \
    --query "DistributionList.Items[?Comment=='Analytics Platform Demo'].[Id,DomainName]" \
    --output table

# Wait for deployment (10-15 minutes)
aws cloudfront wait distribution-deployed --id YOUR_DISTRIBUTION_ID
```

**Time for Step 5**: 2-3 hours

---

## Step 6: Custom Domain and SSL

### 6.1 Request SSL Certificate

```bash
# Request certificate (must be in us-east-1 for CloudFront)
aws acm request-certificate \
    --domain-name analytics-demo.your-domain.com \
    --subject-alternative-names api.analytics-demo.your-domain.com \
    --validation-method DNS \
    --region us-east-1

# Get certificate ARN
CERT_ARN=$(aws acm list-certificates \
    --region us-east-1 \
    --query 'CertificateSummaryList[0].CertificateArn' \
    --output text)

# Get DNS validation records
aws acm describe-certificate \
    --certificate-arn $CERT_ARN \
    --region us-east-1 \
    --query 'Certificate.DomainValidationOptions'
```

**Validate Certificate**:
1. Add CNAME records shown above to your domain DNS
2. Wait for validation (5-30 minutes)
3. Verify: `aws acm describe-certificate --certificate-arn $CERT_ARN --region us-east-1 --query 'Certificate.Status'`

### 6.2 Configure Route53 DNS

```bash
# Create hosted zone (if needed)
aws route53 create-hosted-zone \
    --name your-domain.com \
    --caller-reference $(date +%s)

# Get hosted zone ID
HOSTED_ZONE_ID=$(aws route53 list-hosted-zones \
    --query "HostedZones[?Name=='your-domain.com.'].Id" \
    --output text | cut -d'/' -f3)

# Create A record for frontend (CloudFront alias)
aws route53 change-resource-record-sets \
    --hosted-zone-id $HOSTED_ZONE_ID \
    --change-batch '{
      "Changes": [{
        "Action": "UPSERT",
        "ResourceRecordSet": {
          "Name": "analytics-demo.your-domain.com",
          "Type": "A",
          "AliasTarget": {
            "HostedZoneId": "Z2FDTNDATAQYW2",
            "DNSName": "d1234abcd.cloudfront.net",
            "EvaluateTargetHealth": false
          }
        }
      }]
    }'

# Create A record for API (EC2 IP)
aws route53 change-resource-record-sets \
    --hosted-zone-id $HOSTED_ZONE_ID \
    --change-batch "{
      \"Changes\": [{
        \"Action\": \"UPSERT\",
        \"ResourceRecordSet\": {
          \"Name\": \"api.analytics-demo.your-domain.com\",
          \"Type\": \"A\",
          \"TTL\": 300,
          \"ResourceRecords\": [{\"Value\": \"$YOUR_EC2_IP\"}]
        }
      }]
    }"
```

### 6.3 Update CloudFront with Custom Domain

```bash
# Update CloudFront distribution (easier via Console)
# Via AWS Console:
# 1. Go to CloudFront
# 2. Select your distribution
# 3. Edit settings:
#    - Alternate Domain Names: analytics-demo.your-domain.com
#    - SSL Certificate: Select your ACM certificate
# 4. Save changes
# 5. Wait for deployment (10-15 minutes)
```

**Time for Step 6**: 2-3 hours

---

## Step 7: Deploy Documentation

### 7.1 Setup MkDocs

```bash
# Install MkDocs (local workstation)
pip install mkdocs mkdocs-material

# Create mkdocs.yml in project root
cat > mkdocs.yml << 'EOF'
site_name: Analytics Platform Documentation
site_url: https://analytics-demo.your-domain.com/docs/
theme:
  name: material
  palette:
    primary: indigo
    accent: indigo
  features:
    - navigation.tabs
    - navigation.sections

nav:
  - Home: index.md
  - Architecture:
      - Overview: architecture/PIPELINE_ARCHITECTURE.md
      - Data Contracts: architecture/DATA_CONTRACTS.md
  - Airflow DAGs: airflow/README.md
  - ETL Pipeline:
      - Overview: etl_pipeline/README.md

docs_dir: docs_src
site_dir: site
EOF

# Organize documentation
mkdir -p docs_src/architecture docs_src/airflow docs_src/etl_pipeline
cp etl_pipeline/docs/PIPELINE_ARCHITECTURE.md docs_src/architecture/
cp etl_pipeline/docs/DATA_CONTRACTS.md docs_src/architecture/
cp airflow/README.md docs_src/airflow/
cp etl_pipeline/README.md docs_src/etl_pipeline/

# Create index
cat > docs_src/index.md << 'EOF'
# Healthcare Analytics Platform

Production-grade analytics platform demonstrating end-to-end data engineering capabilities.

## Overview

This platform processes 400+ tables from an OpenDental healthcare database, transforming
raw operational data into actionable analytics through modern data engineering practices.

## Key Features

- **Automated ETL**: Intelligent incremental loading with performance optimization
- **Dimensional Modeling**: 150+ dbt models across 9 business systems
- **Production Quality**: Airflow orchestration, comprehensive testing, monitoring
- **Validated Accuracy**: Systematically validated against industry-standard platform

## Architecture

The platform follows a modern data engineering architecture:

1. **Extraction**: Python-based ETL with intelligent strategy selection
2. **Loading**: MySQL → PostgreSQL with schema conversion
3. **Transformation**: dbt for dimensional modeling and business logic
4. **Orchestration**: Airflow DAGs for automated scheduling
5. **Analytics**: React dashboards consuming FastAPI endpoints

## Tech Stack

- **Languages**: Python, SQL, TypeScript
- **Databases**: MySQL, PostgreSQL
- **Frameworks**: dbt, Airflow, FastAPI, React
- **Infrastructure**: Docker, AWS
- **Testing**: pytest, dbt tests, contract validation

## Documentation

- [Pipeline Architecture](architecture/PIPELINE_ARCHITECTURE.md)
- [Data Contracts](architecture/DATA_CONTRACTS.md)
- [Airflow DAGs](airflow/README.md)

EOF

# Build documentation
mkdocs build
```

### 7.2 Upload Documentation to S3

```bash
# Upload docs to subdirectory
aws s3 sync site/ s3://analytics-demo-yourname/docs/ \
    --delete \
    --cache-control "public, max-age=3600"
```

**Result**: Documentation at `https://analytics-demo.your-domain.com/docs/`

**Time for Step 7**: 2-3 hours

---

## Step 8: Final Configuration and Testing

### 8.1 Enable HTTPS on Backend

```bash
# Install certbot on EC2
sudo yum install certbot python3-certbot-nginx -y

# Request certificate
sudo certbot --nginx \
    -d api.analytics-demo.your-domain.com \
    --agree-tos \
    --email your-email@example.com \
    --redirect \
    --non-interactive

# Auto-renewal is configured via systemd timer
sudo systemctl status certbot-renew.timer
```

### 8.2 Performance Optimization

```bash
# Set optimal cache headers for S3
aws s3 sync dist/ s3://analytics-demo-yourname \
    --cache-control "public, max-age=31536000" \
    --exclude "index.html" \
    --exclude "*.html"

# HTML files get shorter cache
aws s3 sync dist/ s3://analytics-demo-yourname \
    --cache-control "public, max-age=3600" \
    --exclude "*" \
    --include "*.html"

# Enable gzip in CloudFront
# (Configure via Console: Behaviors → Compress Objects Automatically: Yes)
```

### 8.3 Add Database Indexes

```sql
-- Connect to RDS
psql -h $RDS_ENDPOINT -U dbt_user -d dental_analytics

-- Add indexes for common queries
CREATE INDEX idx_appointments_date ON marts.fact_appointment(appointment_date);
CREATE INDEX idx_appointments_provider ON marts.fact_appointment(provider_id);
CREATE INDEX idx_ar_patient ON marts.mart_ar_summary(patient_id);
CREATE INDEX idx_provider_perf ON marts.mart_provider_performance(provider_id, date_id);

-- Analyze tables
ANALYZE marts.fact_appointment;
ANALYZE marts.mart_ar_summary;
ANALYZE marts.mart_provider_performance;
```

### 8.4 Complete System Tests

```bash
# Test frontend
curl -I https://analytics-demo.your-domain.com
# Expect: 200 OK, HTTPS redirect working

# Test API
curl https://api.analytics-demo.your-domain.com/health
# Expect: {"status": "healthy"}

# Test database connectivity
curl https://api.analytics-demo.your-domain.com/api/appointments/summary
# Expect: JSON response with data

# Test documentation
curl -I https://analytics-demo.your-domain.com/docs/
# Expect: 200 OK

# Browser tests
# Open in browser: https://analytics-demo.your-domain.com
# Verify:
# - Dashboards load and display data
# - No console errors
# - Fast performance (<3 seconds)
# - Mobile responsive
# - All links work
```

**Time for Step 8**: 1-2 hours

---

## Deployment Checklist

### Infrastructure Setup

- [ ] VPC created with public and private subnets
- [ ] Internet gateway attached
- [ ] Route tables configured
- [ ] Security groups created (sg-ec2-web, sg-rds-private)
- [ ] RDS PostgreSQL instance running (private)
- [ ] EC2 instance running (public subnet)
- [ ] Elastic IP allocated and associated
- [ ] S3 bucket created and configured
- [ ] CloudFront distribution deployed
- [ ] SSL certificates validated (ACM for CloudFront, Let's Encrypt for EC2)
- [ ] DNS records configured (Route53 or registrar)

### Data and Application

- [ ] Synthetic data generated
- [ ] Data loaded to RDS raw schema
- [ ] dbt models run successfully
- [ ] All 17 marts materialized
- [ ] API running as systemd service
- [ ] Nginx reverse proxy configured
- [ ] Frontend built and uploaded to S3
- [ ] Documentation rendered and uploaded

### Testing and Security

- [ ] All dashboards load with data
- [ ] API endpoints respond correctly
- [ ] HTTPS working on both frontend and API
- [ ] Database not publicly accessible
- [ ] No credentials in code repository
- [ ] No sensitive data in demo
- [ ] CORS configured correctly
- [ ] Performance <3 seconds for page loads
- [ ] Works on mobile devices
- [ ] Cross-browser tested (Chrome, Firefox, Safari)

---

## Cost Management

### Monthly Cost Breakdown

```
Service                    Instance Type    Monthly Cost
──────────────────────────────────────────────────────────
Route53 Hosted Zone        N/A              $0.50
S3 Storage + Requests      Standard         $2-3
CloudFront CDN             Standard         $1-2
EC2 (API + Nginx)          t3.small         $15-17
RDS PostgreSQL             t4g.micro        $15-18
Data Transfer              N/A              $1-2
──────────────────────────────────────────────────────────
Total:                                      $34-42/month
```

### Cost Optimization Strategies

**Reduce Costs**:
- Use t3.micro for EC2 ($7/month) if traffic is low
- Use RDS t4g.nano ($7/month) for minimal database
- Stop EC2 when not in use (saves ~50%)
- Enable CloudFront caching aggressively

**After Deployment Complete**:
- Can downgrade to ~$17/month for maintenance mode
- Can shut down entirely and redeploy when needed
- Keep S3 + documentation for ~$5/month

---

## Maintenance

### Weekly Tasks (15 minutes)

```bash
# Check service status
ssh -i your-key.pem ec2-user@YOUR_EC2_IP
sudo systemctl status analytics-api nginx

# Check logs for errors
sudo journalctl -u analytics-api --since "7 days ago" | grep -i error

# Verify dashboards still work
curl https://api.analytics-demo.your-domain.com/health
```

### Monthly Tasks (1 hour)

```bash
# Review AWS bill
aws ce get-cost-and-usage \
    --time-period Start=2025-10-01,End=2025-10-31 \
    --granularity MONTHLY \
    --metrics BlendedCost

# Update packages
ssh -i your-key.pem ec2-user@YOUR_EC2_IP
sudo yum update -y
cd /opt/dbt_dental_clinic/api
source .venv/bin/activate
pip install -r requirements.txt --upgrade

# Verify SSL certificate renewal
sudo certbot renew --dry-run

# Backup database
aws rds create-db-snapshot \
    --db-instance-identifier analytics-demo-db \
    --db-snapshot-identifier analytics-demo-backup-$(date +%Y%m%d)
```

---

## Troubleshooting

### Frontend Loads But No Data

**Symptoms**: Dashboards render but show empty state

**Diagnosis**: CORS or API connection issue

**Fix**:
```python
# In api/main.py
from fastapi.middleware.cors import CORSMiddleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://analytics-demo.your-domain.com",
        "http://localhost:3000"
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
```

### API Returns 500 Errors

**Symptoms**: `/health` endpoint fails

**Diagnosis**: Database connection failed

**Fix**:
```bash
# SSH into EC2
ssh -i your-key.pem ec2-user@YOUR_EC2_IP

# Check API logs
sudo journalctl -u analytics-api -n 100

# Test database connection
psql -h $RDS_ENDPOINT -U demo_readonly -d dental_analytics -c "SELECT 1;"

# Verify environment variables
cat /opt/dbt_dental_clinic/api/.env

# Restart service
sudo systemctl restart analytics-api
```

### Slow Performance

**Symptoms**: Page loads >5 seconds

**Diagnosis**: Missing indexes or unoptimized queries

**Fix**:
```sql
-- Add indexes to frequently queried tables
CREATE INDEX CONCURRENTLY idx_marts_date 
  ON marts.mart_appointment_summary(date_id);

CREATE INDEX CONCURRENTLY idx_marts_provider 
  ON marts.mart_provider_performance(provider_id);

-- Analyze for query planner
ANALYZE;
```

### High AWS Bill

**Symptoms**: Unexpected charges

**Common Culprits**:
- Multiple EC2 instances running (check: `aws ec2 describe-instances`)
- Large data transfer (enable CloudFront caching)
- RDS instance too large (downgrade to t4g.nano)
- Unattached EBS volumes (check: `aws ec2 describe-volumes`)

**Fix**:
```bash
# Stop unused resources
aws ec2 stop-instances --instance-ids i-xxxxx

# Delete unattached volumes
aws ec2 delete-volume --volume-id vol-xxxxx

# Enable CloudFront compression and caching
# (Configure via Console)
```

---

## Quick Reference

### Common Commands

```bash
# SSH into EC2
ssh -i ~/.ssh/your-key.pem ec2-user@YOUR_EC2_IP

# Check API status
sudo systemctl status analytics-api

# View logs
sudo journalctl -u analytics-api -f

# Restart API
sudo systemctl restart analytics-api

# Update frontend
cd ~/dbt_dental_clinic/frontend
npm run build
aws s3 sync dist/ s3://analytics-demo-yourname --delete

# Invalidate CloudFront cache
aws cloudfront create-invalidation \
    --distribution-id YOUR_DIST_ID \
    --paths "/*"

# Check AWS costs
aws ce get-cost-and-usage \
    --time-period Start=$(date -d "1 month ago" +%Y-%m-%d),End=$(date +%Y-%m-%d) \
    --granularity MONTHLY \
    --metrics BlendedCost
```

### Health Check URLs

```bash
# Frontend
https://analytics-demo.your-domain.com

# API health
https://api.analytics-demo.your-domain.com/health

# API documentation
https://api.analytics-demo.your-domain.com/api/docs

# Documentation site
https://analytics-demo.your-domain.com/docs/
```

---

## Security Best Practices

### Required Security Measures

1. **Data Separation**
   - ✅ No production data in demo environment
   - ✅ No connection to production OpenDental database
   - ✅ Synthetic data only

2. **Network Security**
   - ✅ RDS in private subnet (no public access)
   - ✅ Security groups restrict access by source
   - ✅ SSH restricted to your IP only

3. **Credentials**
   - ✅ No credentials in git repository
   - ✅ Use environment variables
   - ✅ Strong passwords (32+ characters)
   - ✅ Read-only user for API queries

4. **Transport Security**
   - ✅ HTTPS everywhere (SSL certificates)
   - ✅ HTTP redirects to HTTPS
   - ✅ TLS 1.2+ only

### Optional Enhancements

- Use AWS Secrets Manager for credentials
- Enable CloudWatch logs and monitoring
- Add WAF (Web Application Firewall)
- Enable RDS encryption at rest
- Setup CloudWatch alarms for anomalies

---

## Deployment Timeline

### Week 1: Infrastructure and Data (14-18 hours)

**Day 1-2**: VPC and Security Setup (4-6 hours)
- Create VPC, subnets, internet gateway
- Configure security groups
- Setup route tables

**Day 3-4**: Database Setup (4-6 hours)
- Deploy RDS instance
- Create database and schemas
- Configure users and permissions

**Day 5-7**: Sample Data (6-8 hours)
- Generate synthetic data
- Load to RDS
- Run dbt models
- Verify marts created

### Week 2: Application Deployment (12-16 hours)

**Day 1-2**: Backend API (6-8 hours)
- Deploy EC2 instance
- Setup FastAPI application
- Configure systemd service
- Setup Nginx reverse proxy
- Test API endpoints

**Day 3-4**: Frontend (4-6 hours)
- Build production React app
- Deploy to S3
- Setup CloudFront
- Test frontend loading

**Day 5**: DNS and SSL (2-3 hours)
- Configure custom domain
- Request and validate SSL certificates
- Update CloudFront configuration
- Test HTTPS

### Week 3: Documentation and Polish (4-8 hours)

**Day 1-2**: Documentation (3-4 hours)
- Render docs with MkDocs
- Upload to S3
- Test documentation links

**Day 3-5**: Testing and Polish (3-4 hours)
- Cross-browser testing
- Mobile responsiveness
- Performance optimization
- Fix any issues
- Final verification

**Total Time**: 30-40 hours over 3 weeks

---

## Success Criteria

### Technical Requirements

- ✅ Site loads at custom domain with HTTPS
- ✅ All dashboards functional and fast (<3 seconds)
- ✅ API endpoints return correct data
- ✅ Documentation renders properly
- ✅ No errors in browser console
- ✅ Mobile responsive
- ✅ Cross-browser compatible

### Performance Targets

- Page load time: <3 seconds
- API response time: <500ms
- Database query time: <200ms
- Uptime: >99% (monitor with CloudWatch)

### Content Quality

- No typos or grammar errors
- All links functional
- Professional presentation
- Clear navigation
- Accurate metrics (400 tables, 150 models, etc.)

---

## Post-Deployment

### Monitoring

**Setup CloudWatch Alarms**:
```bash
# CPU alarm for EC2
aws cloudwatch put-metric-alarm \
    --alarm-name ec2-cpu-high \
    --alarm-description "Alert when CPU > 80%" \
    --metric-name CPUUtilization \
    --namespace AWS/EC2 \
    --statistic Average \
    --period 300 \
    --threshold 80 \
    --comparison-operator GreaterThanThreshold \
    --evaluation-periods 2

# Database connections alarm
aws cloudwatch put-metric-alarm \
    --alarm-name rds-connections-high \
    --metric-name DatabaseConnections \
    --namespace AWS/RDS \
    --statistic Average \
    --period 300 \
    --threshold 80 \
    --comparison-operator GreaterThanThreshold
```

### Backup Strategy

```bash
# Enable automated RDS backups (already configured)
# Manual snapshot for major releases
aws rds create-db-snapshot \
    --db-instance-identifier analytics-demo-db \
    --db-snapshot-identifier pre-deployment-$(date +%Y%m%d)

# List snapshots
aws rds describe-db-snapshots \
    --db-instance-identifier analytics-demo-db
```

---

## Cleanup / Teardown

### When No Longer Needed

```bash
# Delete CloudFront distribution (takes time)
aws cloudfront delete-distribution --id YOUR_DIST_ID --if-match ETAG

# Delete S3 bucket
aws s3 rb s3://analytics-demo-yourname --force

# Terminate EC2
aws ec2 terminate-instances --instance-ids i-xxxxx

# Release Elastic IP
aws ec2 release-address --allocation-id eipalloc-xxxxx

# Delete RDS (create final snapshot)
aws rds delete-db-instance \
    --db-instance-identifier analytics-demo-db \
    --final-db-snapshot-identifier final-backup-$(date +%Y%m%d)

# Delete VPC resources (subnets, route tables, etc.)
# Delete security groups
# Delete Route53 hosted zone (if dedicated to demo)
```

**Cost After Cleanup**: $0 (only Route53 hosted zone if kept)

---

## Summary

### What This Deployment Provides

**Infrastructure**:
- Scalable multi-tier architecture on AWS
- Private database with public web application
- SSL/TLS encryption everywhere
- Professional custom domain

**Functionality**:
- Interactive dashboards with synthetic data
- Working API with documentation
- Comprehensive technical documentation
- Fast performance (<3 second loads)

**Security**:
- Complete separation from production data
- No PHI or sensitive information
- Industry-standard security practices
- Restricted access patterns

### Total Investment

**Time**: 30-40 hours setup + <2 hours/month maintenance  
**Cost**: $35-45/month during active use  
**Skills Demonstrated**: AWS deployment, cloud architecture, security, DevOps

---

## Next Steps

1. Review this guide completely
2. Prepare prerequisites (AWS account, domain, synthetic data)
3. Follow steps sequentially (don't skip VPC setup)
4. Test thoroughly before making public
5. Monitor costs and performance
6. Iterate based on usage and feedback

**Important**: This deployment is for demonstration purposes only. For production use with real PHI data, implement full HIPAA-compliant infrastructure with encryption, audit logging, and compliance controls.

