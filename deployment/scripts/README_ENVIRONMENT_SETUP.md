# Environment Setup Guide

## Quick Setup for Local Development

**⚠️ Note**: Use `aws-ssm-init` from `scripts/environment_manager.ps1` to load credentials from `deployment_credentials.json`.

### PowerShell (Windows)

```powershell
# Use the environment manager (loads from deployment_credentials.json)
aws-ssm-init

# Now you can run Python scripts or use SSM commands
python deployment/scripts/analyze_production_volumes.py
```

### Alternative: Manual Script (if scripts are available locally)

```powershell
# Load environment variables (if script exists locally)
. .\deployment\scripts\setup_ec2_environment.ps1

# Now you can run Python scripts
python deployment/scripts/analyze_production_volumes.py
```

### Bash (Linux/Mac/WSL)

```bash
# Load environment variables (if script exists locally)
source deployment/scripts/setup_ec2_environment.sh

# Now you can run Python scripts
python deployment/scripts/analyze_production_volumes.py
```

---

## Setting Up EC2 Instance Environment

### Option 1: Add to Shell Profile (Persistent)

**On EC2 instance via SSM:**

```bash
# Connect to EC2
# Get instance ID from deployment_credentials.json → backend_api.ec2.instance_id
aws ssm start-session --target <API_EC2_INSTANCE_ID>

# Add to bashrc (values from deployment_credentials.json)
# NOTE: Replace values below with actual values from deployment_credentials.json
cat >> ~/.bashrc << 'EOF'
# Database environment variables
# Get values from deployment_credentials.json
export POSTGRES_ANALYTICS_HOST="<RDS_ENDPOINT_FROM_CREDENTIALS>"
export POSTGRES_ANALYTICS_PORT="5432"
export POSTGRES_ANALYTICS_DB="opendental_analytics"
export POSTGRES_ANALYTICS_USER="analytics_user"
export POSTGRES_ANALYTICS_PASSWORD="<PASSWORD_FROM_CREDENTIALS>"

export POSTGRES_DEMO_HOST="<DEMO_DB_IP_FROM_CREDENTIALS>"
export POSTGRES_DEMO_PORT="5432"
export POSTGRES_DEMO_DB="opendental_demo"
export POSTGRES_DEMO_USER="opendental_demo_user"
export POSTGRES_DEMO_PASSWORD="<DEMO_PASSWORD_FROM_CREDENTIALS>"

export AWS_DEFAULT_REGION="us-east-1"
EOF

# Reload
source ~/.bashrc
```

### Option 2: Source Script Each Session

**On EC2 instance:**

```bash
# Upload script to EC2 first, then:
source /path/to/setup_ec2_environment.sh
```

### Option 3: Use Script in Project Directory

**If project is on EC2:**

```bash
# Navigate to project directory
cd /opt/dbt_dental_clinic  # or wherever project is

# Source the script
source deployment/scripts/setup_ec2_environment.sh
```

---

### Production Analytics Database (opendental_analytics)

| Variable | Source | Notes |
|----------|--------|-------|
| `POSTGRES_ANALYTICS_HOST` | `deployment_credentials.json` → `backend_api.production_database_reference.rds.endpoint` | RDS endpoint |
| `POSTGRES_ANALYTICS_PORT` | `5432` | Standard PostgreSQL port |
| `POSTGRES_ANALYTICS_DB` | `opendental_analytics` | Database name |
| `POSTGRES_ANALYTICS_USER` | `deployment_credentials.json` → `backend_api.production_database_reference.rds.credentials` | Database user |
| `POSTGRES_ANALYTICS_PASSWORD` | `deployment_credentials.json` → `backend_api.production_database_reference.rds.credentials` | Password (sensitive) |

### Demo Database (opendental_demo)

| Variable | Source | Notes |
|----------|--------|-------|
| `POSTGRES_DEMO_HOST` | `deployment_credentials.json` → `demo_database.database_connection.host` | Demo DB EC2 private IP |
| `POSTGRES_DEMO_PORT` | `5432` | Standard PostgreSQL port |
| `POSTGRES_DEMO_DB` | `opendental_demo` | Database name |
| `POSTGRES_DEMO_USER` | `deployment_credentials.json` → `demo_database.database_connection.user` | Database user |
| `POSTGRES_DEMO_PASSWORD` | `deployment_credentials.json` → `demo_database.database_connection.password` | Password (sensitive) |

### Local Development (via SSH Tunnel)

| Variable | Value | Notes |
|----------|-------|-------|
| `POSTGRES_HOST` | `localhost` | Via SSH tunnel |
| `POSTGRES_PORT` | `5433` | Forwarded port (default) |
| `POSTGRES_DB` | `opendental_analytics` | Database name |
| `POSTGRES_USER` | `analytics_user` | Database user |
| `POSTGRES_PASSWORD` | From `deployment_credentials.json` | Password (sensitive) |

---

## Usage Examples

### Run Production Volume Analysis (Local)

```powershell
# 1. Load environment
. .\deployment\scripts\setup_ec2_environment.ps1

# 2. Start SSH tunnel (in separate terminal)
# Get RDS endpoint from deployment_credentials.json first
aws ssm start-session --target <API_EC2_INSTANCE_ID> --document-name AWS-StartPortForwardingSessionToRemoteHost --parameters '{"host":["<RDS_ENDPOINT>"],"portNumber":["5432"],"localPortNumber":["5433"]}'
# Or use: ssm-port-forward-rds (after running aws-ssm-init)

# 3. Run analysis (in original terminal)
python deployment/scripts/analyze_production_volumes.py
```

### Run Production Volume Analysis (EC2)

```bash
# 1. Connect to EC2
# Get instance ID from deployment_credentials.json → backend_api.ec2.instance_id
aws ssm start-session --target <API_EC2_INSTANCE_ID>

# 2. Load environment (if not in bashrc)
source deployment/scripts/setup_ec2_environment.sh

# 3. Run analysis
python deployment/scripts/analyze_production_volumes.py
```

---

## Making Environment Persistent on EC2

To avoid loading environment variables each session:

```bash
# On EC2 instance
echo 'source /opt/dbt_dental_clinic/deployment/scripts/setup_ec2_environment.sh' >> ~/.bashrc
```