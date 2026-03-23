#!/bin/bash
# Setup script for dbt environment variables on EC2 instance
# This script sets up database connection credentials for dbt to connect to RDS
# Reads credentials from deployment_credentials.json
# 
# Usage:
#   source scripts/setup_ec2_dbt_env.sh
#   OR add to ~/.bashrc for persistence:
#   echo 'source /opt/dbt_dental_clinic/scripts/setup_ec2_dbt_env.sh' >> ~/.bashrc

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}Setting up dbt environment variables for RDS connection...${NC}"

# Find deployment_credentials.json file
CREDENTIALS_FILE=""
CREDENTIALS_LOCATIONS=(
    "/opt/dbt_dental_clinic/deployment_credentials.json"  # EC2 deployment location
    "$HOME/deployment_credentials.json"                   # User home directory
    "$(pwd)/deployment_credentials.json"                   # Current directory
    "$(dirname "$0")/../deployment_credentials.json"      # Project root (if script is in scripts/)
)

for location in "${CREDENTIALS_LOCATIONS[@]}"; do
    if [ -f "$location" ]; then
        CREDENTIALS_FILE="$location"
        echo -e "${GREEN}✓ Found deployment_credentials.json: $CREDENTIALS_FILE${NC}"
        break
    fi
done

if [ -z "$CREDENTIALS_FILE" ]; then
    echo -e "${RED}❌ Error: deployment_credentials.json not found in any expected location${NC}"
    echo -e "${YELLOW}Expected locations:${NC}"
    for location in "${CREDENTIALS_LOCATIONS[@]}"; do
        echo -e "  - $location"
    done
    return 1 2>/dev/null || exit 1
fi

# Check if jq is available (preferred) or python3 (fallback)
USE_JQ=false
USE_PYTHON=false

if command -v jq &> /dev/null; then
    USE_JQ=true
    echo -e "${GREEN}✓ Using jq to parse JSON${NC}"
elif command -v python3 &> /dev/null; then
    USE_PYTHON=true
    echo -e "${GREEN}✓ Using python3 to parse JSON${NC}"
else
    echo -e "${RED}❌ Error: Neither jq nor python3 is available${NC}"
    echo -e "${YELLOW}Please install jq (preferred) or ensure python3 is available${NC}"
    return 1 2>/dev/null || exit 1
fi

# Extract credentials from deployment_credentials.json
# Path: backend_api.clinic_database_reference.rds.credentials.secrets.opendental_analytics.current_value
if [ "$USE_JQ" = true ]; then
    POSTGRES_ANALYTICS_HOST=$(jq -r '.backend_api.clinic_database_reference.rds.credentials.secrets.opendental_analytics.current_value.host' "$CREDENTIALS_FILE" 2>/dev/null)
    POSTGRES_ANALYTICS_PORT=$(jq -r '.backend_api.clinic_database_reference.rds.credentials.secrets.opendental_analytics.current_value.port' "$CREDENTIALS_FILE" 2>/dev/null)
    POSTGRES_ANALYTICS_DB=$(jq -r '.backend_api.clinic_database_reference.rds.credentials.secrets.opendental_analytics.current_value.dbname' "$CREDENTIALS_FILE" 2>/dev/null)
    POSTGRES_ANALYTICS_USER=$(jq -r '.backend_api.clinic_database_reference.rds.credentials.secrets.opendental_analytics.current_value.username' "$CREDENTIALS_FILE" 2>/dev/null)
    POSTGRES_ANALYTICS_PASSWORD=$(jq -r '.backend_api.clinic_database_reference.rds.credentials.secrets.opendental_analytics.current_value.password' "$CREDENTIALS_FILE" 2>/dev/null)
    AWS_DEFAULT_REGION=$(jq -r '.aws_account.region // "us-east-1"' "$CREDENTIALS_FILE" 2>/dev/null)
elif [ "$USE_PYTHON" = true ]; then
    # Use python3 to extract values
    POSTGRES_ANALYTICS_HOST=$(python3 -c "import json, sys; data = json.load(open('$CREDENTIALS_FILE')); print(data['backend_api']['clinic_database_reference']['rds']['credentials']['secrets']['opendental_analytics']['current_value']['host'])" 2>/dev/null)
    POSTGRES_ANALYTICS_PORT=$(python3 -c "import json, sys; data = json.load(open('$CREDENTIALS_FILE')); print(data['backend_api']['clinic_database_reference']['rds']['credentials']['secrets']['opendental_analytics']['current_value']['port'])" 2>/dev/null)
    POSTGRES_ANALYTICS_DB=$(python3 -c "import json, sys; data = json.load(open('$CREDENTIALS_FILE')); print(data['backend_api']['clinic_database_reference']['rds']['credentials']['secrets']['opendental_analytics']['current_value']['dbname'])" 2>/dev/null)
    POSTGRES_ANALYTICS_USER=$(python3 -c "import json, sys; data = json.load(open('$CREDENTIALS_FILE')); print(data['backend_api']['clinic_database_reference']['rds']['credentials']['secrets']['opendental_analytics']['current_value']['username'])" 2>/dev/null)
    POSTGRES_ANALYTICS_PASSWORD=$(python3 -c "import json, sys; data = json.load(open('$CREDENTIALS_FILE')); print(data['backend_api']['clinic_database_reference']['rds']['credentials']['secrets']['opendental_analytics']['current_value']['password'])" 2>/dev/null)
    AWS_DEFAULT_REGION=$(python3 -c "import json, sys; data = json.load(open('$CREDENTIALS_FILE')); print(data.get('aws_account', {}).get('region', 'us-east-1'))" 2>/dev/null)
fi

# Set defaults for optional variables
POSTGRES_ANALYTICS_SCHEMA="${POSTGRES_ANALYTICS_SCHEMA:-dbt}"
POSTGRES_ANALYTICS_SSLMODE="${POSTGRES_ANALYTICS_SSLMODE:-require}"
AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"

# Validate required environment variables
REQUIRED_VARS=(
    "POSTGRES_ANALYTICS_HOST"
    "POSTGRES_ANALYTICS_PORT"
    "POSTGRES_ANALYTICS_DB"
    "POSTGRES_ANALYTICS_USER"
    "POSTGRES_ANALYTICS_PASSWORD"
)

MISSING_VARS=()
for var in "${REQUIRED_VARS[@]}"; do
    if [ -z "${!var}" ] || [ "${!var}" = "null" ]; then
        MISSING_VARS+=("$var")
    fi
done

if [ ${#MISSING_VARS[@]} -gt 0 ]; then
    echo -e "${RED}❌ Error: Failed to extract required credentials from deployment_credentials.json${NC}"
    echo -e "${RED}Missing variables:${NC}"
    for var in "${MISSING_VARS[@]}"; do
        echo -e "  - $var"
    done
    echo -e "${YELLOW}Expected path: backend_api.clinic_database_reference.rds.credentials.secrets.opendental_analytics.current_value${NC}"
    return 1 2>/dev/null || exit 1
fi

# Export all environment variables
export POSTGRES_ANALYTICS_HOST
export POSTGRES_ANALYTICS_PORT
export POSTGRES_ANALYTICS_DB
export POSTGRES_ANALYTICS_USER
export POSTGRES_ANALYTICS_PASSWORD
export POSTGRES_ANALYTICS_SCHEMA
export POSTGRES_ANALYTICS_SSLMODE
export AWS_DEFAULT_REGION

echo -e "${GREEN}✓ Environment variables set:${NC}"
echo -e "  ${YELLOW}POSTGRES_ANALYTICS_HOST${NC}: $POSTGRES_ANALYTICS_HOST"
echo -e "  ${YELLOW}POSTGRES_ANALYTICS_PORT${NC}: $POSTGRES_ANALYTICS_PORT"
echo -e "  ${YELLOW}POSTGRES_ANALYTICS_DB${NC}: $POSTGRES_ANALYTICS_DB"
echo -e "  ${YELLOW}POSTGRES_ANALYTICS_USER${NC}: $POSTGRES_ANALYTICS_USER"
echo -e "  ${YELLOW}POSTGRES_ANALYTICS_SCHEMA${NC}: $POSTGRES_ANALYTICS_SCHEMA"
echo -e "  ${YELLOW}POSTGRES_ANALYTICS_SSLMODE${NC}: $POSTGRES_ANALYTICS_SSLMODE"
echo -e "  ${YELLOW}AWS_DEFAULT_REGION${NC}: $AWS_DEFAULT_REGION"
echo -e "${GREEN}✓ Ready to run dbt commands${NC}"
echo -e "${YELLOW}Example:${NC} cd /opt/dbt_dental_clinic/dbt_dental_models && dbt run --target clinic"
