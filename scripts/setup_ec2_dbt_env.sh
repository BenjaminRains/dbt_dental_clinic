#!/bin/bash
# Setup script for dbt environment variables on EC2 instance
# This script sets up database connection credentials for dbt to connect to RDS
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

# Find and load .env file
ENV_FILE=""
ENV_LOCATIONS=(
    "/opt/dbt_dental_clinic/.env"  # EC2 deployment location
    "$HOME/.env"                    # User home directory
    "$(pwd)/.env"                   # Current directory
    "$(dirname "$0")/../.env"      # Project root (if script is in scripts/)
)

for location in "${ENV_LOCATIONS[@]}"; do
    if [ -f "$location" ]; then
        ENV_FILE="$location"
        echo -e "${GREEN}✓ Found .env file: $ENV_FILE${NC}"
        break
    fi
done

if [ -z "$ENV_FILE" ]; then
    echo -e "${RED}❌ Error: .env file not found in any expected location${NC}"
    echo -e "${YELLOW}Expected locations:${NC}"
    for location in "${ENV_LOCATIONS[@]}"; do
        echo -e "  - $location"
    done
    echo -e "${YELLOW}Please create .env file from .env.template with required credentials${NC}"
    return 1 2>/dev/null || exit 1
fi

# Source the .env file
set -a  # Automatically export all variables
source "$ENV_FILE"
set +a  # Disable automatic export

# Validate required environment variables
REQUIRED_VARS=(
    "POSTGRES_ANALYTICS_HOST"
    "POSTGRES_ANALYTICS_PORT"
    "POSTGRES_ANALYTICS_DB"
    "POSTGRES_ANALYTICS_USER"
    "POSTGRES_ANALYTICS_PASSWORD"
    "POSTGRES_ANALYTICS_SCHEMA"
    "POSTGRES_ANALYTICS_SSLMODE"
    "AWS_DEFAULT_REGION"
)

MISSING_VARS=()
for var in "${REQUIRED_VARS[@]}"; do
    if [ -z "${!var}" ]; then
        MISSING_VARS+=("$var")
    fi
done

if [ ${#MISSING_VARS[@]} -gt 0 ]; then
    echo -e "${RED}❌ Error: Missing required environment variables:${NC}"
    for var in "${MISSING_VARS[@]}"; do
        echo -e "  - $var"
    done
    echo -e "${YELLOW}Please ensure all required variables are set in $ENV_FILE${NC}"
    return 1 2>/dev/null || exit 1
fi

echo -e "${GREEN}✓ Environment variables set:${NC}"
echo -e "  ${YELLOW}POSTGRES_ANALYTICS_HOST${NC}: $POSTGRES_ANALYTICS_HOST"
echo -e "  ${YELLOW}POSTGRES_ANALYTICS_PORT${NC}: $POSTGRES_ANALYTICS_PORT"
echo -e "  ${YELLOW}POSTGRES_ANALYTICS_DB${NC}: $POSTGRES_ANALYTICS_DB"
echo -e "  ${YELLOW}POSTGRES_ANALYTICS_USER${NC}: $POSTGRES_ANALYTICS_USER"
echo -e "  ${YELLOW}POSTGRES_ANALYTICS_SCHEMA${NC}: $POSTGRES_ANALYTICS_SCHEMA"
echo -e "  ${YELLOW}POSTGRES_ANALYTICS_SSLMODE${NC}: $POSTGRES_ANALYTICS_SSLMODE"
echo -e "  ${YELLOW}AWS_DEFAULT_REGION${NC}: $AWS_DEFAULT_REGION"
echo -e "${GREEN}✓ Ready to run dbt commands${NC}"
echo -e "${YELLOW}Example:${NC} cd /opt/dbt_dental_clinic/dbt_dental_models && dbt run --target dev"
