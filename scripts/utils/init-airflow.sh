#!/bin/bash
# Initialize Airflow via Docker Compose (local Phase A).
# Prerequisites: copy .env.template → .env and set AIRFLOW_ADMIN_PASSWORD, DB creds.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$REPO_ROOT"

# Generate Fernet key for Airflow if missing from root .env
if [ ! -f .env ]; then
  echo "ERROR: .env not found. Copy .env.template to .env and configure credentials."
  exit 1
fi

if ! grep -q "^AIRFLOW_FERNET_KEY=.\+" .env; then
  FERNET_KEY=$(python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")
  echo "AIRFLOW_FERNET_KEY=$FERNET_KEY" >> .env
  echo "Added AIRFLOW_FERNET_KEY to .env"
fi

mkdir -p airflow/dags airflow/logs airflow/plugins airflow/scripts

docker-compose --profile init run --rm airflow-init

echo "Airflow initialization complete. Start with:"
echo "  docker-compose up -d postgres mysql airflow-webserver airflow-scheduler" 