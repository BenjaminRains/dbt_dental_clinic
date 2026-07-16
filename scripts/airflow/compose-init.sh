#!/usr/bin/env bash
# One-shot Airflow metadata init for Compose (profile: init).
set -euo pipefail

if [ -z "${AIRFLOW_ADMIN_PASSWORD:-}" ]; then
  echo "ERROR: AIRFLOW_ADMIN_PASSWORD not set. Refusing to create admin with default password."
  exit 1
fi

airflow db migrate

airflow users create \
  --username "${AIRFLOW_ADMIN_USERNAME}" \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email "${AIRFLOW_ADMIN_EMAIL:-admin@localhost}" \
  --password "${AIRFLOW_ADMIN_PASSWORD}" \
  || true

echo "Airflow init complete."
