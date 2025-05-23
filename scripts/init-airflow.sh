#!/bin/bash

# Generate Fernet key for Airflow
FERNET_KEY=$(python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")

# Add Fernet key to .env file if it doesn't exist
if ! grep -q "AIRFLOW_FERNET_KEY" config/.env; then
    echo "AIRFLOW_FERNET_KEY=$FERNET_KEY" >> config/.env
fi

# Ensure airflow directory structure exists
mkdir -p airflow/dags airflow/logs airflow/plugins airflow/scripts

# Initialize Airflow database
docker-compose run --rm airflow-webserver airflow db init

# Create Airflow admin user
docker-compose run --rm airflow-webserver airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin

echo "Airflow initialization complete!" 