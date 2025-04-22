#!/bin/bash

# Set environment variables
export DBT_PROFILES_DIR=$(pwd)
export DBT_PROFILE="your_profile_name"

# Run the index creation macro
dbt run-operation create_payment_indexes --profiles-dir $DBT_PROFILES_DIR --profile $DBT_PROFILE

# Check if the operation was successful
if [ $? -eq 0 ]; then
    echo "Indexes created successfully"
else
    echo "Failed to create indexes"
    exit 1
fi 