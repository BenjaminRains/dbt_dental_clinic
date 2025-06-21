# dbt Setup Instructions

## Initial Setup

1. Copy the template files:
   ```bash
   cp .dbt-env.template .dbt-env
   cp ~/.dbt/profiles.yml.template ~/.dbt/profiles.yml
   ```

2. Edit `.dbt-env` to add your database credentials

3. Run dbt using the wrapper script:
   ```bash
   ./run_dbt.sh debug   # Test connection
   ./run_dbt.sh run     # Run models
   ```

## Security Precautions

* Never commit `.dbt-env` or `profiles.yml` to version control
* Rotate credentials regularly
* Use least-privilege database users for dbt
* When working with PHI data, ensure all outputs are properly secured
