## Local Setup: Restore Databases and Run dbt (Windows + Docker)

This guide walks you through setting up the analytics (Postgres) and replication (MySQL) databases from backups and running dbt locally on Windows/PowerShell.

Prereqs:
- Docker Desktop installed and running
- PowerShell opened at project root: `C:\Users\rains\dbt_dental_clinic`
- Backups available locally:
  - Postgres analytics: a `.backup` file (custom format from pg_dump)
  - MySQL replication: a `.sql` file (plain SQL dump)

---

### 1) Create the `.env` file for Docker

This configures credentials for the Postgres and MySQL containers.

```powershell
@'
# Postgres (analytics)
POSTGRES_USER=analytics_user
POSTGRES_PASSWORD=analytics_password
POSTGRES_DATABASE=postgres

# MySQL (replication)
MYSQL_ROOT_USER=root
MYSQL_ROOT_PASSWORD=replication_root_password
MYSQL_DATABASE=opendental_replication
'@ | Out-File -Encoding ascii .\.env
```

Notes:
- You may change passwords; use the same values later when connecting/restoring.
- Postgres DB `postgres` is used to create/own the target analytics DB.

---

### 2) Start Docker services

On the first run, `docker compose` will automatically CREATE the containers from `docker-compose.yml` and start them. On subsequent runs, it simply starts existing containers.

```powershell
# (First time recommended) Pull images defined in docker-compose.yml
docker compose pull postgres mysql

# Create and start the containers in the background
docker compose up -d postgres mysql

# Verify they are running and healthy
docker ps
```

---

### 3) Restore Postgres analytics backup

**IMPORTANT**: Use Postgres 18+ image for compatibility with newer backup formats.

```powershell
# Ensure docker-compose.yml uses postgres:18 (not 16)
# Edit docker-compose.yml: image: postgres:18

# Start containers
docker compose down -v
docker compose up -d postgres mysql

# Get Postgres container name
$pg = (docker ps --filter "name=postgres" --format "{{.Names}}" | Select-Object -First 1)

# Copy backup into the container
docker cp "F:\db_backups\opendental_analytics_full_20251012_102800.backup" "${pg}:/tmp/analytics.backup"

# Create target database
docker exec -i $pg psql -U analytics_user -d postgres -c "DROP DATABASE IF EXISTS opendental_analytics;"
docker exec -i $pg psql -U analytics_user -d postgres -c "CREATE DATABASE opendental_analytics OWNER analytics_user;"

# Restore (using Postgres 18 pg_restore)
docker exec -i $pg pg_restore -U analytics_user -d opendental_analytics -Fc /tmp/analytics.backup --clean --if-exists --no-owner --no-privileges
```

Quick checks:

```powershell
docker exec -i $pg psql -U analytics_user -d opendental_analytics -c "\dn"
docker exec -i $pg psql -U analytics_user -d opendental_analytics -c "\dt raw.*"
```

**Expected result**: 6 schemas (public, raw, raw_dbt_test__audit, raw_intermediate, raw_marts, raw_staging) with 422+ tables in the raw schema.

---

### 4) Restore MySQL replication backup

**TODO**: Complete this step when ready to restore MySQL replication database.

```powershell
# Get MySQL container name
$my = (docker ps --filter "name=mysql" --format "{{.Names}}" | Select-Object -First 1)

# Ensure database exists
docker exec -i $my mysql -u root -preplication_root_password -e "CREATE DATABASE IF NOT EXISTS opendental_replication CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;"

# Restore (stream file into mysql)
Get-Content "F:\db_backups\opendental_replication_20251012_103307.sql" -Raw | docker exec -i $my mysql -u root -preplication_root_password opendental_replication
```

Quick checks:

```powershell
docker exec -i $my mysql -u root -preplication_root_password -e "SHOW TABLES FROM opendental_replication LIMIT 10;"
```

---

### 5) Create dbt profile

Place `profiles.yml` under `dbt_dental_models\` (the env manager sets `DBT_PROFILES_DIR` to this path).

```powershell
@'
dbt_dental_models:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      user: analytics_user
      password: analytics_password
      port: 5432
      dbname: opendental_analytics
      schema: raw
      threads: 4
      keepalives_idle: 0
'@ | Out-File -Encoding ascii .\dbt_dental_models\profiles.yml
```

**Note**: Schema is set to `raw` (not `public`) since that's where the restored tables are located.

---

### 6) Initialize dbt environment and run dbt

```powershell
# Load project environment manager
. .\load_env.ps1

# Initialize dbt env (installs deps, activates venv)
dbt-init

# Install dbt packages
dbt deps

# Build models and run tests
dbt build
```

Common alternatives:
- `dbt seed` (if seeds exist)
- `dbt run`
- `dbt test`

---

### 7) Optional: Connect via DBeaver

- Postgres analytics: host `localhost`, port `5432`, database `opendental_analytics`, user `analytics_user`, password `analytics_password`.
- MySQL replication: host `localhost`, port `3309`, database `opendental_replication`, user `root`, password `replication_root_password`.

---

### Progress Status

✅ **Completed**:
- Postgres analytics database restored successfully
- 6 schemas created: public, raw, raw_dbt_test__audit, raw_intermediate, raw_marts, raw_staging
- 422+ tables in raw schema
- Docker containers running with Postgres 18

⏳ **Pending**:
- MySQL replication database restore
- dbt profile creation
- dbt environment setup and run

### Troubleshooting

- If containers won't start, ensure Docker Desktop is running and ports `5432` (Postgres) and `3309` (MySQL on host) are free.
- If dbt cannot connect, verify `profiles.yml` values and that the analytics database restored to `opendental_analytics`.
- To tail logs: `docker logs -f <container_name>` for `postgres` or `mysql`.
- **Version compatibility**: Use Postgres 18+ for newer backup formats (1.16+).
