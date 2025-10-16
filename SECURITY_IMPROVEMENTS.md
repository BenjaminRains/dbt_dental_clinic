# Security Improvements - Docker Compose

## Summary
This document describes security hardening applied to `docker-compose.yml` for HIPAA compliance.

## Changes Made

### 1. PostgreSQL Port Exposure Removed
**Before:**
```yaml
ports:
  - "5432:5432"  # Database exposed to host
```

**After:**
```yaml
# SECURITY: Ports removed - use Docker network for internal access
# Uncomment ONLY for maintenance operations like pg_restore, then remove immediately:
# ports:
#   - "5432:5432"
```

**Why:** 
- Reduces attack surface
- PHI database should not be accessible from host unless absolutely necessary
- Services communicate via Docker internal network (`postgres:5432`)

**When to uncomment:** Only during database restore operations, then comment out immediately

---

### 2. Upgraded PostgreSQL Authentication
**Before:**
```yaml
# Temporarily disabled for restore: - POSTGRES_HOST_AUTH_METHOD=md5
```

**After:**
```yaml
- POSTGRES_HOST_AUTH_METHOD=scram-sha-256 # Secure authentication
```

**Why:**
- `scram-sha-256` is the modern, secure authentication method
- `md5` is cryptographically weak (vulnerable to rainbow table attacks)
- Prevents unauthorized database access

---

### 3. Airflow Admin Credentials - No More Hardcoded Defaults
**Before:**
```yaml
airflow users create \
  --username admin \
  --password admin  # ⚠️ Hardcoded insecure password
```

**After:**
```yaml
environment:
  - AIRFLOW_ADMIN_USERNAME=${AIRFLOW_ADMIN_USERNAME}
  - AIRFLOW_ADMIN_PASSWORD=${AIRFLOW_ADMIN_PASSWORD}
  - AIRFLOW_ADMIN_EMAIL=${AIRFLOW_ADMIN_EMAIL:-admin@localhost}

command: >
  bash -c "
    if [ -z \"$${AIRFLOW_ADMIN_PASSWORD}\" ]; then
      echo 'ERROR: AIRFLOW_ADMIN_PASSWORD not set. Refusing to create admin with default password.'
      exit 1
    fi
    airflow users create \
      --username $${AIRFLOW_ADMIN_USERNAME} \
      --password $${AIRFLOW_ADMIN_PASSWORD}
  "
```

**Why:**
- Prevents container from starting if admin password not set
- Forces explicit credential configuration
- Eliminates "forgot to change the default" vulnerability

---

### 4. Created `.env.template`
- Documents all required environment variables
- Provides secure setup instructions
- Includes HIPAA compliance reminders

---

### 5. Enhanced `.gitignore`
Added patterns to prevent accidental commits of:
- `converted_backup.sql`
- `schema.sql`
- `*_backup.sql`
- `*_dump.sql`

These files may contain PHI and must never be committed.

---

## Required Action Before Running

### Update your `.env` file with:
```bash
# Required new variables:
AIRFLOW_ADMIN_USERNAME=your_username
AIRFLOW_ADMIN_PASSWORD=your_secure_password  # Must be set!
AIRFLOW_ADMIN_EMAIL=your_email@example.com
```

### If you need to restore a database:
1. Uncomment the postgres ports section
2. Run your restore operation
3. **Immediately** re-comment the ports section
4. Restart containers: `docker-compose restart postgres`

---

## HIPAA Compliance Notes

These changes address:
- **Administrative Safeguards:** Access controls via authentication
- **Technical Safeguards:** Encryption in transit, access controls
- **Physical Safeguards:** Minimized exposure surface

## Testing Checklist

After applying these changes:

- [ ] Verify `.env` has all required variables
- [ ] Test Airflow init fails if `AIRFLOW_ADMIN_PASSWORD` not set
- [ ] Confirm PostgreSQL not accessible from host (unless ports uncommented)
- [ ] Verify services can communicate via Docker network
- [ ] Test Airflow login with your configured credentials

---

**Created:** October 15, 2025  
**Project:** DBT Dental Clinic - HIPAA-Compliant Analytics Platform

