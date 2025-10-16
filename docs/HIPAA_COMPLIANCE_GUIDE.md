# HIPAA Compliance Guide
## Dental Clinic Analytics System

**Document Version:** 1.0  
**Last Updated:** October 14, 2025  
**Status:** 🔴 Not HIPAA Compliant - Remediation Required

---

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [Current State Assessment](#current-state-assessment)
3. [HIPAA Requirements Overview](#hipaa-requirements-overview)
4. [Gap Analysis](#gap-analysis)
5. [Remediation Roadmap](#remediation-roadmap)
6. [Technical Controls](#technical-controls)
7. [Administrative Controls](#administrative-controls)
8. [Physical Controls](#physical-controls)
9. [Compliance Checklist](#compliance-checklist)
10. [Ongoing Compliance](#ongoing-compliance)

---

## Executive Summary

### Current Status: NOT HIPAA Compliant ❌

Your dental clinic analytics system currently processes Protected Health Information (PHI) but lacks critical security controls required by HIPAA. This document outlines the gaps and provides a roadmap to achieve compliance.

### Critical Issues Identified:

| Issue | Severity | HIPAA Rule | Status |
|-------|----------|------------|--------|
| Unencrypted data at rest | 🔴 CRITICAL | Security Rule §164.312(a)(2)(iv) | Not Implemented |
| Database ports exposed publicly | 🔴 CRITICAL | Security Rule §164.312(e)(1) | Not Implemented |
| No encryption in transit | 🔴 CRITICAL | Security Rule §164.312(e)(2)(ii) | Not Implemented |
| Missing access controls | 🔴 CRITICAL | Security Rule §164.312(a)(1) | Not Implemented |
| No audit logging | 🔴 CRITICAL | Security Rule §164.312(b) | Partial |
| No authentication/authorization | 🔴 CRITICAL | Security Rule §164.312(d) | Not Implemented |
| Credentials in plain text | 🟠 HIGH | Security Rule §164.310(d)(1) | Not Implemented |
| No backup/recovery procedures | 🟠 HIGH | Security Rule §164.308(a)(7)(ii)(A) | Not Implemented |
| Missing BAA with vendors | 🟠 HIGH | Privacy Rule §164.502(e) | Not Applicable (local) |
| No risk assessment documentation | 🟡 MEDIUM | Security Rule §164.308(a)(1)(ii)(A) | Not Implemented |

### Estimated Timeline to Compliance:
- **Phase 1 (Critical):** 2-3 weeks
- **Phase 2 (High Priority):** 3-4 weeks  
- **Phase 3 (Medium Priority):** 4-6 weeks
- **Total:** 9-13 weeks for full compliance

---

## Current State Assessment

### System Architecture

```
┌─────────────────────────────────────────────────────────┐
│  LOCAL DEVELOPMENT ENVIRONMENT                          │
│  Location: C:\Users\rains\dbt_dental_clinic            │
│                                                          │
│  ┌──────────────────┐                                   │
│  │ Docker Compose   │                                   │
│  │                  │                                   │
│  │  ┌────────────┐  │  ← MySQL (OpenDental source)     │
│  │  │ MySQL:8.4  │  │    - Port: EXPOSED publicly       │
│  │  └────────────┘  │    - Encryption: ❌ NONE          │
│  │                  │    - TLS/SSL: ❌ NOT CONFIGURED   │
│  │  ┌────────────┐  │                                   │
│  │  │Postgres:18 │  │  ← Analytics database             │
│  │  └────────────┘  │    - Port 5432: ⚠️ EXPOSED        │
│  │                  │    - Encryption: ❌ NONE          │
│  └──────────────────┘                                   │
│                                                          │
│  ┌──────────────────┐                                   │
│  │ FastAPI (Python) │  ← API Backend                    │
│  │ Port: 8000       │    - Authentication: ❌ NONE      │
│  │                  │    - HTTPS: ❌ NOT CONFIGURED     │
│  │                  │    - Audit Logs: ❌ NONE          │
│  └──────────────────┘                                   │
│                                                          │
│  ┌──────────────────┐                                   │
│  │ React Frontend   │  ← Web Application                │
│  │ Vite Dev Server  │    - HTTPS: ❌ NOT CONFIGURED     │
│  │ Port: 5173       │    - Authentication: ❌ NONE      │
│  └──────────────────┘                                   │
│                                                          │
│  📁 Data Storage                                         │
│  ├─ Docker Volumes (UNENCRYPTED)                        │
│  │  ├─ postgres_data → C:\ProgramData\Docker\volumes\  │
│  │  └─ mysql_data    → C:\ProgramData\Docker\volumes\  │
│  ├─ Logs (May contain PHI)                              │
│  │  └─ ./airflow/logs                                   │
│  └─ Environment Files (Plain text credentials)          │
│     ├─ .env_production                                  │
│     ├─ .env_api_production                              │
│     └─ .env_test                                        │
│                                                          │
└─────────────────────────────────────────────────────────┘
```

### Data Flow Analysis

**PHI Data Points in System:**
- Patient demographics (name, DOB, SSN, address)
- Medical/dental records
- Insurance information
- Financial records
- Clinical notes and treatment plans
- Appointment history

**Where PHI Exists:**
1. ✅ **Source:** OpenDental MySQL database
2. ❌ **In Transit:** Unencrypted network traffic (MySQL → Postgres)
3. ❌ **At Rest:** Unencrypted Docker volumes
4. ❌ **API Responses:** Unencrypted HTTP, no access controls
5. ⚠️ **Logs:** May contain PHI, stored unencrypted
6. ⚠️ **Backups:** No automated backup strategy

---

## HIPAA Requirements Overview

### The HIPAA Security Rule - Three Types of Safeguards

#### 1. Administrative Safeguards (§164.308)
**Purpose:** Policies and procedures to manage security measures

**Required:**
- ✅ Security Management Process
  - ❌ Risk Assessment
  - ❌ Risk Management
  - ❌ Sanction Policy
  - ❌ Information System Activity Review
- ✅ Assigned Security Responsibility
- ✅ Workforce Security (Clearance, Authorization, Termination)
- ✅ Information Access Management
- ✅ Security Awareness and Training
- ✅ Security Incident Procedures
- ✅ Contingency Plan (Backup, Disaster Recovery)
- ✅ Evaluation (Annual review)
- ✅ Business Associate Contracts

#### 2. Physical Safeguards (§164.310)
**Purpose:** Protect physical computer systems and buildings

**Required:**
- ✅ Facility Access Controls
- ✅ Workstation Use and Security
- ✅ Device and Media Controls
  - Data disposal procedures
  - Media re-use procedures
  - Accountability tracking
  - Data backup and storage

#### 3. Technical Safeguards (§164.312)
**Purpose:** Technology to protect ePHI and control access

**Required:**
- ✅ Access Control
  - ❌ Unique User Identification
  - ❌ Emergency Access Procedure
  - ❌ Automatic Logoff
  - ❌ Encryption and Decryption
- ✅ Audit Controls
  - ❌ Hardware, software, and/or procedural mechanisms
- ✅ Integrity Controls
  - ❌ Mechanisms to authenticate ePHI
- ✅ Person or Entity Authentication
  - ❌ Procedures to verify identity
- ✅ Transmission Security
  - ❌ Integrity controls
  - ❌ Encryption

---

## Gap Analysis

### Critical Gaps (Must Fix Immediately)

#### 1. Data Encryption ❌

**Current State:**
```yaml
# docker-compose.yml
volumes:
  postgres_data:  # ← PHI stored UNENCRYPTED
  mysql_data:     # ← PHI stored UNENCRYPTED
```

**HIPAA Requirement:** §164.312(a)(2)(iv) - Encryption and Decryption (Addressable)
> "Implement a mechanism to encrypt and decrypt electronic protected health information."

**Gap:** No encryption at rest. All PHI stored in plain text on disk.

**Risk:** 
- Physical theft of development machine exposes ALL patient data
- Unauthorized local access reveals PHI
- Non-compliance with HIPAA Security Rule

---

#### 2. Network Security ❌

**Current State:**
```yaml
postgres:
  ports:
    - "5432:5432"  # ← Database EXPOSED to internet!
```

**HIPAA Requirement:** §164.312(e)(1) - Transmission Security
> "Implement technical security measures to guard against unauthorized access to ePHI being transmitted over an electronic communications network."

**Gap:** 
- PostgreSQL port exposed publicly
- No TLS/SSL encryption
- Database accessible from any network location

**Risk:**
- Man-in-the-middle attacks can intercept PHI
- Unauthorized database access from internet
- Plain text credentials transmitted over network

---

#### 3. Access Controls ❌

**Current State:**
- No authentication on API endpoints
- No role-based access control (RBAC)
- No user management system
- Database credentials shared across all users

**HIPAA Requirement:** §164.312(a)(1) - Access Control
> "Implement technical policies and procedures that allow only authorized persons to access ePHI."

**Gap:** Anyone with network access can view ALL PHI

**Risk:**
- Unauthorized access to patient records
- No accountability for data access
- Cannot track who viewed what data

---

#### 4. Audit Logging ❌

**Current State:**
- No API access logging
- No database query logging
- No user activity tracking
- No PHI access audit trail

**HIPAA Requirement:** §164.312(b) - Audit Controls
> "Implement hardware, software, and/or procedural mechanisms that record and examine activity in information systems that contain or use ePHI."

**Gap:** Cannot demonstrate who accessed PHI, when, or why

**Risk:**
- No forensic capability for security incidents
- Cannot detect unauthorized access
- HIPAA audit failure (cannot prove compliance)

---

#### 5. Authentication & Authorization ❌

**Current State:**
```python
# api/main.py
@app.get("/patients/")  # ← NO AUTHENTICATION!
async def get_patients():
    return db.query(Patient).all()
```

**HIPAA Requirement:** §164.312(d) - Person or Entity Authentication
> "Implement procedures to verify that a person or entity seeking access to ePHI is the one claimed."

**Gap:** No user authentication, no password requirements, no MFA

**Risk:**
- Anyone can impersonate authorized users
- No way to verify identity
- No session management

---

### High Priority Gaps

#### 6. Credential Management 🟠

**Current State:**
```bash
# .env_api_production (plain text file)
POSTGRES_ANALYTICS_PASSWORD=your_password_here
POSTGRES_ANALYTICS_USER=analytics_user
```

**HIPAA Requirement:** §164.310(d)(1) - Device and Media Controls
> "Implement policies and procedures to govern the receipt and removal of hardware and electronic media that contain ePHI."

**Gap:** Secrets stored in plain text, version controlled, shared via email/slack

**Risk:**
- Password exposure in git history
- Shared credentials = no accountability
- No password rotation policy

---

#### 7. Backup & Recovery 🟠

**Current State:** No automated backup procedures

**HIPAA Requirement:** §164.308(a)(7)(ii)(A) - Data Backup Plan
> "Establish and implement procedures to create and maintain retrievable exact copies of ePHI."

**Gap:** No backup strategy, no disaster recovery plan

**Risk:**
- Data loss from hardware failure
- No recovery from ransomware
- Business continuity failure

---

### Medium Priority Gaps

#### 8. Risk Assessment Documentation 🟡

**Current State:** No formal risk assessment performed

**HIPAA Requirement:** §164.308(a)(1)(ii)(A) - Risk Assessment
> "Conduct an accurate and thorough assessment of the potential risks and vulnerabilities to the confidentiality, integrity, and availability of ePHI."

**Gap:** No documented risk analysis

---

#### 9. Security Policies 🟡

**Current State:** No written security policies

**HIPAA Requirement:** §164.316(a) - Policies and Procedures
> "Implement reasonable and appropriate policies and procedures to comply with the standards."

**Gap:** No security manual, no incident response plan

---

#### 10. Training & Awareness 🟡

**Current State:** No HIPAA training program

**HIPAA Requirement:** §164.308(a)(5) - Security Awareness and Training
> "Implement a security awareness and training program for all workforce members."

**Gap:** Team not trained on HIPAA requirements

---

## Remediation Roadmap

### Phase 1: Critical Security Controls (Weeks 1-3)

#### Week 1: Data Encryption

**1.1 Enable Disk Encryption**
```powershell
# Windows BitLocker (if not already enabled)
Enable-BitLocker -MountPoint "C:" -EncryptionMethod XtsAes256 -UsedSpaceOnly
```

**1.2 Enable Database Encryption at Rest**

PostgreSQL:
```sql
-- Enable transparent data encryption (TDE)
-- Using pgcrypto extension
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Encrypt sensitive columns
ALTER TABLE patient 
  ALTER COLUMN ssn TYPE bytea 
  USING pgp_sym_encrypt(ssn::text, current_setting('app.encryption_key'));
```

MySQL:
```sql
-- Enable InnoDB encryption
SET GLOBAL innodb_encryption_threads=4;
ALTER TABLE patient ENCRYPTION='Y';
```

**1.3 Configure TLS/SSL for Databases**

Update `docker-compose.yml`:
```yaml
postgres:
  command: >
    postgres
    -c ssl=on
    -c ssl_cert_file=/var/lib/postgresql/server.crt
    -c ssl_key_file=/var/lib/postgresql/server.key
    -c ssl_ca_file=/var/lib/postgresql/root.crt
  volumes:
    - ./certs/postgres:/var/lib/postgresql/certs:ro
  environment:
    - PGSSLMODE=require

mysql:
  command: >
    --ssl-ca=/var/lib/mysql/ca.pem
    --ssl-cert=/var/lib/mysql/server-cert.pem
    --ssl-key=/var/lib/mysql/server-key.pem
    --require_secure_transport=ON
```

**1.4 Remove Public Port Exposure**
```yaml
postgres:
  # REMOVE THIS:
  # ports:
  #   - "5432:5432"
  # Access only via Docker network

mysql:
  # Already correct - no ports exposed
```

---

#### Week 2: Access Controls & Authentication

**2.1 Implement JWT Authentication**

Create `api/auth.py`:
```python
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt
from passlib.context import CryptContext
from datetime import datetime, timedelta

SECRET_KEY = os.getenv("API_SECRET_KEY")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

def create_access_token(data: dict):
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

async def get_current_user(token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
    except JWTError:
        raise credentials_exception
    return username
```

Update `api/main.py`:
```python
from fastapi import Depends
from api.auth import get_current_user

@app.get("/patients/")
async def get_patients(current_user: str = Depends(get_current_user)):
    # Now requires authentication!
    return db.query(Patient).all()
```

**2.2 Implement Role-Based Access Control (RBAC)**

Database schema:
```sql
CREATE TABLE users (
    user_id SERIAL PRIMARY KEY,
    username VARCHAR(255) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE roles (
    role_id SERIAL PRIMARY KEY,
    role_name VARCHAR(50) UNIQUE NOT NULL,
    description TEXT
);

CREATE TABLE user_roles (
    user_id INTEGER REFERENCES users(user_id),
    role_id INTEGER REFERENCES roles(role_id),
    granted_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (user_id, role_id)
);

CREATE TABLE permissions (
    permission_id SERIAL PRIMARY KEY,
    permission_name VARCHAR(100) UNIQUE NOT NULL,
    resource VARCHAR(100) NOT NULL,
    action VARCHAR(50) NOT NULL
);

CREATE TABLE role_permissions (
    role_id INTEGER REFERENCES roles(role_id),
    permission_id INTEGER REFERENCES permissions(permission_id),
    PRIMARY KEY (role_id, permission_id)
);

-- Insert default roles
INSERT INTO roles (role_name, description) VALUES
('admin', 'Full system access'),
('provider', 'Clinical staff access'),
('billing', 'Financial data access'),
('receptionist', 'Scheduling and basic patient info');

-- Insert default permissions
INSERT INTO permissions (permission_name, resource, action) VALUES
('view_patients', 'patients', 'read'),
('edit_patients', 'patients', 'write'),
('view_financial', 'financial', 'read'),
('edit_financial', 'financial', 'write'),
('view_clinical', 'clinical', 'read'),
('edit_clinical', 'clinical', 'write');
```

**2.3 Enable HTTPS/TLS**

Generate self-signed certificate (development):
```powershell
# Generate SSL certificate
openssl req -x509 -newkey rsa:4096 -keyout key.pem -out cert.pem -days 365 -nodes
```

Update API to use HTTPS:
```python
# api/main.py
import uvicorn
import ssl

if __name__ == "__main__":
    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ssl_context.load_cert_chain('cert.pem', 'key.pem')
    
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        ssl_certfile="cert.pem",
        ssl_keyfile="key.pem"
    )
```

---

#### Week 3: Audit Logging

**3.1 Implement Comprehensive Audit Logging**

Database schema:
```sql
CREATE TABLE audit_log (
    audit_id BIGSERIAL PRIMARY KEY,
    timestamp TIMESTAMP DEFAULT NOW(),
    user_id INTEGER REFERENCES users(user_id),
    username VARCHAR(255),
    action VARCHAR(100) NOT NULL,
    resource_type VARCHAR(100),
    resource_id VARCHAR(255),
    ip_address INET,
    user_agent TEXT,
    request_data JSONB,
    response_status INTEGER,
    phi_accessed BOOLEAN DEFAULT false,
    session_id VARCHAR(255),
    INDEX idx_timestamp (timestamp),
    INDEX idx_user (user_id),
    INDEX idx_phi (phi_accessed),
    INDEX idx_resource (resource_type, resource_id)
);
```

Audit middleware:
```python
# api/middleware/audit.py
from fastapi import Request
import json
from datetime import datetime

async def audit_middleware(request: Request, call_next):
    # Capture request details
    start_time = datetime.utcnow()
    
    # Determine if this request accesses PHI
    phi_endpoints = ['/patients/', '/appointments/', '/clinical/']
    is_phi_access = any(endpoint in str(request.url) for endpoint in phi_endpoints)
    
    # Process request
    response = await call_next(request)
    
    # Log to audit table
    audit_entry = {
        'timestamp': start_time,
        'user_id': request.state.user.id if hasattr(request.state, 'user') else None,
        'username': request.state.user.username if hasattr(request.state, 'user') else 'anonymous',
        'action': f"{request.method} {request.url.path}",
        'resource_type': request.url.path.split('/')[1] if len(request.url.path.split('/')) > 1 else None,
        'ip_address': request.client.host,
        'user_agent': request.headers.get('user-agent'),
        'response_status': response.status_code,
        'phi_accessed': is_phi_access
    }
    
    # Write to database (async)
    await log_audit(audit_entry)
    
    return response

# Add to main.py
app.middleware("http")(audit_middleware)
```

**3.2 Database Activity Logging**

PostgreSQL:
```sql
-- Enable query logging
ALTER SYSTEM SET log_statement = 'mod';  -- Log all modifications
ALTER SYSTEM SET log_connections = on;
ALTER SYSTEM SET log_disconnections = on;
ALTER SYSTEM SET log_duration = on;
SELECT pg_reload_conf();
```

**3.3 Log Retention Policy**
```python
# Implement log retention (7 years for HIPAA)
AUDIT_LOG_RETENTION_YEARS = 7

async def cleanup_old_audit_logs():
    cutoff_date = datetime.now() - timedelta(days=365 * AUDIT_LOG_RETENTION_YEARS)
    await db.execute(
        "DELETE FROM audit_log WHERE timestamp < :cutoff",
        {"cutoff": cutoff_date}
    )
```

---

### Phase 2: Infrastructure Hardening (Weeks 4-7)

#### Week 4: Secrets Management

**4.1 Implement HashiCorp Vault or AWS Secrets Manager**

Using environment variables securely:
```python
# api/config.py
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential

class SecureConfig:
    def __init__(self):
        credential = DefaultAzureCredential()
        vault_url = os.getenv("AZURE_VAULT_URL")
        self.client = SecretClient(vault_url=vault_url, credential=credential)
    
    def get_secret(self, name: str) -> str:
        return self.client.get_secret(name).value

config = SecureConfig()
DB_PASSWORD = config.get_secret("postgres-password")
```

**4.2 Remove Secrets from Code**
```bash
# Create .gitignore patterns
echo "*.env" >> .gitignore
echo "*.pem" >> .gitignore
echo "*.key" >> .gitignore
echo "secrets/" >> .gitignore

# Audit git history for secrets
git log -p | grep -i "password\|secret\|key"

# Remove from history if found (DANGER - rewrites history)
git filter-branch --force --index-filter \
  "git rm --cached --ignore-unmatch .env_production" \
  --prune-empty --tag-name-filter cat -- --all
```

---

#### Week 5-6: Backup & Disaster Recovery

**5.1 Automated Database Backups**

Create `scripts/backup_databases.sh`:
```bash
#!/bin/bash
BACKUP_DIR="/backups/$(date +%Y%m%d)"
mkdir -p $BACKUP_DIR

# Backup PostgreSQL
docker exec postgres pg_dump \
  -U $POSTGRES_USER \
  -d $POSTGRES_DATABASE \
  --format=custom \
  > $BACKUP_DIR/postgres_$(date +%Y%m%d_%H%M%S).dump

# Backup MySQL
docker exec mysql mysqldump \
  -u $MYSQL_USER \
  -p$MYSQL_PASSWORD \
  $MYSQL_DATABASE \
  > $BACKUP_DIR/mysql_$(date +%Y%m%d_%H%M%S).sql

# Encrypt backups
gpg --encrypt --recipient your-key@example.com \
  $BACKUP_DIR/*.dump $BACKUP_DIR/*.sql

# Upload to secure storage (S3, Azure Blob, etc.)
aws s3 cp $BACKUP_DIR s3://your-hipaa-backups/ \
  --recursive \
  --sse AES256

# Cleanup local unencrypted backups
find $BACKUP_DIR -type f ! -name "*.gpg" -delete

# Delete backups older than 7 years (HIPAA retention)
find /backups -type d -mtime +2555 -exec rm -rf {} \;
```

Schedule with cron:
```bash
# Daily backups at 2 AM
0 2 * * * /path/to/backup_databases.sh
```

**5.2 Disaster Recovery Plan**

Document recovery procedures in `docs/DISASTER_RECOVERY.md`:
```markdown
# Disaster Recovery Procedures

## Recovery Time Objective (RTO): 4 hours
## Recovery Point Objective (RPO): 24 hours

### Backup Restoration

1. Retrieve latest backup from S3
2. Decrypt backup files
3. Restore PostgreSQL: `pg_restore -d dbname backup.dump`
4. Restore MySQL: `mysql dbname < backup.sql`
5. Verify data integrity
6. Resume operations

### Contact List
- DBA: [contact info]
- Security Officer: [contact info]
- Compliance Officer: [contact info]
```

**5.3 Test Recovery Procedures**
```bash
# Quarterly DR test schedule
# Test restoration to isolated environment
# Document test results
# Update procedures based on lessons learned
```

---

#### Week 7: Monitoring & Alerting

**7.1 Security Monitoring**

Implement security event detection:
```python
# api/monitoring/security.py
from datetime import datetime, timedelta

async def detect_anomalies():
    # Failed login attempts
    failed_logins = await db.execute("""
        SELECT user_id, COUNT(*) as attempts
        FROM audit_log
        WHERE action LIKE 'LOGIN_FAILED%'
        AND timestamp > NOW() - INTERVAL '15 minutes'
        GROUP BY user_id
        HAVING COUNT(*) > 5
    """)
    
    if failed_logins:
        await send_security_alert(
            "Multiple failed login attempts detected",
            failed_logins
        )
    
    # After-hours PHI access
    after_hours_access = await db.execute("""
        SELECT user_id, action, resource_id
        FROM audit_log
        WHERE phi_accessed = true
        AND EXTRACT(HOUR FROM timestamp) NOT BETWEEN 7 AND 19
        AND timestamp > NOW() - INTERVAL '1 hour'
    """)
    
    if after_hours_access:
        await send_security_alert(
            "After-hours PHI access detected",
            after_hours_access
        )
    
    # Bulk data export
    bulk_export = await db.execute("""
        SELECT user_id, COUNT(*) as records
        FROM audit_log
        WHERE action LIKE '%export%'
        AND timestamp > NOW() - INTERVAL '5 minutes'
        GROUP BY user_id
        HAVING COUNT(*) > 100
    """)
    
    if bulk_export:
        await send_security_alert(
            "Bulk data export detected",
            bulk_export
        )
```

**7.2 Health Checks & Uptime Monitoring**
```python
# api/health.py
@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "database": await check_db_connection(),
        "encryption": await verify_encryption_enabled(),
        "audit_log": await verify_audit_logging(),
        "timestamp": datetime.utcnow()
    }
```

---

### Phase 3: Administrative & Policy Controls (Weeks 8-13)

#### Week 8-9: Policy Documentation

Create the following policy documents:

**1. Security Policy Manual** (`docs/policies/SECURITY_POLICY.md`)
**2. Incident Response Plan** (`docs/policies/INCIDENT_RESPONSE.md`)
**3. Access Control Policy** (`docs/policies/ACCESS_CONTROL.md`)
**4. Acceptable Use Policy** (`docs/policies/ACCEPTABLE_USE.md`)
**5. Data Retention Policy** (`docs/policies/DATA_RETENTION.md`)

Example structure for Security Policy:
```markdown
# Information Security Policy

## 1. Purpose
Establish security requirements for protecting ePHI

## 2. Scope
Applies to all systems processing PHI

## 3. Roles & Responsibilities
- Security Officer: [Name]
- Privacy Officer: [Name]
- IT Administrator: [Name]

## 4. Password Requirements
- Minimum 12 characters
- Complexity: uppercase, lowercase, numbers, symbols
- Rotation: every 90 days
- No reuse of last 12 passwords

## 5. Access Control
- Least privilege principle
- Role-based access control
- Quarterly access reviews

## 6. Encryption
- All PHI encrypted at rest (AES-256)
- All PHI encrypted in transit (TLS 1.2+)

## 7. Audit Logging
- All access to PHI logged
- Logs retained for 7 years
- Monthly log reviews

## 8. Incident Response
- Report incidents within 1 hour
- Investigation within 24 hours
- Breach notification within 60 days (if applicable)

## 9. Policy Review
- Annual policy review
- Update as needed for regulatory changes
```

---

#### Week 10: Risk Assessment

**Formal HIPAA Risk Assessment** (`docs/RISK_ASSESSMENT.md`)

Template:
```markdown
# HIPAA Security Risk Assessment

**Assessment Date:** [Date]
**Assessed By:** [Name]
**Next Assessment:** [Date + 1 year]

## Methodology
- Asset Identification
- Threat Identification
- Vulnerability Assessment
- Current Controls Assessment
- Likelihood & Impact Analysis
- Risk Determination

## Asset Inventory

| Asset | Type | PHI? | Location | Owner |
|-------|------|------|----------|-------|
| PostgreSQL DB | Database | Yes | Docker Volume | IT |
| MySQL DB | Database | Yes | Docker Volume | IT |
| API Server | Application | Yes | Docker Container | Dev |
| Web Frontend | Application | Yes | Dev Server | Dev |

## Threat Analysis

| Threat | Likelihood | Impact | Risk Level |
|--------|-----------|--------|------------|
| Unauthorized database access | Medium | Critical | HIGH |
| Data breach via API | High | Critical | CRITICAL |
| Physical theft of laptop | Low | Critical | MEDIUM |
| Ransomware | Medium | High | HIGH |
| Insider threat | Low | Critical | MEDIUM |

## Risk Mitigation Plan
[Document remediation steps for each risk]

## Residual Risk
[Document remaining risks after controls implemented]
```

---

#### Week 11-12: Training Program

**HIPAA Training Curriculum** (`docs/training/HIPAA_TRAINING.md`)

Required training modules:
1. HIPAA Overview & Regulations
2. PHI Identification & Handling
3. Access Control Best Practices
4. Password Security
5. Incident Reporting
6. Social Engineering Awareness
7. Physical Security
8. Acceptable Use Policy

Training tracking:
```sql
CREATE TABLE training_records (
    record_id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(user_id),
    training_module VARCHAR(255),
    completion_date DATE,
    score INTEGER,
    expiration_date DATE,  -- Annual renewal
    certificate_url VARCHAR(500)
);
```

---

#### Week 13: Compliance Validation

**Final Compliance Checklist Review**

Run automated compliance scanner:
```python
# scripts/compliance_check.py
import asyncio
from typing import List, Dict

async def run_compliance_checks() -> Dict[str, bool]:
    results = {
        'encryption_at_rest': await verify_disk_encryption(),
        'encryption_in_transit': await verify_tls_enabled(),
        'authentication': await verify_auth_required(),
        'audit_logging': await verify_audit_logs(),
        'access_controls': await verify_rbac(),
        'backup_procedures': await verify_backups_exist(),
        'password_policy': await verify_password_complexity(),
        'session_timeout': await verify_session_timeout(),
        'policies_documented': await verify_policy_docs(),
        'training_completed': await verify_training_records(),
    }
    
    # Generate compliance report
    report = generate_compliance_report(results)
    print(report)
    
    # Alert on failures
    if not all(results.values()):
        await send_compliance_alert(results)
    
    return results

if __name__ == "__main__":
    asyncio.run(run_compliance_checks())
```

---

## Technical Controls

### Summary of Technical Controls to Implement

| Control | HIPAA Reference | Implementation | Priority |
|---------|----------------|----------------|----------|
| **Encryption at Rest** | §164.312(a)(2)(iv) | BitLocker + TDE | CRITICAL |
| **Encryption in Transit** | §164.312(e)(2)(ii) | TLS 1.2+ | CRITICAL |
| **Access Control** | §164.312(a)(1) | JWT + RBAC | CRITICAL |
| **Unique User ID** | §164.312(a)(2)(i) | User authentication | CRITICAL |
| **Emergency Access** | §164.312(a)(2)(ii) | Break-glass accounts | HIGH |
| **Automatic Logoff** | §164.312(a)(2)(iii) | Session timeout (15 min) | HIGH |
| **Audit Controls** | §164.312(b) | Comprehensive logging | CRITICAL |
| **Integrity Controls** | §164.312(c)(1) | Checksums, signatures | MEDIUM |
| **Authentication** | §164.312(d) | MFA for admin access | HIGH |
| **Transmission Security** | §164.312(e) | VPN, TLS, IPSec | CRITICAL |

### Implementation Checklist

#### Encryption
- [ ] Enable BitLocker on all workstations
- [ ] Enable Transparent Data Encryption (TDE) on PostgreSQL
- [ ] Enable encryption on MySQL
- [ ] Implement column-level encryption for SSN, payment info
- [ ] Enable TLS/SSL for all database connections
- [ ] Configure API to use HTTPS only
- [ ] Disable HTTP (port 80)

#### Access Control
- [ ] Implement user authentication (JWT)
- [ ] Implement role-based access control
- [ ] Create user management system
- [ ] Enforce password complexity (12+ chars, mixed case, numbers, symbols)
- [ ] Implement password expiration (90 days)
- [ ] Implement account lockout after 5 failed attempts
- [ ] Implement session timeout (15 minutes idle)
- [ ] Create break-glass emergency access procedure

#### Audit Logging
- [ ] Log all authentication attempts
- [ ] Log all PHI access (read, write, delete)
- [ ] Log all administrative actions
- [ ] Log all database queries
- [ ] Include: timestamp, user, action, resource, result, IP
- [ ] Implement tamper-proof log storage
- [ ] Set up log retention (7 years)
- [ ] Implement log review procedures (monthly)

#### Network Security
- [ ] Remove public database port exposure
- [ ] Implement firewall rules (whitelist only)
- [ ] Use VPN for remote access
- [ ] Implement intrusion detection (IDS)
- [ ] Implement intrusion prevention (IPS)
- [ ] Network segmentation (DMZ for web tier)

---

## Administrative Controls

### Required Policies & Procedures

#### 1. Security Management Process
- **Risk Assessment:** Annual comprehensive risk analysis
- **Risk Management:** Document risk mitigation strategies
- **Sanction Policy:** Consequences for security violations
- **Information System Activity Review:** Quarterly log audits

#### 2. Assigned Security Responsibility
- **Security Officer:** Designated individual responsible for security
- **Privacy Officer:** Designated individual for HIPAA privacy

#### 3. Workforce Security
- **Authorization/Supervision:** Define access levels by role
- **Workforce Clearance:** Background checks for PHI access
- **Termination Procedures:** Disable access immediately upon separation

#### 4. Information Access Management
- **Isolating Clearinghouse Functions:** N/A
- **Access Authorization:** Formal approval process
- **Access Establishment/Modification:** Documented procedures

#### 5. Security Awareness & Training
- **Security Reminders:** Quarterly security awareness emails
- **Protection from Malicious Software:** Antivirus training
- **Log-in Monitoring:** Training on recognizing suspicious activity
- **Password Management:** Strong password training

#### 6. Security Incident Procedures
- **Response and Reporting:** Defined incident response team
- **Documentation:** Incident log and investigation reports

#### 7. Contingency Plan
- **Data Backup Plan:** Daily automated backups
- **Disaster Recovery Plan:** Documented recovery procedures
- **Emergency Mode Operation:** Critical system access procedures
- **Testing and Revision:** Annual DR testing
- **Applications and Data Criticality Analysis:** Document critical systems

#### 8. Evaluation
- **Annual Review:** Evaluate security controls effectiveness

#### 9. Business Associate Agreements (BAA)
- **Contracts:** BAAs with all vendors handling PHI
- **Other Arrangements:** Document data sharing agreements

---

## Physical Controls

### Required Physical Safeguards

#### 1. Facility Access Controls
- [ ] Restrict physical access to server room/data center
- [ ] Badge access logs
- [ ] Visitor escort procedures
- [ ] Facility security plan
- [ ] Access control and validation procedures
- [ ] Maintenance records

#### 2. Workstation Use
- [ ] Define appropriate workstation use
- [ ] Lock screens when unattended (5 min timeout)
- [ ] Position monitors away from public view
- [ ] No unauthorized software installation
- [ ] Physical security cables for laptops

#### 3. Workstation Security
- [ ] Physical safeguards for workstations
- [ ] Secure development laptops (locked office/home)
- [ ] Encrypted hard drives (BitLocker)
- [ ] Remote wipe capability for lost/stolen devices

#### 4. Device and Media Controls
- [ ] **Disposal:** Secure data destruction (shred drives, DOD wipe)
- [ ] **Media Re-use:** Sanitize before re-use (NIST 800-88)
- [ ] **Accountability:** Track hardware containing ePHI
- [ ] **Data Backup and Storage:** Secure offsite backup storage

### Physical Security Checklist

For Development Environment:
- [ ] Laptop encrypted with BitLocker
- [ ] Strong login password (12+ characters)
- [ ] Screen lock enabled (5 min timeout)
- [ ] Physical security when away (locked drawer/office)
- [ ] Secure disposal of printed PHI (shred)
- [ ] No PHI on USB drives (use encrypted cloud storage)
- [ ] Development machine NOT accessible remotely

For Production Environment:
- [ ] Servers in locked data center
- [ ] Badge access with logging
- [ ] Surveillance cameras
- [ ] Environmental controls (fire suppression, cooling)
- [ ] Redundant power (UPS, generator)
- [ ] Physical access reviews (quarterly)

---

## Compliance Checklist

### Pre-Production Compliance Checklist

Use this checklist before deploying to production with real PHI:

#### Technical Safeguards
- [ ] All databases encrypted at rest (TDE enabled)
- [ ] All connections encrypted in transit (TLS 1.2+)
- [ ] All database ports closed to public access
- [ ] Authentication required for all API endpoints
- [ ] Role-based access control (RBAC) implemented
- [ ] Session timeout configured (15 minutes)
- [ ] Password complexity enforced (12+ chars)
- [ ] Multi-factor authentication (MFA) for admin access
- [ ] Audit logging enabled for all PHI access
- [ ] Audit logs stored securely (tamper-proof)
- [ ] Log retention configured (7 years)
- [ ] Automated security monitoring enabled
- [ ] Intrusion detection system (IDS) configured
- [ ] Firewall rules configured (whitelist only)
- [ ] Secrets stored securely (not in code/env files)

#### Administrative Safeguards
- [ ] Security Officer designated
- [ ] Privacy Officer designated
- [ ] Security policies documented
- [ ] Incident response plan documented
- [ ] Risk assessment completed (annual)
- [ ] Business continuity plan documented
- [ ] Disaster recovery plan documented
- [ ] DR plan tested (quarterly)
- [ ] Backup procedures documented
- [ ] Backup restoration tested
- [ ] HIPAA training completed (all staff)
- [ ] Training records maintained
- [ ] Access authorization procedures documented
- [ ] Termination procedures documented
- [ ] Sanction policy documented
- [ ] Data retention policy documented
- [ ] Acceptable use policy documented
- [ ] Business Associate Agreements (BAAs) signed

#### Physical Safeguards
- [ ] Workstations encrypted (BitLocker)
- [ ] Screen lock enabled (5 min timeout)
- [ ] Physical access controls implemented
- [ ] Visitor escort procedures documented
- [ ] Media disposal procedures documented
- [ ] Hardware tracking system implemented
- [ ] Secure backup storage configured

#### Documentation
- [ ] Policies and procedures manual complete
- [ ] Security policy reviewed and approved
- [ ] Privacy policy reviewed and approved
- [ ] Risk assessment documented
- [ ] Risk mitigation plan documented
- [ ] Audit log review procedures documented
- [ ] Incident response procedures documented
- [ ] Training curriculum documented
- [ ] System architecture documented
- [ ] Data flow diagrams created
- [ ] Compliance validation completed

---

## Ongoing Compliance

### Daily Tasks
- [ ] Monitor security alerts
- [ ] Review failed login attempts
- [ ] Check system health dashboards

### Weekly Tasks
- [ ] Review access control changes
- [ ] Check backup success/failure reports
- [ ] Review system logs for anomalies

### Monthly Tasks
- [ ] Review audit logs for PHI access
- [ ] Access control review (terminate separated employees)
- [ ] Vulnerability scanning
- [ ] Security patch deployment
- [ ] Review security incidents

### Quarterly Tasks
- [ ] Disaster recovery plan testing
- [ ] Access rights review (recertification)
- [ ] Security awareness training (refresher)
- [ ] Policy review and updates
- [ ] Physical security audit
- [ ] Compliance validation testing

### Annual Tasks
- [ ] Comprehensive risk assessment
- [ ] Security policy review and approval
- [ ] Full HIPAA training (all staff)
- [ ] Disaster recovery full test
- [ ] External security audit
- [ ] Penetration testing
- [ ] Business Associate Agreement review
- [ ] Compliance certification

---

## Development vs. Production Guidelines

### ✅ Development Environment (Synthetic Data)

**When working with synthetic/fake data ONLY:**
- ✅ Can use relaxed security (local development)
- ✅ Can expose ports locally (localhost only)
- ✅ Can use simple authentication (for testing)
- ✅ Can log verbosely (including data)
- ⚠️ **MUST** clearly label as "DEV/SYNTHETIC DATA"
- ⚠️ **MUST NOT** connect to production data sources

**Synthetic Data Generation:**
```powershell
# Use the safe synthetic data generator
cd etl_pipeline/synthetic_data_generator
.\generate.ps1 -Patients 1000 -DbPassword "demo_password"

# Verify targeting demo database
# Output should show: "Target database: opendental_demo ✅"
```

---

### ❌ Production Environment (Real PHI)

**When working with real patient data:**
- ❌ **NEVER** use development environment
- ❌ **NEVER** expose database ports publicly
- ❌ **NEVER** use HTTP (HTTPS only)
- ❌ **NEVER** disable authentication
- ❌ **NEVER** skip audit logging
- ✅ **ALWAYS** use encrypted connections
- ✅ **ALWAYS** require authentication
- ✅ **ALWAYS** log all access
- ✅ **ALWAYS** use production environment controls

**Production Deployment Checklist:**
- [ ] All items in "Pre-Production Compliance Checklist" completed
- [ ] Security audit passed
- [ ] Penetration testing passed
- [ ] Privacy Officer approval
- [ ] Security Officer approval
- [ ] Legal/Compliance review completed
- [ ] Incident response team on standby
- [ ] Backup/recovery verified

---

## Recommended Tools & Services

### Security Tools

#### Encryption
- **Disk:** BitLocker (Windows), FileVault (Mac), LUKS (Linux)
- **Database:** PostgreSQL TDE, MySQL InnoDB Encryption
- **Application:** AES-256-GCM (Python: `cryptography` library)
- **Transport:** TLS 1.2+ (OpenSSL, Let's Encrypt)

#### Secrets Management
- **Cloud:** Azure Key Vault, AWS Secrets Manager, GCP Secret Manager
- **Self-Hosted:** HashiCorp Vault, CyberArk
- **Development:** git-crypt, SOPS (Secrets OPerationS)

#### Authentication
- **SSO:** Okta, Auth0, Azure AD
- **MFA:** Duo, Google Authenticator, YubiKey
- **API:** JWT, OAuth 2.0, OpenID Connect

#### Monitoring
- **SIEM:** Splunk, ELK Stack, Azure Sentinel
- **Monitoring:** Prometheus + Grafana, DataDog
- **Log Management:** Splunk, Sumo Logic, Loggly
- **Alerting:** PagerDuty, OpsGenie, VictorOps

#### Compliance
- **Scanning:** Nessus, Qualys, OpenVAS
- **Compliance:** Vanta, Drata, SecureFrame
- **Risk Assessment:** HIPAA One, Compliancy Group

---

## Resources & References

### HIPAA Official Resources
- [HHS HIPAA Homepage](https://www.hhs.gov/hipaa)
- [HIPAA Security Rule](https://www.hhs.gov/hipaa/for-professionals/security/index.html)
- [HIPAA Privacy Rule](https://www.hhs.gov/hipaa/for-professionals/privacy/index.html)
- [Breach Notification Rule](https://www.hhs.gov/hipaa/for-professionals/breach-notification/index.html)

### Security Standards
- [NIST Cybersecurity Framework](https://www.nist.gov/cyberframework)
- [NIST 800-66: HIPAA Security Rule](https://csrc.nist.gov/publications/detail/sp/800-66/rev-2/draft)
- [NIST 800-53: Security Controls](https://csrc.nist.gov/publications/detail/sp/800-53/rev-5/final)

### Training Resources
- [HHS HIPAA Training](https://www.hhs.gov/hipaa/for-professionals/training/index.html)
- [SANS HIPAA Training](https://www.sans.org/security-awareness-training/)
- [Medcurity HIPAA Training](https://www.medcurity.com/)

### Best Practices
- [HIPAA Journal](https://www.hipaajournal.com/)
- [Healthcare IT Security](https://healthitsecurity.com/)
- [HITRUST Alliance](https://hitrustalliance.net/)

---

## Appendix A: Sample Configuration Files

### A.1 Secure docker-compose.yml

```yaml
version: '3.8'

services:
  postgres:
    image: postgres:18
    environment:
      - POSTGRES_USER=${POSTGRES_USER}
      - POSTGRES_PASSWORD=${POSTGRES_PASSWORD}
      - POSTGRES_DB=${POSTGRES_DATABASE}
    # NO PUBLIC PORTS - Access via Docker network only
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./certs/postgres:/var/lib/postgresql/certs:ro
    command: >
      postgres
      -c ssl=on
      -c ssl_cert_file=/var/lib/postgresql/certs/server.crt
      -c ssl_key_file=/var/lib/postgresql/certs/server.key
      -c ssl_ca_file=/var/lib/postgresql/certs/root.crt
      -c log_statement=mod
      -c log_connections=on
      -c log_disconnections=on
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ${POSTGRES_USER}"]
      interval: 10s
      timeout: 5s
      retries: 5
    user: "999:999"  # Non-root
    security_opt:
      - no-new-privileges:true
    read_only: true
    tmpfs:
      - /tmp
      - /var/run/postgresql

  api:
    build: ./api
    depends_on:
      - postgres
    environment:
      - DATABASE_URL=postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@postgres:5432/${POSTGRES_DATABASE}?sslmode=require
      - SECRET_KEY=${API_SECRET_KEY}
      - ENVIRONMENT=production
    # HTTPS only
    ports:
      - "8000:8000"
    volumes:
      - ./certs/api:/app/certs:ro
    command: >
      uvicorn main:app
      --host 0.0.0.0
      --port 8000
      --ssl-keyfile=/app/certs/key.pem
      --ssl-certfile=/app/certs/cert.pem
    security_opt:
      - no-new-privileges:true
    read_only: true
    tmpfs:
      - /tmp

volumes:
  postgres_data:
    driver: local
    driver_opts:
      type: none
      o: bind
      # Mount encrypted volume
      device: /mnt/encrypted/postgres_data
```

### A.2 Secure API Configuration

```python
# api/config.py
from pydantic import BaseSettings
from typing import Optional
import secrets

class Settings(BaseSettings):
    # Database
    DATABASE_URL: str
    DATABASE_POOL_SIZE: int = 20
    DATABASE_MAX_OVERFLOW: int = 40
    
    # Security
    SECRET_KEY: str = secrets.token_urlsafe(32)
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 15
    REFRESH_TOKEN_EXPIRE_DAYS: int = 7
    
    # Password Policy
    PASSWORD_MIN_LENGTH: int = 12
    PASSWORD_REQUIRE_UPPERCASE: bool = True
    PASSWORD_REQUIRE_LOWERCASE: bool = True
    PASSWORD_REQUIRE_DIGITS: bool = True
    PASSWORD_REQUIRE_SPECIAL: bool = True
    
    # Session
    SESSION_TIMEOUT_MINUTES: int = 15
    MAX_LOGIN_ATTEMPTS: int = 5
    LOCKOUT_DURATION_MINUTES: int = 30
    
    # Audit
    AUDIT_LOG_ENABLED: bool = True
    AUDIT_LOG_RETENTION_YEARS: int = 7
    
    # CORS
    CORS_ORIGINS: list = ["https://yourdomain.com"]
    
    # TLS
    TLS_ENABLED: bool = True
    TLS_CERT_PATH: str = "/app/certs/cert.pem"
    TLS_KEY_PATH: str = "/app/certs/key.pem"
    
    # Monitoring
    ENABLE_METRICS: bool = True
    SECURITY_ALERTS_EMAIL: str
    
    class Config:
        env_file = ".env.production"
        case_sensitive = True

settings = Settings()
```

### A.3 RBAC Implementation

```python
# api/rbac.py
from enum import Enum
from fastapi import Depends, HTTPException

class Role(str, Enum):
    ADMIN = "admin"
    PROVIDER = "provider"
    BILLING = "billing"
    RECEPTIONIST = "receptionist"

class Permission(str, Enum):
    VIEW_PATIENTS = "view_patients"
    EDIT_PATIENTS = "edit_patients"
    VIEW_CLINICAL = "view_clinical"
    EDIT_CLINICAL = "edit_clinical"
    VIEW_FINANCIAL = "view_financial"
    EDIT_FINANCIAL = "edit_financial"
    MANAGE_USERS = "manage_users"

ROLE_PERMISSIONS = {
    Role.ADMIN: [
        Permission.VIEW_PATIENTS,
        Permission.EDIT_PATIENTS,
        Permission.VIEW_CLINICAL,
        Permission.EDIT_CLINICAL,
        Permission.VIEW_FINANCIAL,
        Permission.EDIT_FINANCIAL,
        Permission.MANAGE_USERS,
    ],
    Role.PROVIDER: [
        Permission.VIEW_PATIENTS,
        Permission.EDIT_PATIENTS,
        Permission.VIEW_CLINICAL,
        Permission.EDIT_CLINICAL,
        Permission.VIEW_FINANCIAL,
    ],
    Role.BILLING: [
        Permission.VIEW_PATIENTS,
        Permission.VIEW_FINANCIAL,
        Permission.EDIT_FINANCIAL,
    ],
    Role.RECEPTIONIST: [
        Permission.VIEW_PATIENTS,
        Permission.EDIT_PATIENTS,
    ],
}

def require_permission(permission: Permission):
    async def permission_checker(current_user = Depends(get_current_user)):
        user_permissions = ROLE_PERMISSIONS.get(current_user.role, [])
        if permission not in user_permissions:
            raise HTTPException(
                status_code=403,
                detail=f"Permission denied. Required: {permission}"
            )
        return current_user
    return permission_checker

# Usage:
@app.get("/patients/financial")
async def get_patient_financial(
    user = Depends(require_permission(Permission.VIEW_FINANCIAL))
):
    # Only billing staff and admin can access
    pass
```

---

## Appendix B: Incident Response Plan Template

### Incident Response Procedures

#### Phase 1: Detection & Analysis (0-1 hour)
1. **Incident Detected**
   - Automated alert OR manual report
   - Log incident in tracking system
   - Assign incident number

2. **Initial Assessment**
   - What happened?
   - When did it occur?
   - What systems/data affected?
   - Is PHI involved?

3. **Classification**
   - Severity: Critical / High / Medium / Low
   - Type: Unauthorized access / Data breach / System compromise / Other
   - Scope: Number of patients affected

4. **Notification**
   - Notify Security Officer (immediate)
   - Notify Privacy Officer (if PHI involved)
   - Notify Management (if Critical/High)

#### Phase 2: Containment (1-4 hours)
1. **Short-term Containment**
   - Isolate affected systems
   - Disable compromised accounts
   - Block malicious IP addresses
   - Preserve evidence

2. **Long-term Containment**
   - Apply security patches
   - Reset credentials
   - Deploy additional monitoring

#### Phase 3: Eradication (4-24 hours)
1. **Root Cause Analysis**
   - How did the incident occur?
   - What vulnerabilities were exploited?

2. **Remove Threat**
   - Remove malware
   - Close security gaps
   - Strengthen controls

#### Phase 4: Recovery (24-72 hours)
1. **Restore Operations**
   - Restore from clean backups
   - Verify system integrity
   - Resume normal operations

2. **Monitoring**
   - Enhanced monitoring for 30 days
   - Watch for recurrence

#### Phase 5: Post-Incident (1-2 weeks)
1. **Documentation**
   - Complete incident report
   - Timeline of events
   - Actions taken
   - Lessons learned

2. **Breach Determination**
   - Was PHI accessed/acquired/disclosed?
   - Risk assessment for affected individuals
   - Breach notification determination (60 days)

3. **Improvements**
   - Update security controls
   - Update policies/procedures
   - Additional training if needed

#### Contact Information
- **Security Officer:** [Name, Phone, Email]
- **Privacy Officer:** [Name, Phone, Email]
- **IT Manager:** [Name, Phone, Email]
- **Legal Counsel:** [Name, Phone, Email]
- **Law Enforcement:** [Contact if criminal activity]
- **HHS OCR:** 1-800-368-1019 (breach reporting)

---

## Appendix C: Quick Reference Commands

### Encryption Setup
```bash
# Enable BitLocker (Windows)
Enable-BitLocker -MountPoint "C:" -EncryptionMethod XtsAes256

# Generate SSL certificates
openssl req -x509 -newkey rsa:4096 -keyout key.pem -out cert.pem -days 365

# Enable PostgreSQL TDE
psql -c "CREATE EXTENSION pgcrypto;"

# Test TLS connection
openssl s_client -connect localhost:5432 -starttls postgres
```

### Security Checks
```bash
# Check for exposed ports
netstat -tuln | grep LISTEN

# Verify TLS enabled
psql "postgresql://user@host/db?sslmode=require" -c "SHOW ssl;"

# Check failed login attempts
psql -c "SELECT * FROM audit_log WHERE action LIKE 'LOGIN_FAILED%' ORDER BY timestamp DESC LIMIT 10;"

# Check after-hours PHI access
psql -c "SELECT * FROM audit_log WHERE phi_accessed=true AND EXTRACT(HOUR FROM timestamp) NOT BETWEEN 7 AND 19;"
```

### Backup & Recovery
```bash
# Manual backup
pg_dump -U user -d dbname -F custom > backup_$(date +%Y%m%d).dump

# Restore from backup
pg_restore -U user -d dbname -c backup.dump

# Verify backup integrity
pg_restore --list backup.dump
```

### Audit & Compliance
```bash
# Run compliance check
python scripts/compliance_check.py

# Export audit logs for review
psql -c "COPY (SELECT * FROM audit_log WHERE timestamp > NOW() - INTERVAL '30 days') TO '/tmp/audit_export.csv' CSV HEADER;"

# Check user access levels
psql -c "SELECT u.username, r.role_name FROM users u JOIN user_roles ur ON u.user_id=ur.user_id JOIN roles r ON ur.role_id=r.role_id;"
```

---

## Next Steps

### Immediate Actions (This Week)
1. ✅ Review this document with your team
2. ✅ Assign Security Officer and Privacy Officer roles
3. ✅ Enable disk encryption (BitLocker) on all development machines
4. ✅ Remove public database port exposure from docker-compose.yml
5. ✅ Switch to synthetic data only for development

### Week 1-3: Critical Fixes
1. ✅ Implement database encryption (TLS/SSL)
2. ✅ Implement API authentication (JWT)
3. ✅ Implement comprehensive audit logging
4. ✅ Enable HTTPS for all services

### Week 4-7: Infrastructure
1. ✅ Set up secrets management (Azure Key Vault / AWS Secrets Manager)
2. ✅ Implement automated backups
3. ✅ Create disaster recovery plan
4. ✅ Set up security monitoring

### Week 8-13: Policies & Compliance
1. ✅ Document all security policies
2. ✅ Complete risk assessment
3. ✅ Conduct HIPAA training
4. ✅ Final compliance validation

### Production Deployment
1. ✅ Complete all items in Pre-Production Compliance Checklist
2. ✅ External security audit
3. ✅ Penetration testing
4. ✅ Legal/Compliance approval
5. ✅ Go live with real PHI

---

## Questions & Support

**For questions about this document or HIPAA compliance:**
- Contact: [Your Security Officer]
- Email: security@yourorganization.com
- Phone: [Phone number]

**External Resources:**
- HIPAA Compliance Consultant: [Contact info]
- Legal Counsel: [Contact info]
- IT Security Firm: [Contact info]

---

**Document Control:**
- **Version:** 1.0
- **Created:** October 14, 2025
- **Last Reviewed:** October 14, 2025
- **Next Review:** January 14, 2026
- **Owner:** Security Officer
- **Approved By:** [Name, Title]

---

*This document is a living document and should be updated as systems, regulations, and threats evolve.*

