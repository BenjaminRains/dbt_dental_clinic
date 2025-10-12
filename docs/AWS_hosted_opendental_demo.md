# OpenDental Demo on AWS — Step‑by‑Step Deployment Guide

**Goal:** Host a **public, read‑only demo** of your OpenDental analytics (dbt models + dashboards/API) that employers can visit in a browser. Keep the database private; only the web app is public.

**Region used below:** `us-east-2 (Ohio)` (matches your EC2 public DNS). Adjust if needed.

---

## 0) At‑a‑Glance Architecture

### HIPAA Compliance: Data Separation

```
┌─────────────────────────────────────────────────────┐
│  SECURE/PRIVATE ENVIRONMENT                         │
│  (On-premises or isolated secure VPC)               │
│                                                      │
│  ┌──────────────────┐                               │
│  │ OpenDental       │  ← REAL PHI DATA              │
│  │ MySQL Source     │                               │
│  └────────┬─────────┘                               │
│           │                                          │
│  ┌────────▼────────────────────┐                    │
│  │ ETL Pipeline                │                    │
│  │ (etl_pipeline/)             │                    │
│  │ - MySQL loaders             │                    │
│  │ - Data transformations      │                    │
│  │ - HIPAA-compliant processing│                    │
│  └─────────────────────────────┘                    │
│                                                      │
└─────────────────────────────────────────────────────┘
        
        ⛔ NO CONNECTION ⛔
        (Airgap between real data and demo)
        
┌─────────────────────────────────────────────────────┐
│  PUBLIC AWS DEMO ENVIRONMENT                        │
│  (EC2 + RDS in us-east-2)                           │
│                                                      │
│  ┌─────────────────────────┐                        │
│  │ Synthetic Data          │  ← FAKE DATA ONLY      │
│  │ Generator Output        │                        │
│  │ (generated CSVs)        │                        │
│  └──────────┬──────────────┘                        │
│             │                                        │
│    ┌────────▼─────────────┐                         │
│    │ RDS PostgreSQL       │                         │
│    │ - raw schema         │                         │
│    │ - No real PHI        │                         │
│    └────────┬─────────────┘                         │
│             │                                        │
│    ┌────────▼─────────────┐                         │
│    │ dbt Models           │                         │
│    │ - staging            │                         │
│    │ - marts (public)     │                         │
│    └────────┬─────────────┘                         │
│             │                                        │
│    ┌────────▼─────────────┐                         │
│    │ EC2 Web/App          │                         │
│    │ - FastAPI            │                         │
│    │ - dbt docs           │                         │
│    │ - Frontend (optional)│                         │
│    └──────────────────────┘                         │
│             │                                        │
│             ▼                                        │
│       Internet (HTTPS)                              │
│                                                      │
└─────────────────────────────────────────────────────┘
```

### AWS Demo Infrastructure

```
Internet → [HTTPS/HTTP 443/80]
            │
            ▼
       [EC2 Web/App]
         (Public Subnet)
            │  (private VPC traffic only)
            ▼
       [RDS PostgreSQL]
         (Private Subnet)
         (Synthetic data ONLY)
```

**Security:**

* EC2: Public on **80/443** (for the demo site), **22** locked to **your IP only**.
* RDS: **Private** (no public IP). Accepts traffic **only from EC2 Security Group**.
* **Data:** Synthetic/anonymized data ONLY - no PHI, no real OpenDental connection.

---

## 1) Prerequisites

* AWS account with admin access (IAM user)
* Domain (optional but recommended): e.g., `demo.mdc-analytics.com`
* Your workstation IP (for SSH allowlist)
* SSH key: `dbt-dental-clinic.pem` already secured on Windows

---

## 2) VPC & Networking (1‑time setup)

1. **Create VPC**: `10.0.0.0/16`
2. **Create Subnets** (example AZs):

   * Public A: `10.0.1.0/24` in `us-east-2a`
   * Private A: `10.0.11.0/24` in `us-east-2a`
3. **Internet Gateway (IGW):** Create + **Attach** to the VPC.
4. **Route Tables:**

   * Public RT → add route `0.0.0.0/0 → IGW` and associate **Public A**.
   * Private RT → **no** IGW route; associate **Private A**.
5. **NACLs:** Keep default (allow all) while building. You can harden later.

---

## 3) Security Groups (SG)

**SG: `sg-ec2-web`** (attach to EC2)

* Inbound:

  * **SSH** TCP 22 from **`YOUR_PUBLIC_IP/32`**
  * **HTTP** TCP 80 from `0.0.0.0/0`
  * **HTTPS** TCP 443 from `0.0.0.0/0`
* Outbound: **All traffic** (default)

**SG: `sg-rds-private`** (attach to RDS)

* Inbound:

  * **PostgreSQL** TCP 5432 from **`sg-ec2-web`** (reference the SG, not a CIDR)
* Outbound: **All traffic** (default)

> If you choose MariaDB/MySQL instead of Postgres, use TCP **3306** in the RDS SG.

---

## 4) RDS (Recommended: PostgreSQL 16) — Private Only

1. **Subnet group:** select the **Private A** subnet.
2. **Public access:** **Disabled**.
3. **Engine:** PostgreSQL 16 (db.t4g.micro or db.t3.micro; dev/test).
4. **Storage:** 20–50 GB gp3.
5. **VPC security group:** `sg-rds-private`.
6. Create initial DB: **`dental_analytics`** (or your preferred name).
7. Save credentials: `admin` / `<secure-password>`.

**Read‑only demo user (after creation):**

```sql
-- Connect as admin (psql or your client)
CREATE ROLE demo_readonly LOGIN PASSWORD '<demo-password>';
GRANT CONNECT ON DATABASE dental_analytics TO demo_readonly;
\c dental_analytics
-- Limit to a specific schema you’ll expose (e.g., marts)
CREATE SCHEMA IF NOT EXISTS marts;
GRANT USAGE ON SCHEMA marts TO demo_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA marts TO demo_readonly;
ALTER DEFAULT PRIVILEGES IN SCHEMA marts GRANT SELECT ON TABLES TO demo_readonly;
```

> Replace `marts` with your actual exposure schema. Keep PHI out of this environment.

---

## 5) EC2 Web/App Host (Amazon Linux 2023)

1. **Launch Instance**

   * AMI: **Amazon Linux 2023 x86_64**
   * Type: `t3.micro` (upgrade later to `t3.small/medium` if needed)
   * Subnet: **Public A** (auto‑assign public IP **enabled**)
   * SG: `sg-ec2-web`
   * Key pair: **dbt-dental-clinic**
   * Allocate + Associate an **Elastic IP** (keeps a stable IP)

2. **(Optional) SSM Access (no SSH needed)**

   * Create IAM role with **AmazonSSMManagedInstanceCore** and attach to EC2.

3. **Bootstrap (SSH in as `ec2-user`)**

```bash
sudo yum update -y || sudo dnf -y update
sudo dnf -y install python3 python3-pip git nginx
# (Postgres client)
sudo dnf -y install postgresql15
sudo systemctl enable --now nginx
```

---

## 6) App Deployment (FastAPI example; Streamlit also works)

> You can swap in Streamlit/Dash; the web exposure and reverse‑proxy steps are the same.

1. **App directory & venv**

```bash
sudo mkdir -p /opt/mdc_demo && sudo chown ec2-user:ec2-user /opt/mdc_demo
cd /opt/mdc_demo
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip wheel
```

2. **Clone your repo & install deps**

```bash
git clone https://github.com/BenjaminRains/dbt_dental_clinic app
cd app
pip install -r requirements.txt
# Ensure drivers:
pip install psycopg2-binary uvicorn fastapi dbt-postgres
```

> **Note:** The full repo will be cloned (including `etl_pipeline/`), but **only** the synthetic data generator and dbt models will be used. The MySQL loaders and real ETL operations will **never** run on this EC2 instance. No `.env_production` or connection credentials to real OpenDental will be deployed.

3. **Environment config**
   Create `/opt/mdc_demo/app/.env`:

```
DATABASE_URL=postgresql+psycopg2://demo_readonly:<demo-password>@<rds-endpoint>:5432/dental_analytics
APP_ENV=demo
```

4. **Simple FastAPI entrypoint** (if not already in repo) `/opt/mdc_demo/app/main.py`:

```python
from fastapi import FastAPI
import os, psycopg2
from psycopg2.extras import RealDictCursor

app = FastAPI(title="OpenDental Demo API")
DSN = os.getenv("DATABASE_URL").replace("+psycopg2","")

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/kpi/production")
def production():
    with psycopg2.connect(DSN, cursor_factory=RealDictCursor) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT date, total_production
                FROM marts.daily_production
                ORDER BY date DESC LIMIT 30;
            """)
            return {"rows": cur.fetchall()}
```

5. **Systemd service for uvicorn**

```bash
sudo tee /etc/systemd/system/mdc_demo.service > /dev/null <<'UNIT'
[Unit]
Description=MDC OpenDental Demo (FastAPI)
After=network.target

[Service]
User=ec2-user
WorkingDirectory=/opt/mdc_demo/app
EnvironmentFile=/opt/mdc_demo/app/.env
ExecStart=/opt/mdc_demo/.venv/bin/uvicorn main:app --host 0.0.0.0 --port 8000
Restart=always

[Install]
WantedBy=multi-user.target
UNIT

sudo systemctl daemon-reload
sudo systemctl enable --now mdc_demo
```

6. **Nginx reverse proxy** (HTTP → app:8000)

```bash
sudo tee /etc/nginx/conf.d/mdc_demo.conf > /dev/null <<'NGINX'
server {
    listen 80;
    server_name _;

    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }

    # Optional: serve dbt docs as static site
    location /dbt-docs/ {
        alias /var/www/dbt_docs/;
        autoindex off;
        try_files $uri $uri/ /dbt-docs/index.html;
    }
}
NGINX

sudo nginx -t && sudo systemctl reload nginx
```

> Test in browser: `http://<Elastic-IP>/health` → `{ "ok": true }`

---

## 7) dbt: Build Models + Docs

1. **dbt profile** for Postgres (on EC2 as `ec2-user`): `~/.dbt/profiles.yml`

```yaml
mdc_demo:
  target: prod
  outputs:
    prod:
      type: postgres
      host: <rds-endpoint>
      user: admin
      password: <admin-password>
      dbname: dental_analytics
      schema: marts
      port: 5432
      threads: 4
      keepalives_idle: 0
```

2. **Run dbt**

```bash
cd /opt/mdc_demo/app
source ../.venv/bin/activate
dbt deps
dbt build --select marts

# Generate docs
mkdir -p /var/www/dbt_docs
dbt docs generate
cp -r target/* /var/www/dbt_docs/
```

Visit: `http://<Elastic-IP>/dbt-docs/`

> For public docs, ensure the marts contain **only demo/anonymized** data.

---

## 8) Seeding Demo Data (No PHI)

**CRITICAL:** The demo EC2 environment must **NEVER** connect to real OpenDental data (HIPAA compliance). All data must be synthetic.

### Data Separation Strategy

1. **Secure Environment (not on demo EC2):**
   - Real OpenDental MySQL database (PHI)
   - ETL pipeline (`etl_pipeline/`) processes real data
   - Stays on-premises or in isolated secure VPC

2. **Demo Environment (public EC2):**
   - Uses `synthetic_data_generator/` output ONLY
   - No connection to real OpenDental
   - All data is fake/anonymized

### Synthetic Data Deployment

**Option A: Pre-generate locally, upload CSVs**

```bash
# On your local workstation (Windows):
cd etl_pipeline
pipenv run python synthetic_data_generator/run_generator.py --output-dir output_demo

# Upload to EC2
scp -i "C:\Users\rains\.ssh\dbt-dental-clinic.pem" -r output_demo/*.csv ec2-user@<Elastic-IP>:/opt/mdc_demo/data/
```

**Option B: Run generator on EC2 (no PHI involved)**

```bash
# On EC2
cd /opt/mdc_demo/app/etl_pipeline
source ../../.venv/bin/activate
python synthetic_data_generator/run_generator.py --output-dir /opt/mdc_demo/data
```

### Load Synthetic Data to RDS

```bash
# On EC2, load CSVs to staging schema
psql -h <rds-endpoint> -U admin -d dental_analytics <<'SQL'
CREATE SCHEMA IF NOT EXISTS raw;

-- Create tables matching your synthetic generator output
\copy raw.patient FROM '/opt/mdc_demo/data/patient.csv' CSV HEADER;
\copy raw.appointment FROM '/opt/mdc_demo/data/appointment.csv' CSV HEADER;
\copy raw.procedurelog FROM '/opt/mdc_demo/data/procedurelog.csv' CSV HEADER;
-- ... etc for all generated tables

SQL
```

Then dbt models transform `raw.*` → `marts.*` for the demo API.

**Label prominently:** Add "DEMO DATA - NOT REAL PATIENTS" watermarks in UI/docs.

---

## 9) Optional: Custom Domain + TLS

1. Create an **A record** (Route 53 or Cloudflare) → point `demo.mdc-analytics.com` to the EC2 **Elastic IP**.
2. Install **certbot** for HTTPS:

```bash
sudo dnf -y install certbot python3-certbot-nginx
sudo certbot --nginx -d demo.mdc-analytics.com --agree-tos -m you@example.com --redirect
```

Auto‑renews via systemd timers.

---

## 10) Observability & Ops

* **Logs:**

  * App: `journalctl -u mdc_demo -f`
  * Nginx: `/var/log/nginx/access.log` & `error.log`
* **CloudWatch (optional):** Install `amazon-cloudwatch-agent` to ship logs/metrics
* **Backups:** Enable RDS automated backups (snapshots)
* **Budgets:** Create an AWS Budget alert (email/SMS) to cap surprises
* **Updates:** Patch monthly (`dnf update`) and rotate app deps

---

## 11) Hardening (after it works)

* Restrict SSH to your IP; consider disabling password auth entirely (key‑only)
* Consider **SSM Session Manager** instead of SSH
* Principle of least privilege on IAM roles/policies
* Tighten NACLs if necessary (allow ephemeral ports 1024–65535)

---

## 12) Smoke Tests (Share‑Ready)

* `http://<Elastic-IP>/health` returns `{ok:true}`
* `http://<Elastic-IP>/dbt-docs/` loads the dbt docs site
* KPI endpoint returns JSON rows (e.g., `/kpi/production`)
* RDS has **no public endpoint**; only EC2 SG can reach 5432
* Demo user `demo_readonly` has **SELECT‑only** permissions

---

## 13) Handy Commands (Windows/PowerShell)

```powershell
# Connect via SSH
ssh -i "C:\Users\rains\.ssh\dbt-dental-clinic.pem" ec2-user@<Elastic-IP>

# Copy .env up (example)
scp -i "C:\Users\rains\.ssh\dbt-dental-clinic.pem" .env ec2-user@<Elastic-IP>:/opt/mdc_demo/app/.env

# Quick port test
Test-NetConnection <Elastic-IP> -Port 80
```

---

## 14) Cleanup / Cost Control (when paused)

* Stop EC2 instance when not needed (EBS persists).
* Consider `t3.small` only if CPU/RAM is tight.
* RDS: turn off Multi‑AZ for demos; use smaller instance.
* Delete unused Elastic IPs and snapshots.
 
---

## 15) Quick Checklist

* [ ] VPC + Subnets (Public A, Private A)
* [ ] IGW attached + public route `0.0.0.0/0 → IGW`
* [ ] SGs: `sg-ec2-web` (22/80/443), `sg-rds-private` (5432 from `sg-ec2-web`)
* [ ] RDS Postgres created (private), DB + read‑only user
* [ ] EC2 launched, Elastic IP, Nginx + app running
* [ ] dbt build, docs generated and hosted under `/dbt-docs/`
* [ ] Domain + TLS (optional) working
* [ ] Smoke tests pass; share the URL with employers

---

### Notes & Variations

* **MariaDB path:** Swap Postgres client/driver for MariaDB (`mariadb` client, `pymysql`/`mysqlclient`, port 3306). Adjust dbt adapter to `dbt-mysql`/`dbt-mariadb` if desired.
* **Frontend React UI:** Build with Vite, serve via Nginx at `/` and proxy API under `/api/` to uvicorn/gunicorn.
* **Readonly SQL access for reviewers:** Optionally publish a **temporary IP allowlist** on a **separate** RDS instance with a dummy dataset; rotate passwords frequently.
* **Repo security:** Never commit `.env_production`, `.env_test`, or any real database credentials to git. The demo EC2 only needs `.env` for the demo RDS connection (synthetic data). Consider using AWS Secrets Manager for production credentials in your secure environment.

---

**You’re ready to implement.** Start with VPC/SGs → RDS → EC2 → App → dbt → Docs → Domain/TLS. When you hit a step, we can fill in exact commands/screenshots for your current console state.
