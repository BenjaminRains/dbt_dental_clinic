### Agent-Friendly Portfolio Profile (Benjamin Rains)

**Name**: Benjamin Rains  
**Primary Role**: Analytics Engineer / Data Engineer (Healthcare)  
**Location**: Remote, United States  
**Email**: rains.bp@gmail.com  

---

### 1. Headline Summary

I design, build, and operate modern analytics platforms for healthcare organizations, with a focus on **OpenDental → PostgreSQL → dbt → FastAPI → React**.  
My core project is a **production analytics platform for a multi-clinic dental organization**, delivering automated, trusted KPIs across clinical, operational, and financial domains.

---

### 2. Target Roles

- Analytics Engineer  
- Data Engineer  
- Analytics Platform / Data Platform Engineer  
- Healthcare Analytics Engineer (Dental, Medical)

---

### 3. Core Technical Stack

- **Languages**: Python, SQL, TypeScript  
- **Data & Warehousing**: PostgreSQL, MariaDB/MySQL (OpenDental), dimensional modeling  
- **Transformation**: dbt Core (staging → intermediate → marts, tests, lineage)  
- **Backend**: FastAPI (REST APIs, Pydantic models, versioned endpoints)  
- **Frontend**: React, TypeScript, Material-UI, Recharts, Zustand  
- **Infrastructure & Ops**: Docker, AWS (S3, CloudFront, EC2, RDS, Lambda, EventBridge), CI/CD, Airflow  
- **ML / LLM**: OpenAI Whisper, Anthropic Claude (LLM analysis pipeline)

---

### 4. Primary System: dbt Dental Clinic Analytics Platform

**Problem**:  
The dental organization had **no centralized analytics**, relied on **manual Excel-style reporting**, and suffered from **unreliable KPIs**. Leadership could not easily see revenue leakage, collections performance, or provider productivity.

**System Built**:  
- **Metadata-driven ELT** from OpenDental (MariaDB) into PostgreSQL  
- **Incremental replication** strategies (full, incremental, incremental_chunked) with schema drift handling  
- **dbt warehouse** with clean layers:
  - Raw (replicated OpenDental sources)  
  - Staging (typed, cleaned tables)  
  - Intermediate (business logic and joins, derived entities)  
  - Marts (KPI and dimensional models ready for analytics)  
- **API-backed analytics layer** using FastAPI (versioned endpoints, documented via OpenAPI)  
- **React frontend** with dashboards and KPI cards for financial, operational, and clinical analytics

**Outcomes**:  
- Automated **daily KPI delivery** instead of manual reporting  
- Identification of **revenue leakage** and **collections opportunities**  
- Better **leadership prioritization** of collections and provider performance  
- Significant **reduction in manual reporting effort**  
- Production-grade platform with tests, monitoring, and cost controls

---

### 5. Architecture (Text Version)

**End-to-end data flow**:

1. **Source**: OpenDental MariaDB (450+ source tables)  
2. **ELT / Replication**:
   - Python-based ELT and schema discovery  
   - SQLAlchemy-powered introspection and metadata-driven extraction  
   - Incremental and backfill-safe loading logic  
   - Schema drift detection and type mapping  
3. **Warehouse (PostgreSQL)**:
   - Raw schema: replicated OpenDental tables  
   - Staging schema: typed, cleaned tables  
   - Intermediate schema: business logic, joins, derived entities  
   - Marts schema: KPI and dimensional models ready for analytics  
4. **Transformation (dbt Core)**:
   - 150+ dbt models  
   - Tests and documentation gating deployments  
   - Lineage graph showing full chain from raw sources to marts  
5. **Analytics Service (FastAPI)**:
   - REST API exposing KPIs and analytic queries  
   - Pydantic models, versioned endpoints, OpenAPI documentation  
   - Access primarily over marts schema (analytics-ready tables)  
6. **Frontend (React + TypeScript)**:
   - Dashboards for revenue, AR aging, and provider performance  
   - KPI cards, trends, and opportunity views  
   - State management using Zustand  
7. **Deployment (AWS)**:
   - CloudFront + S3: static React frontend and assets  
   - EC2 + Docker: FastAPI backend and services  
   - RDS PostgreSQL: analytics warehouse  
   - CI/CD: automated build, test, and deploy pipelines  
   - Cost optimization via EC2 scheduling (Lambda + EventBridge), region consolidation

**Parallel pipeline – Consult Audio Whisper Pipeline**:

- Input: dental consultation recordings  
- Stages:
  1. **Audio Transcription**: OpenAI Whisper → timestamped text (`.txt`, `.tsv`, `.srt`, `.vtt`)  
  2. **Transcript Cleaning**: dental-specific rules and terminology normalization  
  3. **LLM Analysis**: Anthropic Claude → summaries, treatment discussions, QA, provider coaching signals  
  4. **HTML Conversion**: markdown results rendered into responsive HTML reports  
  5. **PDF Conversion**: print-ready PDFs using ReportLab  
- Output: clinician-friendly documentation for treatment acceptance tracking and coaching; optional structured TSV/JSON for analytics and optional warehouse load.

---

### 6. Production Evidence

- **Pipelines and Warehouse**:
  - Daily scheduled ELT processing **450+ OpenDental tables**  
  - **150+ dbt models** with tests and lineage documentation  
  - Three-phase ETL: MySQL → replication → PostgreSQL raw; full / incremental / incremental_chunked strategies  
  - Schema conversion, type mapping, connection pooling, retries, and backfill-safe incremental loads  

- **Deployment and Operations**:
  - CI/CD pipelines gating deployments (tests must pass before production)  
  - Dockerized services for consistent environments  
  - FastAPI serving **versioned analytics endpoints**  
  - Deployed on AWS: S3, EC2, RDS, CloudFront  
  - Cost optimization: EC2 scheduling (Lambda + EventBridge), region consolidation, **~73% cost reduction**

---

### 7. Key Skills in Practice

- **Data Engineering**: dbt, SQL, PostgreSQL, MySQL, ELT/ETL, dimensional modeling  
- **Core Technologies**: Python, TypeScript, SQLAlchemy, FastAPI, React  
- **Analytics & Frontend**: Material-UI, Recharts, Zustand, dashboard design and KPIs  
- **Tools & Cloud**: AWS, Docker, Git, CI/CD, Airflow, environment management automation  
- **ML / LLM Integration**: Whisper transcription, Claude-based summarization and analysis, multi-format reporting

---

### 8. Links and Artifacts

- **Portfolio**: `https://dbtdentalclinic.com`  
- **GitHub Repo** (full system): `https://github.com/BenjaminRains/dbt_dental_clinic`  
- **dbt Documentation and Lineage**: `https://dbtdentalclinic.com/dbt-docs/`  
- **API Documentation (FastAPI)**: `https://api.dbtdentalclinic.com/docs`  
- **Consult Audio Pipeline Code**: `https://github.com/BenjaminRains/dbt_dental_clinic/tree/main/consult_audio_pipe`  

---

### 9. One-Sentence Classifier Summary

Benjamin Rains is an **Analytics Engineer / Data Engineer** specializing in **healthcare (dental) ELT and analytics platforms**, building **tested dbt warehouses**, **API-backed metrics services**, and **production dashboards** on **AWS (S3, CloudFront, EC2, RDS) using Python, SQL, dbt, FastAPI, and React**.

