# 🦷 Frontend Roadmap for Dental Analytics Platform

This document outlines a phased plan for delivering a usable frontend experience for non-technical
 dental clinic staff and the business owner. It builds on the existing dbt + PostgreSQL backend and
  Tableau dashboards.

---

## ✅ Phase 0: Pre-Frontend Prep (You Are Here)

- [x] Finish dbt MART models
- [x] Implement dbt tests, documentation, and freshness monitoring
- [x] Build core Tableau dashboards
- [ ] Create an internal glossary (dbt docs export, Markdown, or Notion)

---

## 🧩 Phase 1: Minimum Viable Frontend (Internal Use)

**Goal:** Embed key Tableau dashboards in a simple web app and control access by user role.

### 🔧 Tech Stack
- **Backend:** FastAPI or Flask
- **Frontend:** HTMX + Tailwind CSS (or React if preferred)
- **Auth (Optional):** Basic login, per-role permissions
- **BI Layer:** Tableau Cloud or Tableau Server with embedded dashboards

### 🔍 Features
- Dashboard navigation menu
- Embedded Tableau iframe views (read-only)
- KPI snapshot cards (optional: use Tableau REST API to extract key stats)
- "Export to CSV" and "Export to PDF" buttons
- Custom 404 & "No Access" pages

### 📁 Routes Example
| Route                     | Role              | Description                       |
|--------------------------|-------------------|-----------------------------------|
| `/`                      | All               | Landing page                      |
| `/dashboards/production`| Dentist, Manager   | Production metrics                |
| `/dashboards/ar`        | Manager            | Accounts receivable & claims      |
| `/dashboards/patient`   | Hygienist, Dentist | Patient treatment progress        |

---

## 🚀 Phase 2: Scheduling & Alerts

**Goal:** Provide proactive insights, not just passive dashboards.

### 🔔 Features
- Daily/weekly automated email reports from Tableau
- “KPI pulse” section with freshness indicators:
  - Example: “🟡 3 claims over 30 days pending submission”
- Conditional formatting on KPIs for quick review

### 🧰 Tooling
- Use Tableau subscriptions or Python (via Tableau REST API) to auto-email dashboards
- Backend cron jobs or Airflow DAGs for alert logic

---

## 🧠 Phase 3: Interactivity & Q&A

**Goal:** Improve usability and add light interactivity without complexity.

### 🧭 Features
- Searchable metric glossary
- Tooltip explanations for KPIs
- Simple filters: provider, date range, insurance
- “Explain this KPI” or “Drill down” buttons

### Optional: Natural Language Interface
- Basic chatbot using LangChain or OpenAI RAG pipeline over warehouse schema
- Let users type questions like: "How much did we collect this week from Delta Dental?"

---

## 🧪 Phase 4: Feedback & Personalization

**Goal:** Tune platform based on real user input and daily clinic workflows.

### 📢 Actions
- Add in-app feedback buttons
- Track dashboard usage analytics
- Monthly stakeholder check-ins to prioritize features

### Future Features
- Patient-level insights with drill-down charts
- KPI benchmarking across months or years
- Goal tracking (e.g., treatment acceptance rate target = 80%)

---

## 📋 Appendix: Tableau Integration Notes

- Tableau Cloud supports **Javascript API** and **Trusted Auth** for seamless iframe embedding
- Tableau REST API can:
  - Query view data for summary metrics
  - Download dashboards as images or PDFs
  - Manage users and permissions programmatically

---

## 🔒 Security & Compliance

- Use HTTPS + basic auth or OAuth2
- Enforce read-only access to dashboards
- Mask or restrict PHI/PII in views shown externally
- Log access to sensitive dashboards

---

## 📅 Suggested Timeline

| Week | Focus Area                      |
|------|---------------------------------|
| 1    | Build FastAPI + HTMX frontend shell |
| 2    | Embed dashboards, build navigation |
| 3    | Add email alerts and role access |
| 4    | Launch MVP to internal users     |
| 5+   | Collect feedback, improve UX, iterate |

---

