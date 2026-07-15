# Portfolio Site Overhaul Plan

**Objective:** Reposition https://dbtdentalclinic.com from "nice project with dashboards" to **production-grade analytics system powering real operations**.

**Target file:** `frontend/src/pages/Portfolio_v3.tsx` (implemented from scratch per this plan; `Portfolio_v2.tsx` retained for reference)

**Evidence sources (used in frontend sections):**
- `docs/career/platform_projects/` — portfolio_feature_roadmap.md (Cost Optimization complete, 73% cost reduction, EC2 scheduling Lambda+EventBridge, KPI Definitions complete), entity_mapping_proposal.md (cross-system mapping design)
- `docs/career/architecture_case_studies/` — analytics_platform_architecture_analysis.md (three-phase ETL, full/incremental/incremental_chunked strategies, staging→intermediate→marts, schema conversion, connection pooling/retry), fastapi_pydantic_service_architecture.md (Pydantic models, service layer, routers, API contracts)

**Core shift:**  
*Old signal:* "Here are dashboards I built."  
*New signal:* "Here is a production analytics system I architected and operate."

---

## 1. New Page Order (Structure)

Implement sections in this order from top to bottom:

| Order | Section | Notes |
|-------|---------|--------|
| 1 | **Hero** | Updated headline + subhead (see §8) |
| 2 | **Why This Is Production-Grade** | New callout box (§6) |
| 3 | **Platform Architecture** | Diagram + copy + data flow example (§1, §4) |
| 4 | **Production Evidence** | Replace "Visual Examples" emphasis (§2) |
| 5 | **dbt Lineage** | Keep lineage screenshot/link, position after evidence |
| 6 | **Synthetic Dashboards** | 2–3 key examples, clearly labeled (§3) |
| 7 | **GitHub / Code Access** | Component-based links, not single button (§7) |
| 8 | **Project cards** | Problem → System → Outcome framing (§5) |
| 9 | **Documentation & Resources** | Keep, adjust links if needed |
| 10 | **Consult Audio Pipeline** | Keep; optionally reframe with Problem → System → Outcome |
| 11 | **Supporting Tools** | Keep |
| 12 | **Skills & Tech Stack** | Keep |
| 13 | **Contact / Footer** | Keep |

---

## 2. Hero Section (§8)

**Current:** Generic "Benjamin Rains", "Data Engineer – Healthcare Analytics & Modern ELT", and a short intro paragraph.

**Change:**

- **Headline (H1):**  
  `Production Analytics Platform Powering Daily Operations for a Multi-Clinic Dental Organization`
- **Subhead:**  
  `Automated ELT, a tested dbt warehouse, and API-driven dashboards delivering trusted clinical, operational, and financial KPIs.`
- Keep: name, role, location, email, LinkedIn, and CTA buttons (View Analytics Demo, GitHub, Resume).
- Optionally shorten or remove the long paragraph that starts with "I build end-to-end analytics platforms..." in favor of the new subhead so the hero stays scannable.

---

## 3. "Why This Is Production-Grade" Callout (§6)

**Place:** Directly below the hero, above the architecture diagram.

**Format:** Small boxed section (e.g. `Paper` or `Card` with subtle background).

**Copy (bullet list):**

- Automated scheduling  
- Monitoring and logging  
- Schema drift handling  
- Backfill support  
- Tests gate deployments  
- Version-controlled models  
- CI/CD pipelines  
- Containerized services  
- Cloud deployment  

**Optional one-liner:**  
"Pipelines are idempotent and support reprocessing, backfills, and partial re-runs without full reloads."

---

## 4. Architecture Section – Move to Top (§1)

**Place:** Immediately after "Why This Is Production-Grade".

**Section title:**  
`Platform Architecture (Production System)`

**Intro copy:**  
"End-to-end architecture showing how raw OpenDental data flows through automated ELT, a tested dbt warehouse, and API-driven services to power analytics applications."

**Under the diagram, list concrete behaviors:**

- Metadata-driven MySQL → PostgreSQL replication  
- Incremental + backfill-safe ELT  
- dbt staging → intermediate → marts with tests  
- FastAPI analytics service  
- React + TypeScript dashboards  
- CI/CD + Docker + AWS deployment  

**Production characteristics (annotate diagram or list as callouts):**  
Incremental Loads • Schema Drift Handling • Idempotent Pipelines • Data Quality Tests • Versioned Models • API Contracts • Containerized Services • Environment Separation (dev / staging / prod)

**Implementation:**  
- Move the existing Mermaid "System Architecture Diagram" card from its current position (after Visual Examples) to this new position.  
- Update the Mermaid chart text if needed to align with the bullets (e.g. add short labels for production characteristics).  
- Add the intro paragraph and the bullet list below the diagram.

---

## 5. Concrete Data Flow Example (§4)

**Place:** Directly below the architecture diagram (same section).

**Title:** e.g. "Example: From Source to Dashboard"

**Copy (single flow):**  
`OpenDental Procedures → stg_procedure → int_patient_procedure → mart_revenue → FastAPI /revenue-opportunities → Revenue Opportunity Dashboard`

**Format:** One line or a small horizontal flow (text or minimal diagram) so it reads as one tangible path.

---

## 6. Production Evidence Section (§2)

**Replace:** The current "Visual Examples" framing as the main proof.

**Section title:**  
`Production Evidence (Redacted)`

**Copy (bullets):**

- Daily scheduled ELT pipelines processing 450+ OpenDental tables  
- 150+ dbt models with full test coverage  
- CI/CD pipelines gating deployments  
- Dockerized services  
- FastAPI serving versioned analytics endpoints  
- Deployed on AWS (S3, EC2, RDS, CloudFront)  

**Where possible, include artifacts (links or screenshots):**

- dbt docs lineage (existing link/screenshot)  
- dbt test run output (screenshot or link)  
- CI pipeline logs (redacted screenshot)  
- OpenAPI documentation (link to api.dbtdentalclinic.com/docs)  
- Repository structure (screenshot)  

**Implementation:**  
- Add a new "Production Evidence" section after Architecture.  
- Use a grid of cards or a list with optional thumbnail/screenshot placeholders.  
- Link to existing dbt-docs, API docs, and GitHub; add placeholder or real assets for "dbt test run" and "CI pipeline" if available.

---

## 7. dbt Lineage

**Place:** After Production Evidence.

- Keep the existing "dbt Lineage Graph" card (dim_patient DAG image + "View dbt Lineage Graph" button).  
- Optionally add a short line: "Interactive dbt lineage showing how 150+ models connect raw OpenDental sources to business-ready marts and KPIs."  
- Ensure the link points to https://dbtdentalclinic.com/dbt-docs/ (or current URL).

---

## 8. Synthetic Dashboards (§3)

**Replace:** Current "Visual Examples" that imply real production data.

**Section title:**  
`Synthetic Production Mockups` or `Dashboard Examples (Synthetic Data)`

**Disclosure (required):**  
"Synthetic data. Layout and metrics match production system."

**Limit to 2–3 key examples:**

1. Revenue & Collections  
2. AR Aging  
3. Provider Performance (or Revenue Opportunity – pick 3 total)

**Implementation:**  
- Keep existing dashboard image cards (e.g. AR Aging, Revenue Trend, Revenue Opportunity) but:  
  - Add a clear banner or caption on each: "Synthetic data. Layout and metrics match production system."  
  - Remove or consolidate so only 2–3 dashboards are shown.  
- Use the same images if they are already synthetic; if not, replace with synthetic mockups and keep layout consistent.

---

## 9. GitHub / Code Access (§7)

**Current:** Single "View on GitHub" and inline "Project Components" links.

**Change:** Elevate GitHub to a "Case Study" style – break into logical components:

- **ETL Replicator** → link to `etl_pipeline` (e.g. tree/main/etl_pipeline)  
- **dbt Project** → link to dbt project root or dbt-docs  
- **FastAPI Service** → link to API repo or api/ folder and api.dbtdentalclinic.com/docs  
- **Frontend Application** → link to frontend repo or tree/main/frontend  

**Copy for each:** One line describing what it is; e.g. "This repository powers the production dental analytics platform demonstrated in this portfolio." can be used in README; on the site use short labels like "Metadata-driven ELT, incremental loads, 450+ tables."

**Implementation:**  
- Replace or augment the single GitHub button in the hero with a "Code & Repositories" section.  
- Reuse or refine the existing "Project Components" grid (Analytics Dashboard, dbt Models, ELT Pipeline, FastAPI Backend, etc.) so each has a clear "Repository" or "Docs" link and outcome-focused description.

---

## 10. Project Cards: Problem → System → Outcome (§5)

**Apply to:** The main "dbt Dental Clinic Analytics Platform" block (the four cards: Data Infrastructure, dbt Transformations, Analytics Frontend, Production Deployment).

**New framing (one approach):**

- **Problem**  
  Clinic had no centralized analytics, manual reporting, and unreliable KPIs.  
- **System Built**  
  Metadata-driven ELT • Incremental replication • dbt warehouse • API-backed analytics layer • React frontend  
- **Outcome**  
  Automated daily KPI delivery • Identified revenue leakage • Enabled leadership prioritization of collections • Reduced manual reporting effort  

**Implementation:**  
- Add a short "Problem" paragraph at the top of the platform section.  
- Add a "System Built" list or chips (can reuse existing card content).  
- Add an "Outcome" bullet list or paragraph.  
- Optionally keep the four cards but reframe their text to align with Problem/System/Outcome (e.g. Data Infrastructure = part of "System Built", with one line on outcome where relevant).

---

## 11. Optional: 5-Minute Walkthrough Video (§9)

- Add a section or card: "5-Minute Platform Walkthrough" with a placeholder or embed (YouTube/Vimeo).  
- Suggested content: Architecture overview → dbt lineage → Repository structure → API docs → Synthetic dashboards.  
- Can be implemented in a later phase; leave a placeholder or "Coming soon" if not ready.

---

## 12. Copy and Messaging Checklist

- [ ] Hero: New headline + subhead (§8)  
- [ ] "Why This Is Production-Grade" box added (§6)  
- [ ] Architecture moved to top; title + intro + behaviors + production characteristics (§1)  
- [ ] Data flow example added under architecture (§4)  
- [ ] "Production Evidence" section added; artifacts linked or placeholders (§2)  
- [ ] dbt Lineage kept after Production Evidence  
- [ ] Dashboards section renamed; synthetic disclaimer on each; only 2–3 examples (§3)  
- [ ] GitHub presented as component-based "Case Study" (§7)  
- [ ] Main project block reframed: Problem → System → Outcome (§5)  
- [ ] Optional: Video placeholder (§9)  

---

## 13. File and Component Structure (Recommendations)

- **Single page:** All of the above can live in `Portfolio_v2.tsx` with sections in the order above.  
- **Optional:** Extract sections into components for readability, e.g.  
  - `PortfolioHero.tsx`  
  - `ProductionGradeCallout.tsx`  
  - `PlatformArchitectureSection.tsx` (diagram + data flow example)  
  - `ProductionEvidenceSection.tsx`  
  - `SyntheticDashboardsSection.tsx`  
  - `GitHubCodeAccessSection.tsx`  
  - `ProjectOutcomeCards.tsx`  

- **Assets:**  
  - Ensure `/ar_aging.png`, `/rev_trend.png`, `/revenue_opp.png`, `/dim_patient_DAG.png` are present in `public/` or equivalent.  
  - If adding new screenshots (dbt test, CI, repo structure), add to `public/` and reference in Production Evidence.

---

## 14. Summary

| Priority | Task |
|----------|------|
| P0 | New hero headline + subhead |
| P0 | "Why This Is Production-Grade" callout below hero |
| P0 | Move architecture diagram to top; add section title, intro, behaviors, production characteristics |
| P0 | Add concrete data flow example under architecture |
| P0 | Add "Production Evidence" section with bullets + artifact links/placeholders |
| P0 | Rename and limit dashboards; add synthetic data disclaimer |
| P0 | Reframe main project block: Problem → System → Outcome |
| P1 | GitHub as component-based (ETL, dbt, FastAPI, Frontend) |
| P1 | Order sections as in §1 (Evidence → Lineage → Dashboards → GitHub → Project cards) |
| P2 | Optional walkthrough video placeholder |
| P2 | Extract sections into components if desired |

The substance (architecture, metrics, links) already exists; the overhaul is **presentation and order** to foreground operational credibility over visuals.

---

## 15. Evidence Sources (docs/career) — Used in Frontend

Content for **Why This Is Production-Grade** and **Production Evidence** is drawn from:

### docs/career/platform_projects/

| File | Evidence used in frontend |
|------|---------------------------|
| **portfolio_feature_roadmap.md** | Cost Optimization complete; 73% cost reduction ($128→$35/month); EC2 scheduling via Lambda + EventBridge; KPI Definitions component complete; interview narratives for ELT, dbt, API. |
| **entity_mapping_proposal.md** | Cross-system mapping design (customers→patients, invoices→procedures, payments→collections); confidence scoring; multi-strategy matching—referenced as planned/design depth. |

### docs/career/architecture_case_studies/

| File | Evidence used in frontend |
|------|---------------------------|
| **analytics_platform_architecture_analysis.md** | Three-phase ETL (Extract MySQL→replication, Load replication→PostgreSQL raw, Transform dbt); extraction strategies (full_table, incremental, incremental_chunked); schema conversion and type mapping; staging → intermediate → marts; connection pooling, retry, backfill-safe incremental loads. |
| **fastapi_pydantic_service_architecture.md** | FastAPI three-layer architecture (Pydantic models, service layer, routers); versioned analytics endpoints; API contracts—referenced in Production Evidence “FastAPI serving versioned analytics endpoints.” |

When adding or updating Production Evidence bullets or artifact links, prefer pulling concrete details from these docs to keep the portfolio aligned with the same narrative.
