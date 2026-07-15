# Frontend Evolution Proposal

## Moving from Analytics Platform to Operational Decision Platform

> Status: **Active — Phase 0 complete; Phase 1 in progress (validated-KPI gate)**
> Last updated: **2026-07-15**
> Audience: Practice Owner, Practice Manager, Insurance Specialist, Platform Maintainer
> Related: [FRONTEND_SPLIT_PLAN.md](./FRONTEND_SPLIT_PLAN.md), [frontend/README.md](../../frontend/README.md), [OFFICE_KNOWLEDGE_ANALYTICS_PROPOSAL.md](./OFFICE_KNOWLEDGE_ANALYTICS_PROPOSAL.md), [api/](../../api/), [powerbi/README.md](../../powerbi/README.md)

---

## Progress log

| Date | Milestone |
|------|-----------|
| 2026-06-16 | Initial proposal drafted — role homes, work queues, exception framing, phased roadmap |
| 2026-03 / earlier | Clinic vs demo build split shipped: `VITE_IS_DEMO` routes `/` to `ClinicHome` (clinic) or `Portfolio_v3` (demo); [Layout.tsx](../../frontend/src/components/layout/Layout.tsx) uses separate sidebar labels and titles |
| 2026-06-18 | **Product direction refined:** clinic UX should prioritize **daily/weekly call lists and patient weighting**, not financial dashboards and charts. Six operational questions identified (see below). |
| 2026-06-18 | [FRONTEND_SPLIT_PLAN.md](./FRONTEND_SPLIT_PLAN.md) drafted — portfolio archive under `frontend/archive/portfolio/`; long-term split into two deployable apps (orthogonal to evolution, same clinic product goals) |
| 2026-06-18 | **Phase 1 implementation not yet merged** — shared components (`WorkQueue`, `InsightCard`, `useApiQuery`, `RoleContext`, role homes) remain design targets only |
| 2026-06-26 | **Validated-KPI gate:** business users see only OD-reconciled metrics on role homes. Daily net collections live on Practice Manager Home (`validatedKpis.ts`). Next validation batch: AR total, total production, total collections — then promote to Owner / PM / Insurance homes. |
| 2026-07-15 | **Insurance Specialist workstream added:** build out `/home/insurance` beyond placeholder; author clinic SOPs (starting with paper EOB batch payment posting) and surface them on the Insurance Home. Plan only — no code yet. See [Insurance Specialist Home + SOPs](#insurance-specialist-home--sops). |

### Phase checklist (clinic build)

| Phase | Goal | Status |
|-------|------|--------|
| **0 — Foundation** | Clinic/demo routing, analytics pages, API client, backend queues | **Done** |
| **1 — Role homes** | Replace `ClinicHome` placeholder; validated KPIs + work queues | **In progress** — PM home has daily net collections; other homes placeholder |
| **1A — KPI validation** | Reconcile next metrics vs OD before home promotion | **Next:** AR total, total production, total collections |
| **1B — Insurance Home + SOPs** | Insurance Specialist landing page + clinic SOP library (paper EOB batch first) | **Planned** — not started |
| **2 — Patient-level queues** | Call lists: overdue, unfinished treatment, implants, reactivation | **Backend partial** (see queue inventory) |
| **3 — Narrative briefs** | Morning brief templates on role homes | **Not started** |
| **4 — Guided investigation** | Insight → drill-down without SQL | **Not started** |
| **5 — Conversational layer** | NL enhancement after structured workflows work | **Deferred** |

### Operational questions → queue mapping (2026-06-18)

Staff care less about “what do the numbers say?” and more about **“who should we call?”** The clinic product should answer:

| Question | Intended queue | Warehouse / API today | Patient-level call list? |
|----------|----------------|----------------------|--------------------------|
| Who should we call? | **Master prioritized queue** (deduped, weighted) | Partial — AR + revenue recovery only | No |
| Which patients have unfinished treatment? | Active / in-progress treatment plans | `int_treatment_plan` (`is_in_progress`, `remaining_amount`, `timeline_status`) | No API |
| Which patients are overdue? | Recall / hygiene overdue | `int_recall_management`; hygiene service computes overdue **percent** only | No patient list endpoint |
| Show me implant opportunities | Extraction + bone graft, no D6010 | `mart_untapped_implant_revenue_detail` (names, balance, contact prefs) | **dbt only — no API** |
| Show me people with money left on the table | Unscheduled accepted treatment $ | Revenue recovery “Treatment Plan Delay”; plan `remaining_amount` | Partial (mixed into revenue recovery) |
| Show me who hasn’t been back | Overdue recall + no future appt; inactive | Hygiene overdue logic (excludes scheduled + seen in 6 mo) | Aggregate only |

**Assumptions to confirm with the office before building queues:** active-patient filter, exclude patients with future appointments, contact preference / consent, recall disabled flags, minimum dollar thresholds, delay buckets (30 / 90 / 180 days), and whether collections (AR) stay separate from scheduling outreach.

---

## Executive Summary

The MDC Analytics platform has matured significantly.

The project now contains:

- Extensive dbt modeling (~150 models)
- Production, collections, and AR metrics
- Insurance claim validation logic (warehouse-side)
- Patient segmentation
- Treatment journey analysis
- Scheduling analytics
- Data quality testing
- Stakeholder investigation reports
- API infrastructure
- Dashboard infrastructure

The primary challenge is no longer data acquisition or metric creation.

The primary challenge is delivering the right information to the right person at the right time.

This proposal recommends shifting development focus away from building additional metric pages and toward improving how analytics are consumed by office staff.

The goal is to transform the platform from a collection of dashboards into an **operational decision platform**.

---

## Current State

### User experience today

Today's experience resembles a traditional BI system.

```text
User
 ↓
Dashboard
 ↓
Charts
 ↓
Interpretation
 ↓
Action
```

The burden of interpretation falls on the user.

Office staff must:

- Locate the correct dashboard
- Understand the metric
- Determine whether action is required
- Decide what to do next

This approach works for analysts but is less effective for operational staff.

### Codebase audit (clinic build) — as of 2026-06-18

The clinic frontend lives in [frontend/](../../frontend/). The clinic site (`clinic.dbtdentalclinic.com`) is built with `VITE_IS_DEMO=false`. Demo and portfolio pages (`Portfolio_v3`, `AgentProfile`, etc.) share the same codebase but are not part of the clinic experience.

| Area | Current state | Implication |
|------|---------------|-------------|
| **Clinic home** | [ClinicHome.tsx](../../frontend/src/pages/ClinicHome.tsx) is still a **placeholder** (“Clinic Analytics” card → use sidebar) | Users land on a stub; `/dashboard` holds the real summary today |
| **Build routing** | [App.tsx](../../frontend/src/App.tsx): `VITE_IS_DEMO=true` → `Portfolio_v3` at `/`; else `ClinicHome` in `Layout` at `/` | Clinic and demo deploy from one repo; correct host separation at build time |
| **Navigation** | [Layout.tsx](../../frontend/src/components/layout/Layout.tsx) — flat sidebar by **metric domain** (Home, Dashboard, Revenue, AR, …); clinic title “MDC & GLIC Analytics” | Classic BI nav; no Home · Queues · Reports grouping yet |
| **Authentication** | API key via `VITE_API_KEY`; no user login or role selection | All users see the same pages; role picker or portal login still TBD |
| **Pages shipped** | 10 analytics routes + `ClinicHome` | Metric coverage is solid; **no role homes, no queue-first pages** |
| **API layer** | [api.ts](../../frontend/src/services/api.ts) — typed client (revenue, ar, appointments, hygiene, treatment acceptance, …) | Ready to compose call lists; **no insurance router**, **no implant / reactivation queue endpoints** |
| **Reusable components** | `KPICard`, `RevenueTrendChart`, `HygieneKPICard`, `MetricsTable`, `InfoTooltip` | Chart/metric patterns exist; **`WorkQueue`, `InsightCard`, `useApiQuery`, `RoleContext` not yet in tree** |
| **State management** | Per-page `useState` / `useEffect`; Zustand in deps but unused | Duplicated loading/error/filter logic — extract still pending |
| **Tests** | None in `frontend/` | Refactors carry higher risk without a baseline |
| **Portfolio cleanup** | Legacy pages archived under [frontend/archive/portfolio/](../../frontend/archive/portfolio/) | Reduces confusion; live demo still imports `Portfolio_v3` from `src/pages/` |

### What already works (and is underused)

Several backend capabilities already support operational workflows, but they are buried inside metric dashboards:

| Existing capability | Where it lives | Operational potential |
|--------------------|----------------|----------------------|
| **AR Priority Queue** | [AR.tsx](../frontend/src/pages/AR.tsx) + `/ar/priority-queue` | Collections work list — patient, balance, priority score, aging |
| **Revenue Opportunities** | [Revenue.tsx](../frontend/src/pages/Revenue.tsx) + `/revenue/opportunities` | Missed appointments, claim rejections, treatment delays |
| **Recovery Plan** | Revenue page + `/revenue/recovery-plan` | Includes `recommended_actions[]` per opportunity — closest thing to actionable guidance today |
| **Today's / Upcoming Appointments** | [Appointments.tsx](../../frontend/src/pages/Appointments.tsx) | Scheduling lists exist but are not framed as a confirmation or fill-the-schedule queue |
| **Top Patient Balances** | Dashboard + `/patients/top-balances` | High-value AR exceptions, not a full reactivation queue |
| **Untapped implant revenue** | `mart_untapped_implant_revenue_detail` (dbt) | Patient-level cohort with name, balance, contact prefs — **not exposed via API or UI** |
| **Treatment plan progress** | `int_treatment_plan` (dbt) | `is_in_progress`, `remaining_amount`, delay buckets — **aggregate treatment acceptance API only** |
| **KPI Definitions** | [KPIDefinitions.tsx](../../frontend/src/pages/KPIDefinitions.tsx) — static, sourced from dbt exposures | Excellent for analysts; too deep for daily operational staff |

### Structural gaps

1. **ClinicHome is empty** while `/dashboard` holds the real executive summary — users land on a stub, then navigate elsewhere.
2. **No role concept** — Practice Manager, Insurance Specialist, Front Desk, and Owner all share one sidebar.
3. **Queues are page features, not first-class objects** — priority tables sit below charts and filters; users must know which page to open.
4. **No unified “who to call” view** — collections and revenue recovery exist separately; no master weighted list, no patient names on queue rows in proposed UI.
5. **No exception framing** — KPI cards show numbers; they rarely say "this is unusual" or "action recommended."
6. **No narrative layer** — no morning brief, weekly summary, or plain-language interpretation.
7. **Insurance is absent from the frontend** — warehouse has claim validation logic, but there is no insurance route, page, or queue. This aligns with [OFFICE_KNOWLEDGE_ANALYTICS_PROPOSAL.md](./OFFICE_KNOWLEDGE_ANALYTICS_PROPOSAL.md): many insurance questions require external knowledge, not just marts.
8. **Page-level duplication** — each page reimplements loading spinners, date filters, `TabPanel`, currency formatting, and error alerts independently.
9. **Metric pages are the default workflow** — staff must interpret charts; product goal is **call lists first, reports second** (sidebar “Reports” group for drill-down only).

---

## Desired State

The platform should increasingly answer:

> **What needs attention today?**

instead of:

> **What do the numbers say?**

Future workflow:

```text
User
 ↓
Role Home
 ↓
Prioritized Insights
 ↓
Recommended Actions
 ↓
Operational Work Queue
```

The platform becomes a decision support system rather than a reporting system.

Metric pages (Revenue, AR, etc.) remain available as **drill-down destinations**, not as the primary entry point.

---

## Design Principle #0

### Validated KPIs Only on Role Homes

Business users should not interpret unvalidated warehouse numbers on their daily landing page.

**Rule:** A metric appears on a role home only after it reaches `within_tolerance` in [KPI_VALIDATION_REGISTRY.md](../../dbt_dental_models/validation/kpi/KPI_VALIDATION_REGISTRY.md) (OpenDental report benchmark). Until then it remains on legacy **Reports** routes (`/dashboard`, `/revenue`, `/ar-aging`, …).

**Frontend contract:** [validatedKpis.ts](../../frontend/src/config/validatedKpis.ts) is the allowlist. Each entry includes mart model, field, OD report name, and menu path. Role homes render a verified chip (see [PracticeManagerHome.tsx](../../frontend/src/pages/homes/PracticeManagerHome.tsx)).

**Current allowlist (2026-06-26):**

| KPI | Role home(s) |
|-----|----------------|
| Daily net collections | Practice Manager |

**Next validation batch (promote to homes after OD reconcile):**

| KPI | Mart | OD report | Role home(s) |
|-----|------|-----------|----------------|
| AR total | `mart_ar_summary` | AR Aging | Owner, Insurance |
| Total production | `mart_provider_performance` | Production and Income | Owner, Practice Manager |
| Total collections | `mart_provider_performance` | Production and Income | Owner, Practice Manager |

Collection rate and referral production stay off role homes until their inputs or definitions pass validation.

**Workflow:** See [validation/kpi/README.md](../../dbt_dental_models/validation/kpi/README.md) — golden export → compare SQL → registry status → `validatedKpis.ts` → role home card.

---

## Design Principle #1

### Organize Around Roles, Not Metrics

Most staff do not think in terms of:

- Production metrics
- Collection metrics
- Scheduling metrics

They think in terms of their responsibilities.

#### Practice Manager

Responsible for:

- Revenue
- Scheduling
- Provider productivity
- Treatment acceptance
- Hygiene retention

Home page should answer:

- What needs attention today?
- What is likely to become a problem next week?
- What actions should be taken?

**Maps to existing data:** Dashboard KPIs, Revenue opportunities, Treatment Acceptance summary, Hygiene retention, Appointments utilization, Provider performance.

#### Insurance Specialist

Responsible for:

- Claim resolution
- Eligibility verification
- Aging claims
- Insurance bottlenecks
- **Posting insurance payments / EOBs** (Open Dental workflows — often Front Desk–adjacent today)

Home page should answer:

- Which claims need action?
- Which claims are stuck?
- Which verifications are missing?
- Which carriers are causing delays?
- **What is the standard way we post paper EOB batch payments?** (SOP, not a warehouse metric)

**Maps to existing data:** Partial — Revenue opportunities include `Claim Rejection` type; AR KPIs expose `insurance_ar`. Full insurance workflow requires new API surfaces and likely complements the separate **Operations Knowledge** track described in the office knowledge proposal.

**Also see:** [Insurance Specialist Home + SOPs](#insurance-specialist-home--sops) — planned Phase **1B** deliverable (page + SOP authorship).

#### Front Desk

Responsible for:

- Scheduling
- Confirmations
- Reactivation

Home page should answer:

- Who should we call today?
- Which appointments need confirmation?
- Which patients are overdue?
- Which patients have unfinished treatment?
- Which patients are implant opportunities?

**Maps to existing data:** Appointments today/upcoming, Revenue missed-appointment opportunities, Hygiene retention gaps. **Primary UX:** tabbed or filtered call lists with reason chips (Overdue · Unfinished TX · Implant · No-show), not KPI cards. Reactivation, implant, and treatment-plan queues need new API endpoints (warehouse models partially exist).

#### Owner

Responsible for:

- Financial performance
- Growth
- Strategic decisions

Home page should answer:

- Are we on target?
- What are the largest risks?
- What trends require intervention?

**Maps to existing data:** Dashboard KPIs, AR aging trends, Referral source KPIs, Revenue trends.

---

## Design Principle #2

### Replace Dashboard Pages with Home Pages

Traditional analytics navigation (current sidebar):

```text
Dashboard
Revenue
AR Aging
Scheduling
Provider
Treatment Acceptance
Hygiene Retention
...
```

Recommended approach:

```text
Practice Manager Home   ← default for PM role
Insurance Home          ← default for insurance role
Front Desk Home         ← default for front desk role
Owner Home              ← default for owner role

More → Revenue, AR, Providers, KPI Definitions, ...
```

Each home page surfaces the most important information from multiple domains.

Users should rarely need to navigate elsewhere for daily work.

**Implementation note:** Keep existing routes (`/revenue`, `/ar-aging`, etc.) during transition. Role homes compose slices from the same API modules — they do not require new backend work for Phase 1 if we reuse existing endpoints.

---

## Design Principle #3

### Surface Exceptions Instead of Metrics

Users rarely need every metric.

They need exceptions.

Instead of:

```text
Schedule Utilization = 81%
```

Display:

```text
Schedule utilization is 9 points below target.
Next week contains 14 unfilled hygiene hours.
```

Instead of:

```text
AR > 90 Days = $41,000
```

Display:

```text
AR over 90 days increased 18% this month.
3 high-risk accounts represent $12,400 of that balance.
```

The system should identify what is unusual.

**Frontend approach:** Introduce an `InsightCard` component that accepts `{ metric, value, target, trend, severity, recommendedAction }` and renders narrative text. Thresholds can start as config constants (or env-driven) and later move to API-backed targets when the warehouse exposes them.

---

## Design Principle #4

### Build Work Queues

The most valuable output of analytics is often a list.

#### Reactivation / “Hasn’t been back” Queue

Patients:

- Overdue for hygiene (recall past due, no future appt)
- Not seen in N months (hygiene logic currently uses 6 months)
- High lifetime value (optional weight)

*Status:* Overdue **percent** in hygiene API; patient-level queue is new backend work. Eligibility rules (active only, contact prefs, recall disabled) need office sign-off — see Progress log.

#### Treatment Acceptance Queue

Patients:

- Large unscheduled treatment plans
- Recent consultations
- High conversion probability

*Status:* Treatment Acceptance page shows aggregate KPIs and provider breakdowns, not a patient-level queue. Warehouse: `int_treatment_plan` has `is_in_progress`, `remaining_amount`, `timeline_status` (30 / 90 / 180 day buckets).

#### Implant opportunities queue

Patients:

- Extraction + bone graft same day
- No implant placement (D6010) on record

*Status:* **dbt mart exists** (`mart_untapped_implant_revenue_detail` with demographics and contact prefs). No API or frontend surface yet. Confirm cohort definition and time window with clinical staff.

#### Collections Queue

Patients:

- High balance + high priority score
- Aging past thresholds

*Status:* **Already exists** — `/ar/priority-queue`. Needs promotion to a shared `WorkQueue` component and surfacing on role homes.

#### Revenue Recovery Queue

Opportunities:

- Missed appointments
- Claim rejections
- Treatment plan delays

*Status:* **Already exists** — `/revenue/opportunities` and `/revenue/recovery-plan` with `recommended_actions`. Needs unified queue UI.

#### Scheduling Queue

Appointments:

- Openings
- Cancellations
- High-risk no-shows

*Status:* Partial — today/upcoming appointment lists exist; no-show risk scoring not visible in frontend.

Analytics should lead directly to work.

**Shared component target:**

```text
WorkQueue
 ├── title, count, severity badge
 ├── sortable/filterable table
 ├── row actions (Open in OpenDental — future; Copy patient ID — now)
 └── link to full metric page for investigation
```

Extract from [AR.tsx](../frontend/src/pages/AR.tsx) priority table and [Revenue.tsx](../frontend/src/pages/Revenue.tsx) opportunities table as the first two instances.

---

## Design Principle #5

### Narrative Summaries

Most staff prefer short explanations over charts.

Example **Morning Brief**:

```text
Collections yesterday: $14,220
Collections are running 4% above target this month.

Three high-value claims remain unresolved.
Hygiene utilization next week is projected at 82%.

Recommended actions:
• Contact patients in the Reactivation Queue (12 patients)
• Follow up on claims older than 30 days (3 claims)
• Confirm Thursday hygiene openings (4 slots)
```

This may provide more value than dozens of charts.

**Phase 3 approach:** Template-driven generation in the frontend (or a thin `/brief/daily` API) using data already fetched for the role home. LLM assistance is optional and should not block delivery.

---

## Technical Foundations

Before building role homes, address structural debt that will compound:

### 1. Shared page infrastructure

Extract common patterns into `frontend/src/hooks/` and `frontend/src/components/common/`:

| Pattern | Used in | Extract to |
|---------|---------|------------|
| Async data fetch + loading + error | Every page | `useApiQuery` hook |
| Date range filter | Dashboard, AR, Treatment Acceptance, etc. | `DateRangeFilter` component |
| Currency / percent formatting | All metric pages | `utils/format.ts` |
| Tab panels | Appointments, KPIDefinitions | `TabPanel` component (currently duplicated) |
| Paginated table | AR, Revenue, Patients | `DataTable` component |

### 2. Role and layout model

Extend [Layout.tsx](../frontend/src/components/layout/Layout.tsx):

- Accept a `role` context (localStorage or future auth claim)
- Render role-appropriate sidebar sections: **Home**, **Queues**, **Reports** (collapsed metric pages)
- Replace placeholder [ClinicHome.tsx](../frontend/src/pages/ClinicHome.tsx) with role router → redirect to `/home/practice-manager` (or role picker on first visit)

### 3. Composition over new pages

Role homes should be **layout compositions**, not monolithic 2000-line files:

```text
pages/homes/
  PracticeManagerHome.tsx   → composes InsightCards + WorkQueues + mini charts
  OwnerHome.tsx
  FrontDeskHome.tsx
  InsuranceHome.tsx         → Phase 1B: SOPs + links; queues when API exists
```

### 4. Align with office knowledge proposal

Per [OFFICE_KNOWLEDGE_ANALYTICS_PROPOSAL.md](./OFFICE_KNOWLEDGE_ANALYTICS_PROPOSAL.md):

- **Practice Manager Home** = Analytics domain (warehouse-grounded)
- **Insurance Home** = Hybrid — warehouse claim status where available; **clinic SOPs** for OD workflows (paper EOB batch first); link to knowledge base / investigation memos for carrier-specific guidance
- Do not conflate the two domains in one UI until Phase 0 question intake confirms staff needs
- SOP authorship is Phase **1B** even if claim queues wait for Phase 2

---

## Proposed Roadmap

### Phase 1 — Role-Based Home Pages (4–6 weeks)

**Goal:** Replace the ClinicHome placeholder with actionable role entry points.

**Status (2026-06-26):** Partial — portal auth routes users to role homes; Practice Manager Home shows one validated KPI; Owner / Front Desk / Insurance are placeholders.

| Deliverable | Details | Status |
|-------------|---------|--------|
| Validated-KPI gate | Only `within_tolerance` metrics on role homes; `validatedKpis.ts` allowlist | **Active** — 1 of 4 target KPIs done |
| Validate AR total | Golden + `compare_ar_aging.sql` → Owner + Insurance homes | Not started |
| Validate total production / collections | Golden from Production and Income → Owner + PM homes | Not started |
| Role from portal login | `AuthContext` → role home path | **Done** |
| Practice Manager Home | Validated KPI cards + work queues | **Partial** — daily net collections only |
| Owner Home | Validated financial snapshot after validation batch | Placeholder |
| Front Desk Home | **Primary:** weighted call list + reason filters; secondary: schedule tab | Placeholder |
| Insurance Home | Placeholder scaffold exists (`InsuranceHome.tsx` → `/home/insurance`); full page + SOPs in **Phase 1B** | Placeholder |
| Shared components | `InsightCard`, `WorkQueue`, `MorningBrief` (static template), `useApiQuery`, `utils/format.ts` | **Partial** — `useApiQuery`, `format.ts` |
| Navigation restructure | Sidebar groups: Home · Queues · Reports (metric pages under Reports) | Not started |
| Refactor extract | Pull queue tables from AR and Revenue into shared `WorkQueue`; show **patient name + phone**, not ID only | Not started |

**Reuse existing API calls:**

- `dashboardApi.getKPIs`
- `arApi.getKPISummary`, `arApi.getPriorityQueue`
- `revenueApi.getOpportunities`, `revenueApi.getRecoveryPlan`
- `appointmentApi.getTodayAppointments`, `appointmentApi.getUpcomingAppointments`
- `hygieneApi.getRetentionSummary`
- `treatmentAcceptanceApi.getKPISummary`

**Success criteria:** Practice Manager opens clinic site, sees prioritized items without visiting `/dashboard` or `/ar-aging`.

---

### Insurance Specialist Home + SOPs

**Status (2026-07-15):** Planned — no code yet  
**Phase:** **1B** (can run in parallel with KPI validation / after PM home pattern is settled)  
**Priority:** High for clinic ops — staff already asking how to streamline paper EOB batch posting  
**Goal:** Give the Insurance Specialist a real landing page that combines (1) whatever validated queues/metrics are ready and (2) clinic SOPs for Open Dental insurance workflows that analytics cannot automate.

This is **not** a substitute for Open Dental. The portal owns **guidance + work lists**; payment posting still happens in OD.

#### Problem signal (from Front Desk / insurance staff)

Staff describe paper EOB posting as: *block out payments individually, then scan EOBs* — i.e. per-claim posting followed by a second pass for scanning/attaching. Pain covers **scanning, allocating dollars, attaching images**, with “HIPAA” meaning **retain proof of payment**, not a privacy edge case.

**Real question:** How do we process one paper multi-claim EOB as a **single batch deposit** (scan once, attach once, allocate under that check) while keeping an audit trail?

#### Proposed SOP content (first SOP)

Author and review with the office, then publish on Insurance Home:

1. Receive check + multi-page paper EOB  
2. Create **one** batch insurance payment for the full check amount (Open Dental ClaimPayment)  
3. Allocate claim amounts **under that batch** from the EOB lines  
4. Scan the EOB **once** and attach it to **that** batch payment (`EobAttach` → claim payment)  
5. Stop when check total = sum of allocated claims **and** the EOB is attached  

Document what this **does not** solve (line-by-line dollar entry from paper) and when ERA/EFT is the longer-term fix.

#### Frontend deliverables (Phase 1B)

| Deliverable | Details | Status |
|-------------|---------|--------|
| Insurance Home v1 | Replace `RoleHomePlaceholder` with a real layout: intro, SOP section, links to available Reports (AR), placeholders for future queues | Not started |
| SOP library (static first) | Markdown (or equivalent) SOPs in repo — e.g. `docs/sops/insurance/` or `frontend` content module — rendered or linked from Insurance Home | Not started |
| Paper EOB batch payment SOP | First authored SOP (draft → office review → published) | Not started |
| SOP discoverability | Insurance Home lists SOPs with short “when to use”; printable/copy-friendly for training | Not started |
| Optional: Front Desk cross-link | If Front Desk also posts EOBs, link the same SOP from Front Desk Home | Not started |

**Out of scope for 1B:** building payment-posting UI inside the portal; OCR of EOBs; ERA automation; claim follow-up API (Phase 2 Insurance Follow-Up queue).

#### Later (Phase 2+)

- Insurance Follow-Up queue (stuck claims, missing verification)  
- Validated AR total card once KPI validation passes  
- EOB documentation coverage as a quality signal from warehouse (`int_insurance_eob_attachments` / claim payment grain) — **monitor**, do not replace OD attachment  

**Success criteria:** Insurance Specialist (or Front Desk posting payments) can open `/home/insurance`, find the paper EOB batch SOP, and follow one check-level workflow without tribal knowledge.

---

### Phase 2 — Operational Work Queues (4–6 weeks)

**Goal:** Unified, filterable work lists that staff can act on daily.

**Status (2026-06-18):** Backend queues partially exist; patient-level call lists and implant endpoint not built.

| Queue | Backend | Frontend | Status |
|-------|---------|----------|--------|
| **Master “who to call”** | New — merge + score + dedupe | Front Desk / PM home | Not started |
| Collections | `/ar/priority-queue` (exists) | `/queues/collections`; export/copy actions | API only |
| Revenue Recovery | `/revenue/recovery-plan` (exists) | Show `recommended_actions` prominently | API only |
| Scheduling | `/appointments/today`, `/appointments/upcoming` (exist) | Confirmation column when API supports it | API only |
| Unfinished treatment | New — query `int_treatment_plan` | Filter: in-progress, accepted-not-started; threshold config | dbt only |
| Overdue / reactivation | New — query `int_recall_management` | Overdue recall, no future appt | dbt + aggregate API |
| Implant opportunities | New — query `mart_untapped_implant_revenue_detail` | Dedicated tab on Front Desk home | **dbt only** |
| Insurance Follow-Up | New endpoint needed | Stuck claims, missing verification | Not started |

**Success criteria:** Staff can complete a morning workflow from queues alone without opening metric dashboards.

---

### Phase 3 — Automated Narrative Briefings (2–4 weeks)

**Goal:** Plain-language summaries on role homes.

| Deliverable | Details |
|-------------|---------|
| Daily brief | Template populated from Phase 1 API bundle |
| Weekly brief | Owner and Practice Manager homes |
| Exception detection rules | Config file: thresholds for "above/below target", "increased X% this month" |
| Optional LLM polish | Post-process template text; never the sole source of numbers |

**Success criteria:** Owner reads a 5-sentence brief and knows whether the week requires intervention.

---

### Phase 4 — Guided Investigation (4–6 weeks)

**Goal:** Drill from anomaly to root cause without SQL.

Example flow:

```text
Collections Down
  ↓
Provider Breakdown
  ↓
Procedure Breakdown
  ↓
Patient / Appointment Detail
```

**Approach:**

- `InsightCard` click opens investigation drawer
- Reuse existing metric pages as leaf nodes
- Add cross-links between related pages (e.g., AR queue row → patient detail)
- Leverage `dbtMetadataApi.getMetricLineage` (already wired in `KPICard` via `InfoTooltip`) for analyst transparency

**Success criteria:** Practice Manager investigates "why collections dropped" in-app without asking the maintainer.

---

### Phase 5 — Conversational Layer

Only after the platform successfully delivers information through role-based workflows.

Natural-language interaction becomes an enhancement rather than a dependency.

Align with [OFFICE_KNOWLEDGE_ANALYTICS_PROPOSAL.md](./OFFICE_KNOWLEDGE_ANALYTICS_PROPOSAL.md) Phase 0 outcomes before investing here.

---

## What Not to Do (Yet)

| Temptation | Why wait |
|------------|----------|
| Build more metric pages | Diminishing returns; existing pages cover core domains |
| Add LLM chat as the primary UI | Does not solve "what needs attention today" without structured queues and exceptions |
| Full auth/RBAC system in Phase 1 | Lightweight role picker is enough to validate UX; defer until deployment requirements are clear |
| Merge insurance knowledge and analytics | Different information domains; conflating them creates confusing UX |
| Rewrite in a new framework | React + MUI + existing API layer are sufficient; evolution, not replacement |

---

## Recommendation

The MDC platform has reached the point where data modeling is no longer the primary constraint for practice manager and owner workflows.

The next major opportunity is improving **information delivery** in [frontend/](../frontend/).

Development should prioritize:

1. **Call lists over charts** — Front Desk and PM workflows centered on weighted “who to call” queues, with metric pages under Reports
2. **Replace ClinicHome placeholder** with role-based home pages
3. **Promote existing queues** (AR priority, revenue recovery) to first-class operational objects with patient names and phones
4. **Add patient-level queue APIs** (overdue recall, unfinished treatment, implant cohort) using existing dbt marts
5. **Add exception framing and narrative briefs** over raw KPI cards
6. **Guided investigation** paths from insight to detail

before investing heavily in conversational analytics or additional metric dashboards.

The goal is not to give staff more data.

The goal is to help staff make better decisions with the data that already exists.

---

## Appendix: Clinic vs Demo Build

| Concern | Clinic (`VITE_IS_DEMO=false`) | Demo / Portfolio |
|---------|-------------------------------|------------------|
| Root route `/` | [ClinicHome.tsx](../../frontend/src/pages/ClinicHome.tsx) placeholder → **target:** role picker → role home | [Portfolio_v3.tsx](../../frontend/src/pages/Portfolio_v3.tsx) |
| Sidebar title | MDC & GLIC Analytics | Dental Analytics |
| Synthetic data banner | Hidden | Shown on portfolio hostnames |
| Target users | Office staff | Portfolio visitors |
| This proposal applies to | **Clinic build only** | Out of scope |

Deployment: clinic frontend via `mdc deploy frontend --target clinic` (see [ENVIRONMENT_FILES.md](../ENVIRONMENT_FILES.md)); demo and clinic are separate deploy targets. Long-term repo layout: [FRONTEND_SPLIT_PLAN.md](./FRONTEND_SPLIT_PLAN.md).

---

## Appendix: File Change Map (Phase 1)

| File | Change | Status |
|------|--------|--------|
| `frontend/src/pages/ClinicHome.tsx` | Replace placeholder → role router or role picker | **Placeholder only** |
| `frontend/src/pages/homes/*.tsx` | Role home compositions (`PracticeManagerHome`, `FrontDeskHome`, …) | Not created |
| `frontend/src/components/queues/WorkQueue.tsx` | Extract from AR + Revenue tables | Not created |
| `frontend/src/components/insights/InsightCard.tsx` | Exception-first metric display | Not created |
| `frontend/src/components/layout/Layout.tsx` | Role-aware sidebar: Home · Queues · Reports | Clinic/demo split only |
| `frontend/src/hooks/useApiQuery.ts` | Shared fetch pattern | Not created |
| `frontend/src/context/RoleContext.tsx` | Role selection and persistence | Not created |
| `frontend/src/config/insights.ts` | Exception thresholds (tunable without code) | Not created |
| `frontend/src/App.tsx` | Add `/home/practice-manager`, `/home/front-desk`, … | Analytics routes only |

Existing metric pages remain unchanged in Phase 1; they become drill-down destinations linked from role homes under a **Reports** nav group.

---

## Appendix: Suggested next steps

1. **Office assumption workshop** — confirm eligibility and weighting rules for each queue (see Progress log table).
2. **Phase 1 scaffold** — `RoleContext`, `WorkQueue`, `useApiQuery`; wire Practice Manager and Front Desk homes to existing AR + revenue + appointment APIs.
3. **Phase 1B — Insurance Home + SOPs** — draft paper EOB batch payment SOP with the office; replace Insurance Home placeholder; surface SOP library (plan in [Insurance Specialist Home + SOPs](#insurance-specialist-home--sops)).
4. **Phase 2 API** — `/queues/overdue-recall`, `/queues/unfinished-treatment`, `/queues/implant-opportunities` backed by existing marts.
5. **Front Desk as primary role** — enable in role picker first; PM/Owner homes can stay thinner until call-list UX is validated.
