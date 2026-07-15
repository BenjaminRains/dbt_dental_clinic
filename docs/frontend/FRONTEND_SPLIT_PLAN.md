# Frontend split plan: Portfolio vs Clinic

**Status:** Active — Phases 0–3 complete in tree; deploy/verify in production next  
**Owner:** Engineering  
**Last updated:** 2026-07-15  
**Tracking:** [TODO.md](../../TODO.md) (Tier 5 → Frontend Split; Frontend Evolution)

---

## 1. Executive summary

Today we ship **one React application** twice: a demo/portfolio build and a clinic build. Both share a single `App.tsx`, router, `Layout`, auth context, and analytics pages. Runtime and build-time flags (`isClinicSite()`, `VITE_IS_DEMO`) choose which product the user sees.

That worked for cost-conscious early deployment but **does not scale** as portfolio becomes a hiring/marketing product and clinic becomes a staff portal with PHI, roles, and WAF restrictions.

**Goal:** Two **deployable frontend apps** in this monorepo, sharing only deliberate libraries (analytics UI, API client, types). **AWS edge infrastructure stays as-is** until operational or compliance pain justifies change.

**Non-goals (for this refactor)**

- Splitting the ALB, RDS, or Route53 zone
- Moving portfolio to a different repo or stack (Phase 6+ option only)
- Rewriting dashboard pages or the FastAPI backend
- Changing demo vs clinic API hostnames

---

## 2. Why this matters

### 2.1 Product divergence

| Dimension | Portfolio (`dbtdentalclinic.com`) | Clinic (`clinic.dbtdentalclinic.com`) |
|-----------|-----------------------------------|----------------------------------------|
| Audience | Recruiters, engineers, public | Staff (owner, front desk, insurance, …) |
| Auth | None (API key in bundle for synthetic data) | Portal login + session |
| Entry | Marketing landing (`Portfolio_v4`) | `/login` → role home |
| Data | `opendental_demo` via demo API | `opendental_analytics` via clinic API |
| Network | Public internet | WAF IP allowlist |
| Future | Case study, artifacts, maybe static/marketing | Workflows, queues, role-specific UX |

These are **different products** that happen to share chart components today.

### 2.2 Current coupling (problems)

1. **One router** registers clinic routes (`/home/owner`, `/login`, …) on the portfolio host.
2. **`Layout.tsx`** branches on `isClinicSite()` for nav, titles, auth chrome, and demo banner.
3. **`AuthProvider`** wraps the entire tree, including portfolio pages that never use it.
4. **`VITE_IS_DEMO` + hostname** duplicate the same “which product?” decision in several files.
5. **Deploy** uploads one `dist/` with route fallbacks tuned for clinic paths; portfolio and clinic SPA route lists are conflated.
6. **Mental model:** “change App.tsx” is the only way to promote a portfolio version; easy to ship the wrong bundle to the wrong bucket.

### 2.3 What AWS already separates (keep until it hurts)

| Resource | Demo / portfolio | Clinic | Notes |
|----------|------------------|--------|-------|
| Domain | `dbtdentalclinic.com`, `www` | `clinic.dbtdentalclinic.com` | Same hosted zone |
| S3 | Demo frontend bucket | `dbt-dental-clinic-clinic-frontend-prod` (see credentials template) | **Already separate** |
| CloudFront | Demo distribution | Clinic distribution + WAF | **Already separate** |
| API host | `api.dbtdentalclinic.com` | `api-clinic.dbtdentalclinic.com` | **Same ALB**, host-based routing |
| API EC2 | Demo instance | Clinic instance | Separate systemd units |
| Database | `opendental_demo` | `opendental_analytics` | **Same RDS**, two DBs |

**Cost-saving choices we preserve:** one ALB, one RDS instance, one domain family, one repo. **What we fix:** one **codebase pretending to be two apps**.

---

## 3. Target architecture

### 3.1 Monorepo layout (end state)

```
frontend/
├── package.json                 # npm workspaces root
├── apps/
│   ├── portfolio/               # Product: public portfolio + synthetic demo
│   │   ├── package.json
│   │   ├── vite.config.ts
│   │   ├── index.html
│   │   └── src/
│   │       ├── main.tsx
│   │       ├── App.tsx          # Portfolio routes only
│   │       ├── pages/
│   │       │   ├── Portfolio.tsx        # v4 landing (rename when stable)
│   │       │   ├── AgentProfile.tsx
│   │       │   ├── EnvironmentManager.tsx   # optional: move to docs-only later
│   │       │   └── SchemaDiscovery.tsx
│   │       └── shell/
│   │           └── DemoLayout.tsx   # Sidebar + synthetic data banner
│   │
│   └── clinic/                  # Product: staff portal
│       ├── package.json
│       ├── vite.config.ts
│       ├── index.html
│       └── src/
│           ├── main.tsx
│           ├── App.tsx          # Clinic routes only
│           ├── pages/
│           │   ├── Login.tsx
│           │   ├── ClinicRoot.tsx
│           │   └── homes/       # OwnerHome, InsuranceHome, …
│           └── shell/
│               └── ClinicLayout.tsx
│
├── packages/
│   ├── analytics-ui/            # Dashboard, Revenue, AR, charts, KPIDefinitions, …
│   ├── analytics-api/           # api.ts, authApi.ts (clinic-only export), types
│   └── ui-common/               # MermaidDiagram, InfoTooltip, format utils (optional)
│
└── archive/
    └── portfolio/               # Legacy v1–v3 (reference, not built)
```

### 3.2 Dependency rules

```
apps/portfolio  →  analytics-ui, analytics-api (demo client only), ui-common
apps/clinic     →  analytics-ui, analytics-api (full), ui-common

analytics-ui    →  analytics-api, ui-common
analytics-api   →  (no app imports)

apps/portfolio  ✗  apps/clinic
apps/clinic     ✗  apps/portfolio
```

### 3.3 Route ownership (end state)

**Portfolio app**

| Path | Page |
|------|------|
| `/` | Portfolio landing |
| `/agent-profile` | Agent-readable profile |
| `/dashboard`, `/revenue`, … | Shared analytics pages (synthetic) |
| `/environment-manager`, `/schema-discovery` | Engineering showcase (optional; may move behind GitHub links only) |

**Clinic app**

| Path | Page |
|------|------|
| `/login` | Portal login |
| `/` | Redirect to role home (authenticated) |
| `/home/*` | Role homes |
| `/dashboard`, `/revenue`, … | Same analytics components, clinic API + auth |

No cross-product routes in either `App.tsx`. No `isClinicSite()` in shared pages.

### 3.4 Deploy mapping (unchanged AWS targets)

| CLI | Builds | S3 / CloudFront | Env baked at build |
|-----|--------|-----------------|-------------------|
| `mdc deploy frontend --target demo` | `apps/portfolio` | `deployment_credentials.json` → `frontend` | `VITE_API_URL=https://api.dbtdentalclinic.com`, demo API key |
| `mdc deploy frontend --target clinic` | `apps/clinic` | `deployment_credentials.json` → `clinic_frontend` | `VITE_API_URL=https://api-clinic.dbtdentalclinic.com`, clinic API key |

`mdc frontend dev` gains a `--app portfolio|clinic` flag (default `portfolio`).

---

## 4. Phased implementation

Each phase has **deliverables**, **exit criteria**, and **rollback**. Do not skip exit criteria.

---

### Phase 0 — Guardrails (1–2 days)

**Purpose:** Stop the bleeding while the structural refactor is planned. No new folders yet.

**Work**

1. Split route registration in current `App.tsx`:
   - `portfolioRoutes` — `/`, `/agent-profile`, analytics, showcase tools
   - `clinicRoutes` — `/login`, `/`, `ClinicRoot`, `/home/*`, analytics behind `ClinicAuthGate`
   - Render clinic routes **only when** `isClinicSite()` is true
2. Add `docs/frontend/ROUTE_INVENTORY.md` (optional checklist) or keep inventory in this doc §5.
3. Split `CLINIC_SPA_ROUTE_KEYS` vs `PORTFOLIO_SPA_ROUTE_KEYS` in `deploy_frontend.py` (upload correct fallbacks per `--target`).
4. Ensure `Portfolio_v4` and clinic pages are committed; deploy demo after any portfolio promotion.

**Exit criteria**

- [x] `dbtdentalclinic.com/home/owner` returns 404 or redirects to `/` (not clinic UI) — portfolio `/home/*` → `Navigate` to `/` in `App.tsx`
- [x] `clinic.dbtdentalclinic.com/login` still works; WAF + portal unchanged — clinic routes unchanged; clinic-only mount when `isClinicSite()`
- [x] Demo and clinic deploy scripts upload route keys appropriate to each target — `PORTFOLIO_SPA_ROUTE_KEYS` / `CLINIC_SPA_ROUTE_KEYS` via `spa_route_keys_for_target()`

**Rollback:** Revert `App.tsx` route guards only.

---

### Phase 1 — Shared packages scaffold (3–5 days)

**Purpose:** Extract shared code without moving apps yet. Reduces duplicate work in Phase 2–3.

**Work**

1. Enable **npm workspaces** at `frontend/package.json`.
2. Create `packages/analytics-api`:
   - Move `src/services/api.ts`, `src/types/api.ts`
   - Move `src/hooks/useApiQuery.ts`
   - Clinic auth: `authApi.ts` lives here but is imported only by clinic app (or `analytics-api/clinic` subpath)
3. Create `packages/analytics-ui`:
   - Move dashboard pages: `Dashboard`, `Revenue`, `AR`, `Providers`, `Patients`, `Appointments`, `TreatmentAcceptance`, `HygieneRetention`, `ReferralSources`, `KPIDefinitions`
   - Move `components/charts/*`, shared table/card components used by those pages
   - Export stable page components (e.g. `export function RevenuePage()`)
4. Create `packages/ui-common` (if needed):
   - `MermaidDiagram`, `InfoTooltip`, `format.ts`
5. Current single app imports from `@mdc/analytics-ui` etc.; behavior unchanged.

**Exit criteria**

- [x] `npm run build` at workspace root succeeds (`build:portfolio` + `build:clinic`)
- [x] `mdc frontend dev` works; dashboards load locally (`--app portfolio|clinic`)
- [x] No circular imports between packages
- [x] Bundle size not materially worse (±10%) — clinic main bundle smaller without portfolio/mermaid chrome

**Rollback:** Keep packages on a branch; main stays on single `src/` until verified.

---

### Phase 2 — Portfolio app (3–5 days)

**Purpose:** First deployable split — portfolio product stands alone.

**Work**

1. Create `apps/portfolio` with own Vite config, `index.html`, `main.tsx`.
2. Move:
   - `Portfolio_v4.tsx` → `apps/portfolio/src/pages/Portfolio.tsx`
   - `AgentProfile.tsx`
   - `EnvironmentManager.tsx`, `SchemaDiscovery.tsx` (or defer removal)
   - `DemoLayout.tsx` extracted from current `Layout.tsx` demo branch (portfolio nav, synthetic banner)
3. `App.tsx` — portfolio routes only; lazy-load pages from `@mdc/analytics-ui`.
4. **No** `AuthProvider`, `ClinicAuthGate`, `Login`, role homes, `isClinicSite()`.
5. Update `mdc deploy frontend --target demo` to `npm run build --workspace=portfolio` (or `cd apps/portfolio && npm run build`).
6. Deploy to demo bucket; verify production portfolio.

**Exit criteria**

- [x] Portfolio app at `apps/portfolio` — no clinic login, `/home/*` redirects to `/` (pending production redeploy)
- [x] Analytics demo pages wired against `@mdc/analytics-ui` + demo API env at build
- [x] CLI builds `@mdc/portfolio` for `--target demo` (Phase 3 also switched clinic; no legacy dual-build window)

**Rollback:** Point demo CloudFront back to previous unified `dist` build artifact.

---

### Phase 3 — Clinic app (3–5 days)

**Purpose:** Clinic product stands alone; remove unified app.

**Work**

1. Create `apps/clinic` with Vite config, `main.tsx`, `AuthProvider` in clinic entry only.
2. Move:
   - `Login`, `ClinicRoot`, `pages/homes/*`
   - `ClinicLayout.tsx` (clinic nav, role home, sign out)
   - `ClinicAuthGate`, `AuthContext`, `roleTypes`, `roleNavigation`
   - `RoleSwitcher` (clinic only)
3. `App.tsx` — clinic routes only; analytics from `@mdc/analytics-ui`.
4. Update `mdc deploy frontend --target clinic` to build `apps/clinic`.
5. Update `mdc frontend dev --app clinic` for portal development.
6. **Delete** legacy `frontend/src/App.tsx` monolith and `config/clinicSite.ts` hostname routing (each build is one product).

**Exit criteria**

- [x] Clinic app at `apps/clinic` — login, role homes, reports, sign out (pending production redeploy)
- [x] Portal auth client unchanged (`authApi` via `@mdc/analytics-api/clinic`)
- [x] SPA fallbacks use clinic-only route key list
- [x] Unified `frontend/src/` app removed; product identity is the workspace app (not hostname `isClinicSite`)

**Rollback:** Keep last unified clinic `dist` artifact; redeploy to clinic bucket.

---

### Phase 4 — Deploy & dev ergonomics (2–3 days)

**Purpose:** Make two-app workflow obvious and hard to get wrong.

**Work**

1. `mdc frontend dev --app portfolio|clinic` — writes correct `.env.local` per app (API URL, keys; portfolio sets no clinic secrets).
2. `mdc frontend status` — show both apps, buckets, last deploy hint.
3. `mdc deploy frontend --target demo|clinic` — validate workspace, run typecheck for that app only.
4. Document in `frontend/README.md` and `tools/mdc_cli/README.md`.
5. CI (if/when added): `npm run build --workspaces` or matrix build both apps.

**Exit criteria**

- [ ] New contributor can run portfolio locally without clinic env vars
- [ ] Deploying demo cannot accidentally upload clinic bundle (assert `VITE_API_URL` / package name in preflight)

---

### Phase 5 — Hardening & cleanup (2–4 days)

**Purpose:** Pay down debt; clarify ownership.

**Work**

1. Remove dead code: `ClinicHome.tsx` if superseded, old `RoleContext` indirection, `isClinicSite` helpers.
2. Rename `Portfolio_v4` → `Portfolio` in portfolio app when stable.
3. Audit `packages/analytics-ui` for demo-only copy (synthetic banner text) — inject via props or layout wrapper, not `isClinicSite()` inside pages.
4. Optional: extract `DemoDataBanner` component used only by portfolio `DemoLayout`.
5. Update portfolio docs (`frontend/docs/*`) to reference new paths.

**Exit criteria**

- [ ] Grep for `isClinicSite` / `VITE_IS_DEMO` in `apps/` and `packages/` returns only deploy config or zero hits
- [ ] Both apps pass `npm run type-check`

---

### Phase 6 — Portfolio as independent product (future)

**Purpose:** Optional; only when portfolio IA, branding, or release cadence diverges sharply from clinic.

**Options (not mutually exclusive)**

| Option | When |
|--------|------|
| Portfolio repo + separate CI | Marketing site on different cadence than clinic |
| Static export / SSR (Astro, Next marketing) | SEO, blog, case studies; keep demo dashboards in monorepo |
| `demo.dbtdentalclinic.com` | Split “marketing” from “interactive synthetic demo” |
| Drop shared `analytics-ui` from portfolio | Portfolio links out to demo subdomain instead of embedding charts |

**AWS still unchanged** until: WAF complexity, compliance audit, or team ownership forces separate accounts.

---

## 5. File inventory (current → target)

| Current path | Target |
|--------------|--------|
| `src/pages/Portfolio_v4.tsx` | `apps/portfolio/src/pages/Portfolio.tsx` |
| `src/pages/AgentProfile.tsx` | `apps/portfolio/src/pages/` |
| `src/pages/EnvironmentManager.tsx` | `apps/portfolio/src/pages/` (or remove later) |
| `src/pages/SchemaDiscovery.tsx` | `apps/portfolio/src/pages/` (or remove later) |
| `src/pages/Login.tsx`, `ClinicRoot.tsx`, `homes/*` | `apps/clinic/src/pages/` |
| `src/pages/Dashboard.tsx`, `Revenue.tsx`, … | `packages/analytics-ui/src/pages/` |
| `src/components/charts/*` | `packages/analytics-ui/` |
| `src/services/api.ts`, `types/api.ts` | `packages/analytics-api/` |
| `src/services/authApi.ts` | `packages/analytics-api/` (clinic consumer only) |
| `src/context/AuthContext.tsx`, `roleTypes.ts` | `apps/clinic/src/auth/` |
| `src/components/auth/ClinicAuthGate.tsx` | `apps/clinic/src/auth/` |
| `src/components/layout/Layout.tsx` | Split → `DemoLayout`, `ClinicLayout` |
| `src/config/clinicSite.ts` | **Delete** after Phase 3 |
| `src/App.tsx` | **Delete** → two app-level `App.tsx` files |
| `frontend/archive/portfolio/*` | Unchanged (reference) |

---

## 6. Testing strategy

### Per phase

| Check | Portfolio | Clinic |
|-------|-----------|--------|
| `/` loads correct home | Landing v4 | Login or role redirect |
| Deep link `/dashboard` | Demo layout + synthetic banner | Auth gate → login if logged out |
| API calls | `api.dbtdentalclinic.com` + demo key | `api-clinic.dbtdentalclinic.com` + clinic key |
| No PHI on demo | Synthetic data only | Real data behind WAF + login |

### Scripts

- Existing: `scripts/verification/verify_clinic_portal_auth.ps1` (clinic)
- Add: `scripts/verification/verify_portfolio_routes.ps1` — smoke HTTP checks for demo host (no `/home/*` 200s)

### Manual smoke after each deploy

```text
Demo:     https://dbtdentalclinic.com/
          https://dbtdentalclinic.com/dashboard
          https://dbtdentalclinic.com/home/owner  → must NOT show clinic home

Clinic:   https://clinic.dbtdentalclinic.com/login
          (from allowlisted IP) login → role home → dashboard → sign out
```

---

## 7. Risks and mitigations

| Risk | Mitigation |
|------|------------|
| Wrong bundle to wrong bucket | Deploy preflight: assert API URL + app name; separate `dist/` dirs per app |
| Shared package breaking both apps | Workspace versioning; build both in CI before merge |
| Clinic SPA deep links break | Keep clinic-specific `SPA_ROUTE_KEYS`; CloudFront 403→index on clinic distribution |
| Portfolio deploy breaks dbt-docs path | dbt-docs is separate S3 prefix (`mdc deploy dbt-docs`); unchanged |
| Large bang-for-buck Phase 1 | Phase 0 first; Phase 1 can overlap with portfolio v4 content work |
| Two `node_modules` / install pain | npm workspaces hoisting; single `npm install` at `frontend/` |

---

## 8. Decision log

| Date | Decision |
|------|----------|
| 2026-06-18 | Split into `apps/portfolio` + `apps/clinic` + shared packages; keep AWS ALB/RDS/zone until pain |
| 2026-06-18 | Archive legacy portfolio pages under `frontend/archive/` (not built) |
| 2026-06-18 | Phase 0 route guards before monorepo scaffold |

---

## 9. Related docs

- [deployment_credentials.json.template](../../deployment_credentials.json.template) — S3, CloudFront, WAF per surface
- [deployment-connections/README.md](../deployment-connections/README.md) — clinic WAF, portal login
- [ENVIRONMENT_FILES.md](../ENVIRONMENT_FILES.md) — `VITE_*`, deploy env
- [api/README.md](../../api/README.md) — demo vs clinic API separation

---

## 10. Quick reference: what “done” looks like

```text
Before:  one App.tsx + isClinicSite() + two deploy flags
After:   two apps, shared analytics packages, same two S3 buckets and CloudFront distributions

Portfolio engineer:  cd frontend && mdc frontend dev --app portfolio
Clinic engineer:     cd frontend && mdc frontend dev --app clinic

Ship portfolio:      mdc deploy frontend --target demo
Ship clinic:         mdc deploy frontend --target clinic
```

No hostname trickery. No clinic routes on the public site. Portfolio and clinic can evolve independently while sharing the chart library we already built.
