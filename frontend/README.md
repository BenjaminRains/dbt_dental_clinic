# Dental Analytics Frontend

npm workspaces monorepo with **two deployable apps** and shared packages.

| App / package | Role |
|---------------|------|
| `@mdc/portfolio` (`apps/portfolio`) | Public portfolio + synthetic demo (`dbtdentalclinic.com`) |
| `@mdc/clinic` (`apps/clinic`) | Staff portal + PHI analytics (`clinic.dbtdentalclinic.com`) |
| `@mdc/analytics-ui` | Shared dashboard pages + charts |
| `@mdc/analytics-api` | Shared API client + types (`authApi` via `@mdc/analytics-api/clinic` only) |
| `@mdc/ui-common` | Format helpers, tooltips, Mermaid |

See [docs/frontend/FRONTEND_SPLIT_PLAN.md](../docs/frontend/FRONTEND_SPLIT_PLAN.md).

## Getting started

### Prerequisites
- Node.js 18+
- npm

### Install (repo root of this folder)

```bash
npm install
```

### Local development

Prefer the CLI (writes `.env.local` under the chosen app):

```bash
mdc frontend dev                 # portfolio (default)
mdc frontend dev --app clinic    # clinic portal
```

Or from this directory:

```bash
npm run dev:portfolio
npm run dev:clinic
```

App URLs: `http://localhost:3000` (Vite proxies `/api` → `http://localhost:8000`).

### Environment variables

Written per-app to `apps/<name>/.env.local` by `mdc frontend dev`:

| Variable | Purpose |
|----------|---------|
| `VITE_API_URL` | Backend API (local: `http://localhost:8000`) |
| `VITE_API_KEY` | API key |
| `VITE_IS_DEMO` | Build metadata (`true` portfolio / `false` clinic). Routing no longer depends on hostname helpers. |

## Project structure

```
frontend/
├── apps/
│   ├── portfolio/          # Public product
│   └── clinic/             # Staff portal
├── packages/
│   ├── analytics-api/
│   ├── analytics-ui/
│   └── ui-common/
├── package.json            # workspaces root
└── vite.shared.ts          # shared Vite alias / chunk config
```

## Scripts

| Script | Description |
|--------|-------------|
| `npm run dev` / `dev:portfolio` | Portfolio Vite dev |
| `npm run dev:clinic` | Clinic Vite dev |
| `npm run build:portfolio` | Production build → `apps/portfolio/dist` |
| `npm run build:clinic` | Production build → `apps/clinic/dist` |
| `npm run build` | Build both apps |
| `npm run type-check` | Typecheck both apps |

## Deploy

AWS targets unchanged; CLI chooses workspace:

```bash
mdc deploy frontend --target demo     # builds @mdc/portfolio
mdc deploy frontend --target clinic   # builds @mdc/clinic
```

SPA deep-link fallbacks are uploaded per target (`PORTFOLIO_SPA_ROUTE_KEYS` / `CLINIC_SPA_ROUTE_KEYS`).

CI (`.github/workflows/frontend.yml`) type-checks and builds both workspace apps on PRs that touch `frontend/`.

## Tech stack

- React 18, TypeScript, Material-UI 6, Recharts, Axios, React Router 6, Vite 6
