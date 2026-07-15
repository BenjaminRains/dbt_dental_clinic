# demo-frontend-deploy investigation (historical)

> **Superseded (2026-07):** The unified SPA and hostname/`VITE_IS_DEMO` routing were removed in the frontend app split.
> Deploy with `mdc deploy frontend --target demo|clinic` (builds `@mdc/portfolio` / `@mdc/clinic`).
> See [frontend/README.md](../README.md) and [FRONTEND_SPLIT_PLAN.md](../../docs/frontend/FRONTEND_SPLIT_PLAN.md).

The notes below describe the **pre-split** build (single `frontend/` Vite app + PowerShell deploy aliases). Kept for incident archaeology only.

---

# demo-frontend-deploy: Why It Used Vite “demo=true” (legacy)

**Purpose (legacy):** Document how `demo-frontend-deploy` produced a demo/portfolio build and where messaging was defined.

## 1. Command and Flow (legacy)

| Alias | Function | Effect |
|-------|----------|--------|
| `demo-frontend-deploy` | `Deploy-Frontend` | Built unified frontend with demo env, uploaded to S3 |
| `clinic-frontend-deploy` | `Deploy-ClinicFrontend` | Built with clinic env, deployed to clinic bucket |

Current equivalent:

```text
mdc deploy frontend --target demo
mdc deploy frontend --target clinic
```

## 2. Historical notes

- Demo vs clinic was selected with `VITE_IS_DEMO` + `isClinicSite()` hostname checks in one `App.tsx`.
- Product identity now comes from which workspace package is built; `VITE_IS_DEMO` remains build metadata only.
