# Archived portfolio landing pages

Superseded portfolio implementations kept for reference. **Not routed in production.**

| File | Notes |
|------|--------|
| `Portfolio.tsx` | Original legacy landing page |
| `Portfolio_v2.tsx` | Second iteration |
| `Portfolio_v3.tsx` | Production system presentation (pre–v4 hire-focused layout) |

**Live portfolio:** [`apps/portfolio/src/pages/Portfolio.tsx`](../../apps/portfolio/src/pages/Portfolio.tsx) in the `@mdc/portfolio` workspace app (`mdc deploy frontend --target demo`).

These files were moved from the former monolith `src/pages/`; import paths (`../components/...`) assume the old location and are not compiled. Copy into `apps/portfolio/src/pages/` and fix paths if you need to run one locally.
