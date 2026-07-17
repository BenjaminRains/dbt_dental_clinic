# Frontend documentation

| Document | Purpose |
|----------|---------|
| [FRONTEND_EVOLUTION_PROPOSAL.md](./FRONTEND_EVOLUTION_PROPOSAL.md) | **Product direction:** role homes, call lists, operational queues (clinic build) |
| [FRONTEND_SPLIT_PLAN.md](./FRONTEND_SPLIT_PLAN.md) | **Phased refactor:** split portfolio and clinic into two deployable apps |
| [../../TODO.md](../../TODO.md) | Tracked under **Deployment / Infrastructure → Frontend split** |
| [../ENVIRONMENT_FILES.md](../ENVIRONMENT_FILES.md) | Env files, `VITE_*` vars, `mdc deploy frontend` |
| [../../frontend/README.md](../../frontend/README.md) | Local dev setup, Vite scripts |

**Live surfaces**

| Host | Product | Workspace app | Build target |
|------|---------|---------------|--------------|
| `dbtdentalclinic.com` | Portfolio + synthetic analytics demo | `@mdc/portfolio` | `mdc deploy frontend --target demo` (or `--target portfolio`) |
| `clinic.dbtdentalclinic.com` | Staff portal + PHI analytics | `@mdc/clinic` | `mdc deploy frontend --target clinic` |

Local: `mdc frontend dev --app portfolio|clinic`

Nomenclature: use **portfolio/clinic** for the Vite apps (`--app`); use **demo/clinic** for env stages; frontend deploy accepts both `demo` and `portfolio`.
