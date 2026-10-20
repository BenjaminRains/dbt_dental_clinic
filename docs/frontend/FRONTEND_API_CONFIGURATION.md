# Frontend API Configuration for Demo/Clinic Deployment

## Problem (historical)

Deployed dashboards failed to load data when the frontend build used localhost defaults instead of the demo/clinic API URL and key.

## Current solution (`mdc`)

`mdc deploy frontend --target demo|portfolio|clinic` sets build-time env from `deployment_credentials.json` (see `tools/mdc_cli/mdc_cli/deploy_frontend.py`):

| Target | App | Typical `VITE_API_URL` |
|--------|-----|-------------------------|
| `demo` / `portfolio` | `@mdc/portfolio` | `https://api.dbtdentalclinic.com` |
| `clinic` | `@mdc/clinic` | `https://api-clinic.dbtdentalclinic.com` |

```powershell
mdc deploy frontend --target demo
mdc deploy frontend --target clinic
```

Do **not** use archived `scripts/environment_manager.ps1` / `frontend-deploy` for new work. Env file inventory: [ENVIRONMENT_FILES.md](../deployment/ENVIRONMENT_FILES.md). Split layout: [FRONTEND_SPLIT_PLAN.md](FRONTEND_SPLIT_PLAN.md).

## Local dev

```powershell
mdc frontend dev --app portfolio
# or
mdc frontend dev --app clinic
```

`mdc frontend dev` writes `frontend/.env.local` with `VITE_API_URL` (and key when available) for localhost.

## Security Considerations

**Important:** Including the API key in the frontend build means it is visible in the browser JavaScript bundle. Acceptable for:

- Demo/portfolio sites with synthetic data
- Public-facing read-only dashboards

**Not recommended for:** write paths or strict PHI exposure controls beyond clinic IP allowlisting.

## Testing

```powershell
Invoke-WebRequest -Uri "https://api.dbtdentalclinic.com/docs"
# Visit https://dbtdentalclinic.com — DevTools → Network for API calls
```
