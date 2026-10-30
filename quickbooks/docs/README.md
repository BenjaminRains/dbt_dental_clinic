# QuickBooks investigation notes

Planning notes for the QuickBooks source. The project root is `quickbooks/`. Credentials, the report catalog, and the extraction client live there, not in `docs/`.

## Documents

- **`QUICKBOOKS_INTEGRATION_PLAN.md`** — architecture plan
- **`QUICKBOOKS_CONNECTION_SETUP.md`** — connection notes
- **`QUICKBOOKS_BUSINESS_VALUE_AND_STRATEGY.md`** — business case

## Investigation Status

- [x] QuickBooks type: Online Plus
- [x] Read role: `integration_user` with View company reports
- [x] Reports user email and password recorded in `quickbooks/.env_clinic`
- [ ] Production client id and secret from Keys and credentials. The MDC Analytics app is still In development, so only Development keys exist today.
- [ ] OAuth completed as the reports user (realm id and refresh token)
- [ ] Sample reports extracted
- [ ] OpenDental is not the QuickBooks source: program `QuickBooksOnline` is disabled and its token fields are placeholders

Report catalog: `quickbooks/config/reports.yml`

From the `quickbooks/` directory:

```
python -m quickbooks --env clinic status
```

---

**Last Updated**: 2026-10-12