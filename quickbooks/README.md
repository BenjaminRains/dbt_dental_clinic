# QuickBooks

QuickBooks Online reports for the clinic. Investigation notes are in `docs/`.

Settings follow the repo stage files. `clinic` is the real company. `local` and `test` use an Intuit sandbox company. A value already in the process environment wins over the file.

The Intuit developer app is **MDC Analytics**. While that app is "In development", its keys are development keys and belong in `.env_local`. `.env_clinic` stays without a client id until **Get production keys** is finished. The App ID on the overview page is not the client id. Client id and client secret are under **Keys and credentials**. The redirect URI on that page must be `http://localhost:8080/callback`.

## Layout

| Path | Contents |
|---|---|
| `.env_clinic` | Clinic app keys and company tokens. Gitignored. |
| `.env_local` | Sandbox app keys and company tokens. Gitignored. |
| `.env_test` | Sandbox file for integration tests. Gitignored. |
| `.env_*.template` | Shape of those files |
| `config/reports.yml` | Reports the view-company-reports user can pull |
| `quickbooks/` | Extraction client |
| `docs/` | Integration plan and connection notes |
| `logs/` | Downloaded report JSON |

Copy a template to the matching `.env_<stage>` file before the first run. `.env_clinic` is already started on this machine.

## Commands

Run these from this directory:

```
python -m quickbooks --env clinic status
python -m quickbooks --env clinic auth-url --entity MDC
python -m quickbooks --env clinic exchange --entity MDC --callback-url "<callback url>"
python -m quickbooks --env clinic pull --entity MDC --start 2026-09-01 --end 2026-09-30
```

`clinic` calls `https://quickbooks.api.intuit.com`. `local` and `test` call the sandbox host. Token refresh writes the new refresh token back to the stage file.

## OAuth

The client follows Intuit's OAuth 2.0 guide with direct HTTPS calls. The Python SDK is not used. The scope is `com.intuit.quickbooks.accounting`. Python 3.11 negotiates TLS 1.2 or higher.

The reports user is only the browser sign-in. The API uses the app client id, client secret, and the refresh token from that sign-in.

| Intuit step | Where it lives |
|---|---|
| Development keys, for a sandbox company | `QUICKBOOKS_CLIENT_ID` and `QUICKBOOKS_CLIENT_SECRET` in `.env_local` |
| Production keys, for the live company | the same two fields in `.env_clinic` |
| Redirect URI, matched exactly | `http://localhost:8080/callback` |
| Authorization request | `python -m quickbooks --env clinic auth-url --entity MDC` |
| User consent | Sign in as the reports user and choose the live company |
| Exchange the code for tokens | `python -m quickbooks --env clinic exchange --entity MDC --callback-url "<callback url>"` |
| Report API calls | `python -m quickbooks --env clinic pull --entity MDC --start 2026-09-01 --end 2026-09-30` |

An access token lasts one hour. A refresh token rolls forward for 100 days each time it is used, and the new value is written back to the stage file. If that token expires, run `auth-url` again.
