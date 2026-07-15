# Deployment connections — clinic access allowlist

**Purpose:** Track every network location that must reach IP-restricted clinic resources (`https://clinic.dbtdentalclinic.com` and, when restricted, `https://api-clinic.dbtdentalclinic.com`).

**Operational source of truth for AWS:** `deployment_credentials.json` → `clinic_frontend.waf.ip_sets` (WAF IP set IDs and CIDRs applied in us-east-1).

**Human-readable inventory:** Copy `connections.template.yaml` → `connections.yaml` and fill it in. `connections.yaml` is gitignored (contains real IPs).

---

## How access is enforced today

| Resource | Domain | Mechanism |
|----------|--------|-----------|
| Clinic frontend | `clinic.dbtdentalclinic.com` | CloudFront + **AWS WAF** IP allowlist |
| Clinic API | `api-clinic.dbtdentalclinic.com` | Documented as IP-restricted; confirm ALB/WAF rules match frontend if browser calls fail from new IPs |

WAF only sees the client’s **public IPv4** (or IPv6 if configured). Private addresses (`192.168.x.x`, `10.x.x.x`) are never used in allowlists.

**Live WAF policy (verified):** default action **Block**. Only explicit allow rules pass:

| WAF rule | Allowed CIDR | Who it covers |
|----------|--------------|---------------|
| `clinic-office-allowlist` | `99.89.219.137/32` | Browsers at the office (office WAN / NAT) |
| `clinic-dev-allowlist` | `50.158.66.217/32` | Dev machine direct internet egress |

No owner-home or VPN-client entries exist yet.

---

## WireGuard to MDC does **not** unlock the clinic site

This is the usual reason the business owner cannot reach `https://clinic.dbtdentalclinic.com` from home **even with WireGuard connected**.

### What WireGuard is doing

Typical MDC WireGuard config is **split tunnel**:

- **Through VPN:** clinic LAN only (e.g. `192.168.2.0/24` — OpenDental, file shares, RDP to MDC server).
- **Not through VPN:** the public internet, including `clinic.dbtdentalclinic.com` (CloudFront in AWS).

So when the owner opens a browser on his **home PC or home server** with WireGuard up:

```
Browser → clinic.dbtdentalclinic.com (CloudFront)
         ↑
         exits via HOME WAN public IP  →  WAF Block (not on allowlist)
```

WireGuard never carries that HTTPS flow. AWS WAF still sees the **home ISP IP**, not the office IP.

Same pattern documented for dev: *“WireGuard routes 192.168.2.x; general web uses this IP.”*

### What *would* use the office allowlisted IP

Traffic must **egress the internet from the office WAN** (`99.89.219.137`):

| Approach | Egress IP WAF sees | Works today? |
|----------|-------------------|--------------|
| Browser on home PC, WireGuard on (split tunnel) | Home WAN | **No** |
| Browser on home server, WireGuard on | Home WAN | **No** |
| **RDP to MDC server**, browser **on the server** | Office WAN (if server NATs like other office PCs) | **Should work** — verify with `curl ifconfig.me` on server |
| Add owner **home WAN** to WAF `owner_home` IP set | Home WAN | **Yes** — after AWS update |
| HTTP/SOCKS proxy on MDC; browser configured to use it | Office WAN | **Yes** — if proxy is set up |
| Route `clinic.dbtdentalclinic.com` / `api-clinic…` through VPN + MDC NAT | Office WAN | **Yes** — requires network/WireGuard `AllowedIPs` + NAT on MDC (not default) |

### Quick diagnosis (have owner run these)

**1. From home machine (WireGuard connected), in a browser:**

Open `https://ifconfig.me` → note IP **A**.

**2. On MDC server (RDP over WireGuard), in PowerShell:**

```powershell
curl.exe -s https://ifconfig.me
```

→ note IP **B**.

| Result | Meaning |
|--------|---------|
| **A** ≠ `99.89.219.137` | Home browsing is blocked by WAF — expected with split tunnel |
| **B** = `99.89.219.137` | Browsing **from the MDC server** should reach the clinic site; if it still fails, check API/CORS or a different error (not WAF IP) |
| **B** ≠ `99.89.219.137` | Office server uses a different egress IP — that IP must be added to WAF, not just the WAN you assumed |

**3. Clinic site test from MDC server:**

```powershell
curl.exe -sI https://clinic.dbtdentalclinic.com
```

`HTTP/2 200` or `304` → WAF allows that egress IP. `403` → IP still not allowlisted.

### Recommended fixes (pick one)

1. **Add owner home WAN to WAF** (`owner_home` IP set) — simplest for daily use from home PC/phone on Wi‑Fi.
2. **RDP to MDC and use Edge/Chrome on the server** — no WAF change if server egress is already `99.89.219.137`.
3. **Proxy via MDC** — owner browser points at an office proxy; all clinic HTTPS exits office IP.
4. **WireGuard policy routing** (advanced) — add CloudFront edge prefixes or `AllowedIPs` for clinic hostnames through MDC with SNAT; coordinate with whoever manages MDC WireGuard.

---

## Information needed per connection

Collect one row per **public IP** (or stable IP range), not per device. Multiple devices behind the same home router share one WAN IP.

### Required

| Field | Example | Notes |
|-------|---------|-------|
| **Label** | `Owner home — main` | Short name for WAF description / runbooks |
| **Public IPv4 (CIDR)** | `203.0.113.45/32` | Always `/32` for a single host unless ISP assigns a static block |
| **Connection type** | `clinic_office`, `owner_home`, `owner_mobile`, `dev`, `home_server` | Maps to WAF IP set grouping (see template) |
| **Who / contact** | Name, role | Who to ask when access breaks |
| **Static vs dynamic** | `static` or `dynamic` | Dynamic IPs need a change process (see below) |

### Strongly recommended

| Field | Why |
|-------|-----|
| **ISP / carrier** | Predicts stability (e.g. Comcast business static vs residential DHCP) |
| **Physical location** | Home city, office site — helps support |
| **Services needed** | `frontend`, `api`, `both` | Frontend alone is enough for browser; API matters if restricted separately |
| **Last verified** | Date someone confirmed the IP (e.g. from https://ifconfig.me from that network) |
| **How IP was obtained** | `browser whatismyip`, `router WAN status`, `ISP bill` |

### For home servers (same network as owner home)

Home servers do **not** get a separate public IP unless they have their own ISP line. Record:

- Same **WAN IP** as the home router (one allowlist entry for the whole LAN)
- **Hostname / role** of each server (e.g. `nas`, `etl-box`) in notes — for inventory only
- Whether jobs run **on the server** (outbound HTTPS to API) or someone **browses from a PC** on that LAN (same WAN IP)

### For the business owner’s cell phone

| Topic | Detail |
|-------|--------|
| **Cellular data** | Public IP changes often (tower handoff, airplane mode, CGNAT). **Not suitable** for a long-term WAF entry unless updated frequently. |
| **Phone on home Wi‑Fi** | Uses the **home WAN IP** — no separate mobile entry needed when at home. |
| **Phone on office Wi‑Fi** | Uses **office WAN IP** — already covered by `clinic_office` if configured. |
| **Practical options** | (1) Owner uses home/office Wi‑Fi for clinic site; (2) add cellular IP temporarily when traveling and remove after; (3) VPN into a location with a static allowlisted IP; (4) site-to-site VPN (see `docs/deployment/CLINIC_INFRASTRUCTURE_PLAN.md`). |

If you still add a mobile IP, record **carrier**, **approximate date/time verified**, and set **review_by** to a short date — assume it will change.

### Optional (IPv6, VPN, DDNS)

| Field | When |
|-------|------|
| **Public IPv6 (CIDR)** | Only if the network uses IPv6 to reach the internet and WAF IP sets include IPv6 |
| **DDNS hostname** | Dynamic residential IP — hostname for humans; WAF still needs the current A record resolved to CIDR |
| **VPN endpoint** | Split-tunnel VPN (WireGuard to MDC) does **not** change clinic-site egress — allowlist **home WAN** or browse from office/RDP (see above) |

---

## Current WAF IP sets (from template)

Defined in `deployment_credentials.json.template` under `clinic_frontend.waf.ip_sets`:

| Key | WAF IP set name | Typical use |
|-----|-----------------|-------------|
| `clinic_office` | `clinic-office-ips` | MDC / GLIC office WAN |
| `dev` | `clinic-dev-ips` | Developer machines |

**Suggested additions** for owner home + servers:

| Key | Suggested name | Typical use |
|-----|----------------|-------------|
| `owner_home` | `clinic-owner-home-ips` | Business owner residence WAN |
| `owner_mobile` | `clinic-owner-mobile-ips` | Cellular only — use sparingly |

Add keys to `deployment_credentials.json` and create matching IP sets + WAF rules in AWS (same pattern as office/dev).

---

## Workflow: add or change an allowed IP

1. **Inventory** — Update `docs/deployment-connections/connections.yaml` (local only).
2. **Verify IP** — From that network, open https://ifconfig.me or check the router’s WAN address. Confirm `/32` CIDR.
3. **AWS WAF** — WAF & Shield (us-east-1) → IP sets → edit the right set → add/remove CIDR → save.
4. **Credentials** — Mirror CIDRs in `deployment_credentials.json` → `clinic_frontend.waf.ip_sets.*.ip_addresses`.
5. **Test** — From that network: `https://clinic.dbtdentalclinic.com` loads; API health if applicable.
6. **Audit** — Set `last_verified` in `connections.yaml`; remove stale mobile IPs.

Verification script (read-only): `.\scripts\verification\check_waf_web_acls.ps1`

---

## Clinic portal login (username / password / role)

After WAF allows traffic, staff **sign in** at `https://clinic.dbtdentalclinic.com/login`. Each account maps to a role; the UI only shows routes that role is allowed to open (`admin` sees everything):

| Role key | UI label | Home route | Access |
|----------|----------|------------|--------|
| `admin` | Admin | `/home/admin` | All homes + all reports |
| `owner` | Owner | `/home/owner` | All homes + all reports (same as admin for now) |
| `practice-manager` | Practice Manager | `/home/practice-manager` | Manager home + all reports |
| `front-desk` | Front Desk | `/home/front-desk` | Desk home + schedule/patients/hygiene |
| `insurance` | Insurance Specialist | `/home/insurance` | Insurance home + AR/revenue/treatment |

### Stock accounts (temporary passwords)

Canonical user file (gitignored, **deploy to EC2**):

- `api/clinic-portal-users.json` — loaded by API (`CLINIC_PORTAL_USERS_FILE`)
- Copy for reference: `docs/deployment-connections/clinic-portal-users.json` (same content)
- Template (committed): `clinic-portal-users.template.json`

| Username | Password (temp) | Role | Home after login |
|----------|-----------------|------|------------------|
| `admin` | `Tmpp0rtal-Admin-9qXe2m` | Admin | `/home/admin` |
| `owner` | `Tmpp0rtal-Owner-8kRm4n` | Owner | `/home/owner` |
| `manager` | `Tmpp0rtal-Mgr-5hWq9x` | Practice Manager | `/home/practice-manager` |
| `frontdesk` | `Tmpp0rtal-Desk-2jZk7p` | Front Desk | `/home/front-desk` |
| `insurance` | `Tmpp0rtal-Ins-6bNf3v` | Insurance Specialist | `/home/insurance` |

Session signing secret (temp): `Tmpp0rtal-Session-Sign-9vLx2mKq8Wp` → `CLINIC_PORTAL_SESSION_SECRET` in `api/.env_api_clinic`.

### Deploy login (API code + users file)

`mdc deploy api --env clinic` copies **only** `api/.env`. The `/auth/login` routes must be deployed separately:

```powershell
.\scripts\deployment\deploy_clinic_portal_auth.ps1   # portal.py, portal_auth router, main.py, users JSON
mdc deploy api --env clinic                          # .env with CLINIC_PORTAL_USERS_FILE
```

Then hard-refresh `https://clinic.dbtdentalclinic.com/login`.

### Direct URLs (`/login`, `/dashboard`, …)

The clinic app is a single-page app (React Router). Only `/` had `index.html` in S3, so paths like `/login` returned **S3 AccessDenied** XML.

**Fix (automatic):** `mdc deploy frontend --target clinic` copies `index.html` to each route path in S3 (e.g. `login`, `dashboard`).

**Fix (preferred long-term):** CloudFront custom error responses — run once (needs `cloudfront:UpdateDistribution`):

```powershell
.\scripts\deployment\configure_clinic_cloudfront_spa.ps1
```

Or AWS Console → CloudFront → distribution `E2FPSEQG9KPO4E` → **Error pages** → create:

| HTTP code | Response page | HTTP response |
|-----------|---------------|---------------|
| 403 | `/index.html` | 200 |
| 404 | `/index.html` | 200 |

TTL **0** on both. Wait 5–15 minutes after saving.

**Security notes:**

- Passwords live **only on the clinic API server** (`api/.env_api_clinic` → EC2 `api/.env`), not in the frontend bundle.
- WAF IP allowlist is still required — login does not replace network restrictions.
- Sessions last **12 hours** (browser `sessionStorage`); use **Sign out** on shared PCs.
- `CLINIC_API_KEY` remains separate (frontend → API data calls). Portal login gates the UI only.

### Local dev

1. Set `CLINIC_PORTAL_USERS` and `CLINIC_PORTAL_SESSION_SECRET` in `api/.env_api_local` or `api/.env_api_clinic`.
2. `mdc api run --env clinic` (or `local` with `API_ENVIRONMENT=clinic`).
3. `mdc frontend dev` with clinic API URL / proxy.

---

## Related files

| File | Role |
|------|------|
| `connections.template.yaml` | Committed template — copy to `connections.yaml` |
| `connections.yaml` | Gitignored — your filled inventory |
| `deployment_credentials.json` | Gitignored — AWS IDs + live CIDR list |
| `deployment_credentials.json.template` | Committed — structure reference |
| `clinic-portal-users.template.json` | Committed — starter accounts for `CLINIC_PORTAL_USERS` |
