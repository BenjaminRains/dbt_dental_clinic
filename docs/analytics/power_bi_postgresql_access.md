# Power BI — PostgreSQL access (`bi_user`, selective marts)

## Local development (no gateway)

For skill-building and pilot authoring on a development machine, none of the
gateway/VPC machinery below applies: Power BI Desktop connects **directly to the local
PostgreSQL** instance (`localhost:5432`, database `opendental_analytics`) using the same
least-privilege **`bi_user`** pattern. `bi_user` credentials live in
`etl_pipeline/.env_local` (`POSTGRES_BI_USER` / `POSTGRES_BI_PASSWORD`, gitignored);
the grant script is the same [sql/bi_user_marts_grants.sql](sql/bi_user_marts_grants.sql).
Step-by-step setup, model guide, and DAX starter measures:
[powerbi/README.md](../../powerbi/README.md).

Because mart tables are dropped and recreated by `dbt run`, per-table grants disappear
on rebuild — re-run the `GRANT SELECT` statements after dbt runs (or adopt dbt `+grants`
for the pilot tables once `bi_user` exists in every target environment).

Everything below describes the **deployed** architecture (private Aurora).

## Analytics warehouse (how Power BI reaches it)

The **analytics database** is **Amazon Aurora PostgreSQL** (or Aurora-compatible), deployed like other clinic infra: **inside a private VPC**, **not reachable from the public internet**. There is no security-group rule that exposes the cluster endpoint to `0.0.0.0/0`; only approved paths (for example the API EC2, bastion, VPN, or peering) can connect. That matches the mental model in [NETWORK_INFRASTRUCTURE_EXPLAINED.md](../deployment/NETWORK_INFRASTRUCTURE_EXPLAINED.md) (RDS/Aurora as **vault**, VPC-only).

**Implications for Power BI:**

| Context | How you connect |
| ------- | ---------------- |
| **Power BI service** (scheduled **Import** refresh) | **Required:** [On-premises data gateway](https://learn.microsoft.com/power-bi/connect-data/service-gateway-onprem) on a Windows host that can resolve DNS to the **Aurora endpoint** and open **5432** to the cluster **or** **VNet / enterprise** gateway patterns if you standardize on those. The Microsoft cloud does **not** connect directly to a private Aurora hostname without a gateway path. |
| **Power BI Desktop** (authoring) | Run Desktop on a machine with **network reachability** to Aurora: corporate VPN into the VPC, **jump host / Session Manager** port-forwarding, or Remote Desktop to a **build EC2** in the same VPC. Use the same `bi_user` credentials and SSL settings as the gateway data source so the model matches what refreshes in the service. |

Use the **Aurora cluster endpoint** (or **reader endpoint** for heavy read-only workloads if you split BI traffic—optional). Confirm host, port, and DB name in `profiles.yml`, Secrets Manager, or your deployment runbooks.

---

## dbt schemas

dbt models use **`staging`**, **`int`**, and **`marts`** — not `public` or `public_*`. See [dbt_project.yml](../../dbt_dental_models/dbt_project.yml) and [generate_schema_name.sql](../../dbt_dental_models/macros/utils/generate_schema_name.sql).

## Least privilege: `bi_user`

Use a single login (**`bi_user`**) for Power BI (and similar BI tools) against Postgres:

1. **`CONNECT`** on the analytics database.
2. **`USAGE`** on schema **`marts`** — required so PostgreSQL can resolve object names in that schema.
3. **`SELECT`** only on **explicitly approved** tables in `marts` — not `ALL TABLES IN SCHEMA`.

New mart tables from dbt stay **invisible** to BI until you `GRANT SELECT` on them.

Executable template: [sql/bi_user_marts_grants.sql](sql/bi_user_marts_grants.sql) (pilot tables match [power_bi_pilot_dataset.md](power_bi_pilot_dataset.md)).

### Optional: `int` or `staging`

Avoid for standard reports. If needed, use the same pattern: `USAGE` on that schema + `SELECT` on **named** tables only.

## Connection strings (reference)

**Power BI Desktop / Gateway — PostgreSQL connector**

- **Server**: Aurora **writer or reader** endpoint (hostname from RDS console; no public IP in private setups).
- **Database**: analytics database (e.g. `opendental_analytics` — confirm in deploy config).
- **Port**: `5432` unless your operator changed it.
- **Username**: `bi_user`.
- **SSL**: **Require** (or stricter) — typical for Aurora and required by many org policies.

Example keyword string (placeholders only):

```text
Host=your-aurora-cluster.cluster-xxxxx.us-east-1.rds.amazonaws.com;Port=5432;Database=opendental_analytics;Username=bi_user;SSL Mode=Require;
```

Register the **same** connection on the gateway you use for scheduled refresh so Desktop and service behave consistently.

## Data gateway (private Aurora)

For this platform, treat the gateway as **part of the standard design**, not optional:

1. Install **On-premises data gateway** (standard mode) on a **Windows** host in a subnet that can reach Aurora (often the same VPC as the API EC2, or a management subnet with routing to Aurora’s security group).
2. Allow the gateway host’s security group as a **source** on Aurora’s inbound rule for PostgreSQL (principle of least privilege — not open to the internet).
3. In Power BI service, create the PostgreSQL data source **through that gateway cluster**, then bind your dataset refresh to it.

**Cost tip:** Reuse an existing management or app Windows/EC2 instance if policy allows, instead of a net-new VM, to avoid extra hosting cost.

## Cost estimations (order of magnitude)

Rough **USD** planning bands; verify on [Power BI pricing](https://powerbi.microsoft.com/pricing/) and your Microsoft agreement. Excludes tax.

| Item | Minimum / frugal pattern | Typical small team | Notes |
| ---- | ------------------------- | ------------------ | ----- |
| **Power BI Desktop** | $0 | $0 | Authoring; connect only from a VPC/VPN-capable workstation or jump box. |
| **Sharing & scheduled refresh** | ~\$10 × **1** user/month ≈ **\$120/yr** | ~\$10 × **5** users ≈ **\$600/yr** | **Power BI Pro** is the usual gate for workspaces and cloud refresh. List price often **~\$10/user/month**; some M365 SKUs include it. |
| **Premium Per User (PPU)** | Avoid unless needed | ~\$20–\$25/user/month | Only if you need premium-only features; else Pro minimizes cost. |
| **Premium / Fabric capacity** | $0 for pilot | Skip | **Thousands \$/month** — not needed for initial Aurora Import workloads. |
| **Aurora analytics cluster** | **$0 incremental** for BI reads | Same | BI uses the **existing** warehouse; `bi_user` only reduces **data exposure**, not instance size. |
| **Gateway host** | $0 if colocated | Small VM / instance if net-new | Prefer **existing** VPC host with a route to Aurora; otherwise budget **~\$5–\$25/mo**-class compute depending on shape. |
| **Import refresh** | Daily or post-dbt only | Same | Limits Aurora **read I/O**; Pro caps scheduled refreshes per dataset — plan cadence in the [pilot doc](power_bi_pilot_dataset.md). |

**Frugal default:** Minimal **Pro** count for authors and consumers of the workspace; **Import** mode; dataset limited to the **four pilot tables** in the grant script.

## Security notes

- Store `bi_user` in a **secret manager**; rotate like other DB service accounts.
- Do **not** place the Aurora cluster in a **public** subnet or attach **public** `0.0.0.0/0` ingress on 5432.
- Prefer **Import** over **DirectQuery** unless you have a strong freshness requirement (Import also reduces open connections to Aurora during report browsing).
- **RLS** in Power BI only if clinics/providers must be isolated.

## Related

- [Power BI pilot dataset](power_bi_pilot_dataset.md) — tables, refresh timing, gateway binding.
- [Pilot implementation artifacts](../../powerbi/README.md) — local setup, model guide, DAX measures.
- [Platform / career note](../../career/platform_projects/POWER_BI.md) — PBI vs React/API.
- [Deployment workflow](../deployment/DEPLOYMENT_WORKFLOW.md) — broader API/frontend/DB context.
