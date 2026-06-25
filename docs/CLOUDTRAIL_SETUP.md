# CloudTrail setup (dental-clinic AWS account)

CloudTrail records AWS API calls (who did what, when). Use it to audit Secrets Manager access before deleting or rotating credentials.

**Jun 2025 audit:** `GetSecretValue` on legacy `dental-clinic/database` showed only developer debugging — no EC2 roles or Lambda. Safe to delete that secret. The authoritative clinic RDS password is the **RDS master user secret** (`rds!db-...`); audit that name if you need ongoing access reviews.

**Corbin22 cannot create CloudTrail** — needs **root** or an **IAM admin**.

---

## Option A — Console (recommended first time)

1. Sign in as **root** or admin → region **us-east-1**.
2. Search **CloudTrail** → **Trails** → **Create trail**.
3. **Trail name:** `dental-clinic-trail`
4. **Storage location:** Create new S3 bucket (e.g. `dental-clinic-cloudtrail-412475545187`).
5. **Log events:**
   - **Management events:** **Read** and **Write** (Read is required for `GetSecretValue` audit).
   - Multi-region trail: **On**
   - Log file validation: **On**
6. Create trail → ensure **Logging** status is **On** (trail detail page).

### View secret access (after trail is running)

1. **CloudTrail** → **Event history**
2. **Lookup attributes** → **Event name** → `GetSecretValue`
3. Open an event → JSON → check `requestParameters.secretId` and `userIdentity.arn`

Event history only keeps **~90 days**. For longer retention, rely on the S3 log bucket lifecycle rules.

---

## Option B — Admin PowerShell script

From repo root, with **admin** AWS credentials:

```powershell
.\scripts\aws\setup_cloudtrail.ps1
```

Creates bucket, trail, enables logging, and sets **Read + Write** management events.

---

## Developer read-only access (Corbin22)

Merge into an **existing** IAM policy (avoid new attachment if at quota):

[`scripts/iam/cloudtrail_lookup_readonly_policy.json`](../scripts/iam/cloudtrail_lookup_readonly_policy.json)

Then:

```powershell
# RDS master user secret (authoritative for clinic analytics DB)
.\scripts\aws\lookup_secrets_manager_access.ps1 -SecretName "rds!db-83a24c7f-7e85-4168-ba14-ad6e63905c49"
```

Replace the secret name with your instance’s `MasterUserSecret` from `aws rds describe-db-instances`.

---

## Troubleshooting

| Issue | Fix |
|--------|-----|
| Can't find CloudTrail in console | Search bar → type **CloudTrail** (under Management & Governance) |
| No "Resource permissions" on secret | Normal — that tab is for cross-account policies on the secret, not CloudTrail |
| Event history empty | Trail must be **Logging**; wait 5–15 min; need **Read** management events |
| `AccessDenied` on lookup | Attach `cloudtrail:LookupEvents` to your user |
| `AccessDenied` on setup script | Run as admin, not Corbin22 |

---

## Related

- Clinic RDS secrets: [CLINIC_ANALYTICS_WORKFLOW.md](CLINIC_ANALYTICS_WORKFLOW.md) — **`rds!db-...` only**; legacy `dental-clinic/database` removed after CloudTrail audit
- IAM scripts: [scripts/iam/](../scripts/iam/)
