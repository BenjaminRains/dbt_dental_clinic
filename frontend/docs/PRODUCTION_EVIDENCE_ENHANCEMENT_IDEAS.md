# Production Evidence Section — Enhancement Ideas

**Context:** The "Production Evidence (Redacted)" section in `Portfolio_v3.tsx` currently lists capabilities (pipeline, dbt, CI/CD, AWS) and a few links. To strengthen the signal that this is an *operated* system rather than a UI demo, we need more **verifiable evidence** alongside those claims.

**Target section:** `Portfolio_v3.tsx` — "5. Production Evidence (§2)" (around lines 197–240).

---

## 1. Reframe Links as Verifiable Evidence

**Idea:** Keep the same URLs but present them explicitly as "proof you can check yourself."

**Example copy:**

| Link | Label | One-line proof |
|------|--------|-----------------|
| https://api.dbtdentalclinic.com/docs | Live API docs | OpenAPI (Swagger) served by the production FastAPI app — open to verify. |
| https://dbtdentalclinic.com/dbt-docs/ | Live dbt docs | dbt docs built and hosted from the real project; lineage and tests are current. |
| https://github.com/BenjaminRains/dbt_dental_clinic | Repository | CI/CD and pipeline code are public; you can see runs and deployment steps. |

**UI example (Artifacts block):**

```tsx
<Typography variant="subtitle2" fontWeight="bold" gutterBottom>Verify in production</Typography>
<Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 2 }}>
  <Box>
    <Link href="https://api.dbtdentalclinic.com/docs" target="_blank" rel="noopener noreferrer">
      Live API docs →
    </Link>
    <Typography variant="caption" display="block" color="text.secondary">
      OpenAPI served by production FastAPI
    </Typography>
  </Box>
  <Box>
    <Link href="https://dbtdentalclinic.com/dbt-docs/" target="_blank" rel="noopener noreferrer">
      Live dbt docs →
    </Link>
    <Typography variant="caption" display="block" color="text.secondary">
      Built from real project; lineage & tests
    </Typography>
  </Box>
  <Box>
    <Link href="https://github.com/BenjaminRains/dbt_dental_clinic" target="_blank" rel="noopener noreferrer">
      Repository →
    </Link>
    <Typography variant="caption" display="block" color="text.secondary">
      CI/CD and pipeline code visible
    </Typography>
  </Box>
</Box>
```

---

## 2. Live API Proof (Health / Version Endpoint)

**Idea:** If the FastAPI backend exposes a health or version endpoint, the frontend can call it and display "API status: OK" or "Backend version: x.y" — real-time evidence the system is running.

**Example endpoints:**

- `GET /health` → `{ "status": "ok" }`
- `GET /api/version` or `GET /version` → `{ "version": "1.0.0", "environment": "production" }`

**UI example:**

```tsx
// Fetch on mount or in a small hook
const [apiStatus, setApiStatus] = useState<{ status: string; version?: string } | null>(null);

useEffect(() => {
  fetch('https://api.dbtdentalclinic.com/health')
    .then((r) => r.json())
    .then((data) => setApiStatus(data))
    .catch(() => setApiStatus({ status: 'unreachable' }));
}, []);

// In JSX:
{apiStatus && (
  <Chip
    icon={apiStatus.status === 'ok' ? <CheckCircle /> : <Warning />}
    label={apiStatus.status === 'ok' ? 'API status: Live' : 'API status: Check link'}
    color={apiStatus.status === 'ok' ? 'success' : 'default'}
    size="small"
  />
)}
```

**Redaction note:** Only expose status/version or similar; do not expose internal hostnames or PHI.

---

## 3. Redacted Quantitative Proof (Dates / Counts)

**Idea:** Replace or supplement vague bullets with concrete-but-redacted facts so it reads as evidence, not marketing.

**Example bullets (manual or from CI/metadata later):**

- Last full pipeline run: **2025-02-24** (450+ tables).
- Last dbt run: **2025-02-24** — 150+ models, all tests passed.
- API and dbt docs regenerated on **each deploy** / **weekly**.

**Example UI (small evidence callout):**

```tsx
<Paper variant="outlined" sx={{ p: 2, bgcolor: 'grey.50' }}>
  <Typography variant="subtitle2" fontWeight="bold" gutterBottom>
    Last run evidence (redacted)
  </Typography>
  <Box component="ul" sx={{ m: 0, pl: 2, '& li': { typography: 'body2', color: 'text.secondary' } }}>
    <li>Pipeline: 450+ tables replicated (last run: 2025-02-24)</li>
    <li>dbt: 150+ models, all tests passed (last run: 2025-02-24)</li>
    <li>Docs: API and dbt docs rebuilt on each deploy</li>
  </Box>
</Paper>
```

**Maintenance:** Dates can be updated manually on deploy, or later wired to a status API or CI artifact.

---

## 4. Screenshots as Evidence (Redacted)

**Idea:** One or two small redacted screenshots that show "this really runs" — no PHI or sensitive details.

**Suggested assets:**

| Asset | What it shows | Redaction |
|-------|----------------|-----------|
| CI run | GitHub Actions (or other CI) green build, pipeline/dbt steps | Blur or hide org/repo name if needed |
| ETL log line | Single line, e.g. "Table X replicated, 12,345 rows" | Use generic table name and redacted row count if sensitive |
| API response | JSON e.g. `{"status":"ok","version":"1.0"}` in a code block | No internal hostnames or tokens |

**Example caption:**

```tsx
<Box>
  <img src="/production_evidence_ci_redacted.png" alt="Production CI run (redacted)" />
  <Typography variant="caption" color="text.secondary">
    Production CI run (redacted). Pipeline and dbt steps run on each merge.
  </Typography>
</Box>
```

**Code-block example (no screenshot needed):**

```tsx
<Paper variant="outlined" sx={{ p: 2, fontFamily: 'monospace', fontSize: '0.875rem' }}>
  {`{"status":"ok","version":"1.0.0","environment":"production"}`}
</Paper>
<Typography variant="caption" color="text.secondary">
  Sample response from production API /health (redacted).
</Typography>
```

---

## 5. Restructure: "Evidence" vs "Capabilities"

**Idea:** Split the section so **evidence** (things a reader can verify) is clearly separate from **capabilities** (what the system does).

**Suggested structure:**

1. **Subtitle:** e.g. "You can verify this is a live system: open the links below and check the API."
2. **Evidence (verifiable):**
   - Live API docs link + one-line "what this proves."
   - Live dbt docs link + one-line "what this proves."
   - Optional: API health/version call (if implemented).
   - Optional: "Last pipeline / dbt run" date or "CI passing" badge.
3. **Capabilities (what runs in production):**
   - Keep the two existing cards (Pipeline & warehouse, Deployment & operations) as a short summary of what the system does, not the only "proof."

**Example section outline:**

```text
Production Evidence (Redacted)
Operational proof that this is an operated system, not a UI demo.

[Evidence — verify yourself]
  • Live API: https://api.dbtdentalclinic.com/docs  → Proves: deployed FastAPI with real OpenAPI spec.
  • Live dbt docs: https://dbtdentalclinic.com/dbt-docs/  → Proves: real dbt project, built and hosted.
  • Repository: https://github.com/...  → Proves: CI/CD and code are visible.
  [Optional] API status: Live  (from /health)
  [Optional] Last pipeline run: 2025-02-24 (450+ tables)

[Capabilities — what runs in production]
  [Card 1] Pipeline & warehouse: daily ELT, 450+ tables, 150+ dbt models, full/incremental strategies...
  [Card 2] Deployment & operations: CI/CD, Docker, FastAPI, AWS (S3, EC2, RDS, CloudFront), 73% cost reduction...
```

---

## 6. Low-Effort Copy Tweaks Only

**Idea:** No new endpoints or assets; just clarify that the links are evidence.

**Example changes:**

- **Subtitle:**  
  From: "Operational proof that this is an operated system, not a UI demo."  
  To: "You can verify this is a live system: open the links below to see the running API and dbt docs."
- **Artifacts heading:**  
  From: "Artifacts"  
  To: "Verify in production"
- **Per-link labels:**  
  - "dbt docs lineage" → "Live dbt docs (lineage & tests)"  
  - "OpenAPI documentation" → "Live API (OpenAPI / Swagger)"  
  - "Repository" → "Source code & CI"

---

## 7. Suggested Implementation Order

| Priority | Approach | Effort | Impact |
|----------|----------|--------|--------|
| 1 | Restructure into Evidence vs Capabilities + reframe links with "what this proves" | Low | High |
| 2 | Add API health/version call if backend already has `/health` or `/version` | Low–medium | High (real-time proof) |
| 3 | Add one "last run" or "last built" date (manual or from CI) | Low | Medium |
| 4 | Add one redacted screenshot (CI or log/response) with caption | Medium | High (visual proof) |

---

## 8. References

- **Current implementation:** `frontend/src/pages/Portfolio_v3.tsx` — "5. Production Evidence (§2)" (lines ~197–240).
- **Overhaul plan:** `frontend/docs/PORTFOLIO_OVERHAUL_PLAN.md` (§2 Production Evidence).
