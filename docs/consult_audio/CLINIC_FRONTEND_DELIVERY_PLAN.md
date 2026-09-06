# Consult summaries → clinic frontend

> Status: **Early rough plan — review later, do not implement yet**
> Written: **2026-09-05**
> Related: [`consult_audio_pipe/README.md`](../../consult_audio_pipe/README.md), [FRONTEND_EVOLUTION_PROPOSAL.md](../frontend/FRONTEND_EVOLUTION_PROPOSAL.md), [TODO.md](../../TODO.md)

A finished consult **summary** (and optionally the skill **analysis**) should be
openable on `clinic.dbtdentalclinic.com` instead of opening HTML/PDF on the
machine that ran the pipe.

This is a delivery-layer plan, not a transcription-quality plan. Silent / bad
recordings stay a separate problem.

---

## Today

Two systems sit next to each other and never meet.

**Consult audio pipe** — drop audio in `raw_audio/` → Whisper → clean transcript
→ Claude or ChatGPT → `*_summary.md` / `*_analysis.md` → HTML + PDF. All of that
is gitignored PHI. No upload, no API, no warehouse load, no email.

**Clinic frontend** — staff portal with portal login and role homes plus
analytics reports. Roles: `admin`, `owner`, `practice-manager`, `front-desk`,
`insurance`. Data calls use a shared `X-API-Key`. There is no consult route, no
file-serving API, and no patient ID on pipe outputs (filename only).

```
raw_audio → Whisper → clean → LLM → local .md / .html / .pdf → [STOP]
clinic SPA → FastAPI → dbt marts
```

---

## The missing piece

Frontend work is the last mile. The new path is:

**publish → store → authorize → list → render**

| Step | Exists today | Needed |
| --- | --- | --- |
| Produce summary + analysis | Yes, local pipe | Keep as-is |
| Publish off the laptop | No | New publish step after a good run |
| Store + authorize | No | API the clinic app can call |
| Open in clinic UI | No | New `/consults` route + role gate |
| Match to OpenDental patient | Filename guess only | Decide now or defer |

---

## Decisions (open)

These four choices lock the architecture. Settle them before writing routes.

### 1. Who is the reader

Clinic roles today are staff, not dentist-as-dentist and not patient. Owner and
practice-manager are the natural first audience. Front-desk and insurance
probably should not see skill coaching notes.

Discuss: owner + practice-manager only for v1, or also admin? Dentist login later?

### 2. Which artifact

- **Summary** — factual consult write-up (concerns, plan, money, follow-up).
  Useful on a patient or work-queue screen.
- **Analysis** — skill / sales coaching. Different audience. Higher sensitivity
  if staff beyond the doctor can open it.

Discuss: summary-only in clinic for v1, analysis behind a tighter role, or both
on one page with tabs.

### 3. How it gets off the laptop

| Option | Fit | Tradeoff |
| --- | --- | --- |
| Publish to Postgres + API | Matches how clinic already loads marts | Markdown/HTML in the DB, or split text vs files |
| Upload HTML/PDF to object storage | Keeps blobs out of RDS | Need S3 (or similar) + signed URLs + clinic IAM |
| Serve files from clinic EC2 | Fastest if the pipe later runs there | Weak if the pipe still runs on a workstation |

The pipe still runs locally today. A publish command after a successful run is
enough for v1. Browser upload and in-clinic recording can wait.

### 4. Patient match

Outputs are named from the audio filename (`Gruessing wyatt_summary.md`). There
is no PatNum. The clinic Patients page is keyed off warehouse patients.

Discuss: v1 as a standalone consult list with a display name, or require a
PatNum / fuzzy match before publish.

---

## Suggested shape (if we keep it inside clinic)

Not a third app. Add a consults area next to existing reports, reuse
`ClinicLayout` and portal login, and put a real auth check on the new API
(Bearer + role), not only the shared API key.

**Clinic UI**

- `/consults` — list of published consults (name, date, status, model)
- `/consults/:id` — rendered summary, optional analysis tab, download PDF
- Later: card on owner / practice-manager home; link from a patient page once
  PatNum exists

**API**

- `GET /consults` — list metadata
- `GET /consults/:id` — summary (and analysis if allowed)
- `GET /consults/:id/pdf` — file or redirect

Do not expose raw audio or full transcripts in v1 unless we explicitly want that
PHI surface.

**Auth gap to budget for:** clinic pages are role-gated in the browser. Analytics
endpoints still accept the shared API key. Consult notes need portal session +
role on the new router.

---

## Phased build (after we agree)

| Phase | Outcome | Hold back |
| --- | --- | --- |
| 0. Decisions | Audience, artifact, storage, match rule | No code |
| 1. Publish | `mdc consult-audio publish` writes one consult record + files | No UI yet |
| 2. API | List/detail with portal role check | No audio, no upload |
| 3. Clinic page | `/consults` list + summary viewer in `ClinicLayout` | No patient deep-link yet |
| 4. Fit and finish | Home card, PatNum match, PDF download, analysis tab | QC gate and in-clinic ingest stay separate |

---

## Recommended defaults to argue with

- Owner + practice-manager see **summaries**
- Analysis is owner-only
- v1 is publish-after-a-good-run (no browser upload, no Whisper on EC2)
- No PatNum required
- No raw audio in the clinic app

---

## Out of scope for this path

- Silent-audio / volume QC before Whisper
- Browser audio upload
- Running Whisper on EC2
- Patient-facing portal
- Warehouse NLP marts

Those can attach later. This path assumes a human still drops a file, runs the
pipe, then publishes a good result.
