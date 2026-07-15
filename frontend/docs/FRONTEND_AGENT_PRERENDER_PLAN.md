> **Path update (2026-07):** Portfolio lives in `frontend/apps/portfolio`. Deploy with
> `mdc deploy frontend --target demo` (builds `@mdc/portfolio` → `apps/portfolio/dist`).
> Prerender paths below that mention a monolithic `frontend/dist` apply to that app dist.

### Goal

Make the demo portfolio site (`dbtdentalclinic.com`) **agent- and crawler-friendly** by ensuring that:

- The **homepage (`/`)** and the **agent profile page (`/agent-profile`)** are served with **fully rendered HTML** (SSR/SSG-style) at deploy time.
- Agents that do not execute JavaScript can still:
  - Understand **who** Benjamin Rains is.
  - Classify **role, stack, and domain**.
  - See **project/system evidence** and navigate to `/agent-profile`.

This plan assumes the existing stack:

- Vite + React SPA.
- Deployed via `demo-frontend-deploy` → build to `dist` → upload to **S3 + CloudFront**.

---

### High-Level Strategy

Use a **prerender step in the deploy pipeline**:

1. Run the normal Vite production build (`npm run build` → `dist/`).
2. Start a temporary static file server serving `dist`.
3. Use a headless browser (Playwright or Puppeteer) to:
   - Load `http://localhost:PORT/` and `http://localhost:PORT/agent-profile`.
   - Wait for React to finish rendering.
   - Capture the final HTML.
4. Write that HTML back into:
   - `dist/index.html` for `/`.
   - `dist/agent-profile/index.html` (create directory) for `/agent-profile`.
5. Upload `dist` as usual to the S3 bucket.

Result: CloudFront serves **pre-rendered HTML** for the key pages, while the app still behaves as a SPA for users.

---

### Step 0 – Choose tooling and scope

**Tool choice**: Use **Playwright** (preferred) or **Puppeteer**.

- Playwright advantages:
  - Good defaults for waiting until the page is stable.
  - Simple API for `page.goto(url, { waitUntil: 'networkidle' })`.

**Scope (initial)**:

- Pre-render:
  - `/` (homepage / `Portfolio_v3`).
  - `/agent-profile` (plain-text agent profile page).
- Can be extended later to other critical routes (e.g., `/dashboard`) if needed.

---

### Step 1 – Add prerender dependencies

**1.1. Install Playwright (preferred)**

- Add dev dependency in `frontend/package.json`:
  - `"@playwright/test"` (or `"playwright"` if using core).

**1.2. Add a prerender script entry**

- In `frontend/package.json`, plan to add:

```json
"scripts": {
  "build": "tsc && vite build",
  "prerender": "node scripts/prerender-agent-pages.mjs"
}
```

The `prerender` script will:

- Assume `npm run build` has already run.
- Start a static server on `dist`.
- Use Playwright to render and write HTML.

---

### Step 2 – Implement `scripts/prerender-agent-pages.mjs`

Create `frontend/scripts/prerender-agent-pages.mjs` with the following responsibilities:

**2.1. Start a static file server for `dist/`**

- Use a small HTTP server:
  - Either Node's `http` + `fs` (manual static server), or
  - A minimal dependency like `serve-handler`.
- Bind to `localhost` on a free port (e.g., `4174` or dynamic).

**2.2. Use Playwright to prerender**

- Pseudocode:

```js
import { chromium } from 'playwright';

const routesToPrerender = [
  { path: '/', out: 'index.html' },
  { path: '/agent-profile', out: 'agent-profile/index.html' }
];

for each route:
  - open new page
  - goto(`http://localhost:PORT${path}`, { waitUntil: 'networkidle' })
  - grab `const html = await page.content()`
  - normalize `<script>` tags if needed (keep hydration scripts)
  - write to `dist/out` (create directory for nested paths)
```

**2.3. File writing rules**

- For `/`:
  - Overwrite existing `dist/index.html` with the rendered HTML.
- For `/agent-profile`:
  - Ensure directory `dist/agent-profile/` exists.
  - Write `dist/agent-profile/index.html`.

**2.4. Cleanup**

- Close browser.
- Stop static server.
- Exit with non-zero status if any route fails (so deploy script can stop).

---

### Step 3 – Integrate prerender into `demo-frontend-deploy`

Update `scripts/environment_manager.ps1` (inside `Deploy-Frontend`) with an extra step **after** `npm run build` and **before** S3 upload.

**3.1. Current simplified flow (demo mode)**

- Set `VITE_IS_DEMO="true"` etc.
- `npm run build` (Vite → `dist`).
- Upload `dist` to S3.
- Invalidate CloudFront.

**3.2. Target flow**

1. Set demo env vars (`VITE_IS_DEMO="true"`, API URL, etc.).
2. `npm run build`.
3. **Run prerender**:
   - `npm run prerender`.
4. Upload `dist` to S3 (unchanged).
5. Invalidate CloudFront (unchanged).

**3.3. Error handling**

- If `npm run prerender` fails:
  - Write a clear error to the console: prerender of `/` or `/agent-profile` failed.
  - Abort the deployment (non-zero exit in PowerShell function).

---

### Step 4 – Testing and verification

**4.1. Local test loop**

From `frontend/`:

1. `npm run build`
2. `npm run prerender`
3. Serve `dist` locally (`npx serve dist` or `vite preview`).
4. Run:

```bash
curl -L http://localhost:PORT/ | head -n 80
curl -L http://localhost:PORT/agent-profile | head -n 80
```

Verify:

- `/` HTML includes:
  - The hero text from `Portfolio_v3`.
  - The “machine-readable profile” link.
  - Footer link to `/agent-profile`.
- `/agent-profile` HTML includes:
  - The full agent profile text (name, role, stack, primary system, architecture, evidence).

**4.2. Demo deploy verification**

After running `demo-frontend-deploy`:

- Run:

```bash
curl -L https://dbtdentalclinic.com/ | head -n 80
curl -L https://dbtdentalclinic.com/agent-profile | head -n 80
```

Confirm:

- The **raw HTML** (without JS) already contains:
  - Name, role, location, email.
  - Core stack.
  - Project and architecture summaries.
  - Links to GitHub, dbt docs, API docs.

---

### Step 5 – Future enhancements (optional)

- **Extend prerender routes**:
  - Add `/dashboard` (demo analytics), or other key marketing pages as needed.

- **Add JSON-LD schema**:
  - Use `react-helmet-async` on `Portfolio_v3` and/or `AgentProfile` to emit:
    - `Person` schema for Benjamin Rains.
    - Optionally a `SoftwareApplication` / `CreativeWork` for the analytics platform.
  - Prerender will ensure this JSON-LD is also present in the initial HTML.

- **Sitemap or robots.txt**:
  - Add `/agent-profile` to any sitemap you maintain, and ensure it is crawlable.

---

### Summary of exact implementation order

1. **Add Playwright dependency** and `prerender` script entry in `frontend/package.json`.
2. **Create `scripts/prerender-agent-pages.mjs`**:
   - Static server for `dist`.
   - Playwright logic to render `/` and `/agent-profile`.
   - Write HTML into `dist/index.html` and `dist/agent-profile/index.html`.
3. **Update `environment_manager.ps1` (Deploy-Frontend)**:
   - After `npm run build`, run `npm run prerender`.
   - Fail deploy if prerender fails.
4. **Test locally**:
   - `npm run build && npm run prerender`.
   - Verify `curl` against local preview.
5. **Run `demo-frontend-deploy`** and verify with `curl` against production URLs.

