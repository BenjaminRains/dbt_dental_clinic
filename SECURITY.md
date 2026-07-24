# Security Policy

## Supported surfaces

Security reports are welcome for:

- The public demo site and API ([dbtdentalclinic.com](https://dbtdentalclinic.com), [api.dbtdentalclinic.com](https://api.dbtdentalclinic.com/docs)) — synthetic data only
- In-repo application code: API, frontend, ETL pipeline, dbt models, and `mdc` CLI as shipped in this repository

## Out of scope

- OpenDental itself (vendor product)
- Third-party cloud or SaaS platforms used by this project
- Clinic production infrastructure and environments that hold real Protected Health Information (PHI)

Do not attempt to access, probe, or test clinic/PHI systems. Good-faith research is limited to the public demo and this repository’s code.

## PHI, credentials, and secrets

**Never** open a public GitHub issue (or PR description) that includes:

- Patient data or any PHI
- Credentials, API keys, tokens, or `.env` contents
- Connection strings, clinic hostnames, or internal network details

Use private vulnerability reporting or email only for those cases.

## How to report a vulnerability

Preferred order:

1. **GitHub Private Vulnerability Reporting** — open the repository’s [Security](https://github.com/BenjaminRains/dbt_dental_clinic/security) tab and choose **Report a vulnerability** (Advisories).
2. **Email** — `rainsbp@gmail.com` with subject line starting with `[SECURITY]`.

### What to include

- Description of the issue and potential impact
- Steps to reproduce (or proof of concept that does not require PHI/clinic access)
- Affected component and, if known, version or commit

There is no bug bounty for this project.

## Response expectations

Reports are acknowledged within approximately **5 business days**. Fix timelines depend on severity and complexity.

## Safe harbor

If you research in good faith, stay within the supported surfaces above, avoid destroying data or degrading service, and do not access clinic/PHI systems, we will not pursue legal action related to that research. Always follow applicable law.

## Maintainer setup (post-merge)

After this policy lands, enable these under **Settings → Code security** (or Security product settings) if they are not already on:

| Setting | Purpose |
|---------|---------|
| Private vulnerability reporting | Preferred intake; avoids public issues |
| Dependabot alerts | Surfaces known CVEs |
| Dependabot security updates | Opens PRs for vulnerable dependencies |
| Dependency graph | Required for Dependabot |
| Secret scanning | Detects committed secrets |
| Push protection | Blocks pushes that contain secrets |
