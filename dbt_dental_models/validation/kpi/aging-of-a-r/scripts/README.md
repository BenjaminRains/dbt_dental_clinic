# Aging of A/R golden helpers

Optional: add a parser later to turn OD Aging CSV section/column totals into
`golden/snapshots/*.snapshot.yml` (PHI-free), matching daily-payments /
daily-production-by-procedure scripts.

Until then, paste the report grand total and bucket column totals into:

- `compare/compare_aging_ar_total.sql` → `od_total`
- `compare/compare_aging_ar_buckets.sql` → `od_buckets`
- `golden_manifest.yml` (copy from `golden_manifest.example.yml`)
