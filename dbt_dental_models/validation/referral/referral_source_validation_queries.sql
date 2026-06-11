-- Referral source analytics: run in DBeaver against PostgreSQL database opendental_analytics (analytics_user).
-- Use these to validate ETL coverage and how referrers (e.g. Drakos) are stored before relying on marts.
--
-- PATIENT vs REFERRAL TABLE
-- OpenDental does not require a Referral* column on patient. Your column list for raw.patient is normal: attribution is
--   raw.refattach (patient + ReferralNum + RefDate) -> referral dimension table (provider / source names, IsDoctor, etc.).
--
-- TABLE NAME SPELLING
-- Query-0 should return referral, referralcliniclink → use raw.referral in queries 3–4.

-- 0) List referral-related raw tables (expect referral + referralcliniclink in a standard import).
SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema = 'raw'
  AND table_type = 'BASE TABLE'
  AND table_name ILIKE 'refer%'
ORDER BY table_name;

-- 1) Column inventory for raw.patient
-- information_schema describes tables in schema "raw"; you are not querying "instead of" raw—it lists columns ON raw.patient.
-- If 1b returns 0 rows, there is simply no column on patient whose name contains "referral" — expected for many snapshots.
-- If 1a returns nothing, try table_name = 'Patient' (quoted DDL) or run 1c to find the real schema/name.

-- 1a) All columns (confirms schema/table name and spelling in the catalog).
SELECT column_name, data_type, ordinal_position
FROM information_schema.columns
WHERE table_schema = 'raw'
  AND table_name = 'patient'
ORDER BY ordinal_position;

-- 1b) Only columns with "referral" in the name (OpenDental versions differ).
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'raw'
  AND table_name = 'patient'
  AND column_name ILIKE '%referral%'
ORDER BY ordinal_position;

-- 1c) If 1a returns no rows, your patient table may live under another schema or relname; list candidates:
SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_type = 'BASE TABLE'
  AND table_schema NOT IN ('pg_catalog', 'information_schema')
  AND lower(table_name) IN ('patient', 'patients');

-- 1d) pg_catalog alternative (physical columns on raw.patient)—useful if identifiers differ from information_schema expectations.
SELECT a.attname AS column_name, format_type(a.atttypid, a.atttypmod) AS postgres_type
FROM pg_attribute a
JOIN pg_class c ON a.attrelid = c.oid
JOIN pg_namespace n ON c.relnamespace = n.oid
WHERE n.nspname = 'raw'
  AND c.relname = 'patient'
  AND a.attnum > 0
  AND NOT a.attisdropped
ORDER BY a.attnum;

-- 2) Refattach row counts and date range (staging uses referral_date >= 2023-01-01 filter; adjust if you broaden history).
SELECT
    count(*) AS refattach_rows,
    min("RefDate"::date) AS min_ref_date,
    max("RefDate"::date) AS max_ref_date
FROM raw.refattach;

-- 3) Referral masters flagged as doctors: spot-check display names.
SELECT
    "ReferralNum" AS referral_id,
    trim("LName") AS last_name,
    trim("FName") AS first_name,
    trim("BusinessName") AS business_name,
    "IsDoctor"::int AS is_doctor,
    "IsHidden"::int AS is_hidden
FROM raw.referral
WHERE "IsDoctor" = 1
ORDER BY "LName", "FName"
LIMIT 50;

-- 4) Match a specific referrer (example: Drakos) in raw.referral.
SELECT "ReferralNum", trim("LName") AS lname, trim("FName") AS fname, trim("BusinessName") AS biz
FROM raw.referral
WHERE trim(lower("LName")) LIKE '%drakos%'
   OR trim(lower("BusinessName")) LIKE '%drakos%';

-- 5) After dbt build: bridge row count vs distinct patient-referral pairs from staging.
-- SELECT count(*) FROM public_marts.int_patient_referral_bridge;
