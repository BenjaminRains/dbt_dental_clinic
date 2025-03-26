-- Table: public.pharmclinic

-- DROP TABLE IF EXISTS public.pharmclinic;

CREATE TABLE IF NOT EXISTS public.pharmclinic
(
    "PharmClinicNum" integer NOT NULL DEFAULT nextval('"pharmclinic_PharmClinicNum_seq"'::regclass),
    "PharmacyNum" bigint,
    "ClinicNum" bigint,
    CONSTRAINT pharmclinic_pkey PRIMARY KEY ("PharmClinicNum")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.pharmclinic
    OWNER to postgres;
-- Index: pharmclinic_ClinicNum

-- DROP INDEX IF EXISTS public."pharmclinic_ClinicNum";

CREATE INDEX IF NOT EXISTS "pharmclinic_ClinicNum"
    ON public.pharmclinic USING btree
    ("ClinicNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: pharmclinic_PharmacyNum

-- DROP INDEX IF EXISTS public."pharmclinic_PharmacyNum";

CREATE INDEX IF NOT EXISTS "pharmclinic_PharmacyNum"
    ON public.pharmclinic USING btree
    ("PharmacyNum" ASC NULLS LAST)
    TABLESPACE pg_default;